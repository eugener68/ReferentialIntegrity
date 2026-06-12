# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Legacy Snapshots + SCD2 Version Diagnostic
# MAGIC
# MAGIC Per enabled provider:
# MAGIC 1. **Snapshot** the legacy table from the foreign catalog into `staging.legacy_<t>`,
# MAGIC    computing `natural_key` and `nk_hash` **in flight** (§1.5). SQL Server stays read-only.
# MAGIC 2. **Collision gate** (§1.3) on snapshot and (inline) on target.
# MAGIC 3. For SCD2/HUB_SCD2: the **§2.2 version diagnostic** → suggest/record Path A or B.
# MAGIC
# MAGIC Writes only to `staging` and `ri_repair`. Safe on live prod.

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

w = load_package_settings(require_saved=True)
ctx = Ctx(w)
providers = load_providers(ctx)
assert providers, "No enabled providers in scope."
errors = []

# COMMAND ----------

# MAGIC %md ## 1. Snapshots (CTAS from foreign catalog, hash computed in flight)

# COMMAND ----------

def snapshot_exists(p):
    return spark.catalog.tableExists(
        f"{w['target_catalog']}.{w['staging_schema']}.legacy_{p['provider_table']}")

for p in providers:
    t = p["provider_table"]
    if snapshot_exists(p) and w["refresh_snapshots"].lower() != "true":
        print(f"snapshot exists, skipping: legacy_{t} (set refresh_snapshots=true to redo)")
        continue
    nk_cols = ", ".join(f"`{c}`" for c in p["nk_cols"])
    scd2 = p["archetype"] in ("SCD2", "HUB_SCD2")
    extra = ""
    if scd2:
        extra = f", `{p['effective_start_col']}`"
        if p["effective_end_col"]:
            extra += f", `{p['effective_end_col']}`"
        if p["record_status_col"]:
            extra += f", `{p['record_status_col']}`"
    ctx.exec_mut(f"""
CREATE OR REPLACE TABLE {ctx.stg(t)} AS
SELECT
  `{p['sk_col']}`                                   AS old_sk,
  {nk_cols}{extra},
  {nk_string_expr(p['nk_cols'], p['nk_type_overrides'])}  AS natural_key,
  {nk_hash_expr(p['nk_cols'], p['nk_type_overrides'])}    AS nk_hash,
  current_timestamp()                               AS snapshot_at
FROM {ctx.src(t)}""", f"snapshot legacy_{t}")

# COMMAND ----------

# MAGIC %md ## 2. Collision gate (§1.3) — must be 0 everywhere

# COMMAND ----------

gate_schema = T.StructType([T.StructField(n, t) for n, t in [
    ("run_id", T.StringType()), ("table_name", T.StringType()),
    ("gate", T.StringType()), ("violations", T.LongType()),
    ("passed", T.BooleanType()), ("run_at", T.TimestampType())]])
gates = []

for p in providers:
    t = p["provider_table"]
    if not snapshot_exists(p):
        if not ctx.dry:
            errors.append(f"{t}: snapshot missing")
        continue
    v = scalar(ctx.query(f"""
      SELECT count(*) FROM (
        SELECT nk_hash FROM {ctx.stg(t)}
        GROUP BY nk_hash HAVING count(DISTINCT natural_key) > 1)"""))
    gates.append((RUN_ID, f"legacy_{t}", "hash_collision", v, v == 0,
                  datetime.datetime.utcnow()))
    # same check inline on the target (hash column may not exist yet -> expression)
    v2 = scalar(ctx.query(f"""
      SELECT count(*) FROM (
        SELECT {nk_hash_expr(p['nk_cols'], p['nk_type_overrides'])} h
        FROM {ctx.tgt(t)}
        GROUP BY 1
        HAVING count(DISTINCT {nk_string_expr(p['nk_cols'], p['nk_type_overrides'])}) > 1)"""))
    gates.append((RUN_ID, t, "hash_collision", v2, v2 == 0, datetime.datetime.utcnow()))
    if v or v2:
        errors.append(f"{t}: xxhash64 collision detected (legacy={v}, target={v2})")

record_rows(ctx, "gate_results", gates, gate_schema)
for g in gates:
    print(f"{'PASS' if g[4] else 'FAIL'}  {g[2]:<16} {g[1]} (violations={g[3]})")

# COMMAND ----------

# MAGIC %md ## 3. §2.2 version diagnostic per SCD2 / HUB_SCD2 → Path A or B

# COMMAND ----------

diag_schema = T.StructType([T.StructField(n, t) for n, t in [
    ("run_id", T.StringType()), ("provider_table", T.StringType()),
    ("status", T.StringType()), ("version_rows", T.LongType()),
    ("matched_pct", T.DoubleType()), ("suggested_path", T.StringType()),
    ("run_at", T.TimestampType())]])
diag_rows = []
threshold = float(w["path_a_threshold"])

for p in providers:
    if p["archetype"] not in ("SCD2", "HUB_SCD2") or not snapshot_exists(p):
        continue
    t, start = p["provider_table"], p["effective_start_col"]
    res = ctx.query(f"""
      WITH legacy AS (
        SELECT nk_hash, `{start}` AS esd, count(*) c
        FROM {ctx.stg(t)} GROUP BY 1, 2),
      target AS (
        SELECT {nk_hash_expr(p['nk_cols'], p['nk_type_overrides'])} AS nk_hash,
               `{start}` AS esd, count(*) c
        FROM {ctx.tgt(t)} GROUP BY 1, 2)
      SELECT CASE WHEN l.nk_hash IS NULL THEN 'VERSION_ONLY_IN_TARGET'
                  WHEN t.nk_hash IS NULL THEN 'VERSION_ONLY_IN_LEGACY'
                  ELSE 'VERSION_MATCHED' END AS status,
             count(*) AS version_rows
      FROM legacy l FULL OUTER JOIN target t
        ON l.nk_hash = t.nk_hash AND l.esd = t.esd
      GROUP BY 1""", f"version diagnostic {t}").collect()

    counts = {r.status: r.version_rows for r in res}
    matched = counts.get("VERSION_MATCHED", 0)
    only_legacy = counts.get("VERSION_ONLY_IN_LEGACY", 0)
    legacy_total = matched + only_legacy
    pct = matched / legacy_total if legacy_total else 0.0
    suggested = "A" if pct >= threshold else "B"
    print(f"{t}: matched={matched}, only_legacy={only_legacy}, "
          f"only_target={counts.get('VERSION_ONLY_IN_TARGET', 0)}, "
          f"matched_pct(of legacy)={pct:.4f} -> suggested Path {suggested}")
    now = datetime.datetime.utcnow()
    for s, c in counts.items():
        diag_rows.append((RUN_ID, t, s, c, pct, suggested, now))

    if w["auto_set_path"].lower() == "true" and not p["version_match_path"]:
        ctx.exec_mut(f"""
          UPDATE {ctx.cfg('config_providers')}
          SET version_match_path = '{suggested}', updated_at = current_timestamp()
          WHERE lower(provider_table) = lower('{t}') AND version_match_path IS NULL""",
          f"set path {suggested} for {t}")
    elif p["version_match_path"] and p["version_match_path"] != suggested:
        print(f"  NOTE: config says Path {p['version_match_path']}, diagnostic suggests "
              f"{suggested} — review before 02.")

record_rows(ctx, "version_diagnostics", diag_rows, diag_schema)

# COMMAND ----------

# MAGIC %md ## 4. Outcome

# COMMAND ----------

display(spark.sql(f"""
  SELECT provider_table, archetype, version_match_path, topo_level, enabled
  FROM {ctx.cfg('config_providers')} ORDER BY topo_level, provider_table"""))

if errors:
    raise Exception("01 finished with blocking issues:\n" + "\n".join(errors))
print("02 complete. Review paths above, then run 03_provider_hash_keymap.")
