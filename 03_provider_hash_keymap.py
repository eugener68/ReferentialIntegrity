# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Provider Hashes + Key-Maps (Use Cases A, B, D-provider)
# MAGIC
# MAGIC Per enabled provider, in topo order:
# MAGIC 1. Add + populate `nk_hash` (and `ver_hash` for Path-A SCD2/HUB) on the **target** table.
# MAGIC 2. Uniqueness / window-overlap **gates** (A2, B3). Failing tables get **no key-map**.
# MAGIC 3. Build `keymap.<provider>_keymap` (A4, B4, D2) and audit its status distribution.
# MAGIC
# MAGIC ⚠️ Step 1 mutates **repair target** tables (WIP clones when `repair_target_mode=wip_clone`,
# MAGIC or production when `in_place`). Run **02b_wip_clone** first in default safe mode.

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

w = load_package_settings(require_saved=True)
ctx = Ctx(w)
assert_mutating_target_allowed(ctx)
providers = load_providers(ctx)
assert providers, "No enabled providers in scope."
errors, gate_failed = [], set()

gate_schema = T.StructType([T.StructField(n, t) for n, t in [
    ("run_id", T.StringType()), ("table_name", T.StringType()),
    ("gate", T.StringType()), ("violations", T.LongType()),
    ("passed", T.BooleanType()), ("run_at", T.TimestampType())]])
gates = []

def gate(table, name, violations):
    ok = (violations == 0)
    gates.append((RUN_ID, table, name, violations, ok, datetime.datetime.utcnow()))
    print(f"{'PASS' if ok else 'FAIL'}  {name:<28} {table} (violations={violations})")
    if not ok:
        gate_failed.add(table.lower())
        errors.append(f"{table}: gate '{name}' failed ({violations})")
    return ok

def is_scd2(p):  return p["archetype"] in ("SCD2", "HUB_SCD2")
def path(p):     return (p["version_match_path"] or "").upper()

# COMMAND ----------

# MAGIC %md ## 1. Add + populate hash columns on target providers

# COMMAND ----------

for p in providers:
    t = p["provider_table"]
    if is_scd2(p) and path(p) not in ("A", "B"):
        errors.append(f"{t}: version_match_path not set — run 02 or set manually")
        gate_failed.add(t.lower())
        continue
    cols = {"nk_hash": "BIGINT"}
    if is_scd2(p) and path(p) == "A":
        cols["ver_hash"] = "BIGINT"
    ensure_columns(ctx, ctx.tgt(t), cols)

    guard = "" if w["recompute_hashes"].lower() == "true" else "WHERE nk_hash IS NULL"
    sets = [f"nk_hash = {nk_hash_expr(p['nk_cols'], p['nk_type_overrides'])}"]
    if "ver_hash" in cols:
        sets.append(f"ver_hash = {ver_hash_expr(p['nk_cols'], p['effective_start_col'], p['nk_type_overrides'])}")
    ctx.exec_mut(f"UPDATE {ctx.tgt(t)} SET {', '.join(sets)} {guard}",
                 f"populate hashes on {t}")

# COMMAND ----------

# MAGIC %md ## 2. Gates (A2 / B3) — uniqueness and window overlap

# COMMAND ----------

for p in providers:
    t = p["provider_table"]
    if t.lower() in gate_failed:
        continue
    if ctx.dry and "nk_hash" not in [c.lower() for c in table_columns(ctx.tgt(t))]:
        print(f"dry-run: hashes not present on {t}, skipping gates")
        continue

    if p["archetype"] == "SCD1":
        v = scalar(ctx.query(f"""
          SELECT count(*) FROM (SELECT nk_hash FROM {ctx.tgt(t)}
          GROUP BY nk_hash HAVING count(*) > 1)"""))
        gate(t, "scd1_nk_hash_unique", v)
        continue

    start, end = p["effective_start_col"], p["effective_end_col"]
    # (nk_hash, startDate) uniqueness — decides whether the status tiebreaker is needed (§2.3)
    v = scalar(ctx.query(f"""
      SELECT count(*) FROM (SELECT nk_hash, `{start}` FROM {ctx.tgt(t)}
      GROUP BY 1, 2 HAVING count(*) > 1)"""))
    if v > 0 and p["use_status_tiebreaker"] and p["record_status_col"]:
        v2 = scalar(ctx.query(f"""
          SELECT count(*) FROM (
            SELECT nk_hash, `{start}`, {status_class_expr(p['record_status_col'])} sc
            FROM {ctx.tgt(t)} GROUP BY 1, 2, 3 HAVING count(*) > 1)"""))
        gate(t, "nk_start_status_unique", v2)
    else:
        ok = gate(t, "nk_start_unique", v)
        if not ok:
            print(f"  -> {v} duplicate (nk,start) groups. If two rows legitimately differ "
                  f"only by deleted-status, set use_status_tiebreaker=true for {t}.")

    if path(p) == "A":
        v = scalar(ctx.query(f"""
          SELECT count(*) FROM (SELECT ver_hash FROM {ctx.tgt(t)}
          GROUP BY ver_hash HAVING count(*) > 1)"""))
        gate(t, "ver_hash_unique", v)

    if end:
        v = scalar(ctx.query(f"""
          SELECT count(*) FROM (
            SELECT a.nk_hash FROM {ctx.tgt(t)} a JOIN {ctx.tgt(t)} b
              ON a.nk_hash = b.nk_hash AND a.`{p['sk_col']}` < b.`{p['sk_col']}`
             AND a.`{start}` < {window_end_expr(end, 'b')}
             AND b.`{start}` < {window_end_expr(end, 'a')}
            GROUP BY a.nk_hash)"""))
        gate(t, "no_window_overlap", v)

record_rows(ctx, "gate_results", gates, gate_schema)

# COMMAND ----------

# MAGIC %md ## 3. Build key-maps (skipped for gate-failed tables)

# COMMAND ----------

audit_schema = T.StructType([T.StructField(n, t) for n, t in [
    ("run_id", T.StringType()), ("provider_table", T.StringType()),
    ("map_status", T.StringType()), ("cnt", T.LongType()),
    ("run_at", T.TimestampType())]])
audit_rows = []

for p in providers:
    t = p["provider_table"]
    if w["build_keymaps"].lower() != "true":
        break
    if t.lower() in gate_failed:
        print(f"SKIP keymap for {t} — gates failed; fix the dim first (A5).")
        continue
    sk = p["sk_col"]
    tgt_nk = nk_string_expr(p["nk_cols"], p["nk_type_overrides"])

    if p["archetype"] == "SCD1":
        stmt = f"""
CREATE OR REPLACE TABLE {ctx.km(t)}
TBLPROPERTIES ('ri_repair.version_match' = 'none-scd1') AS
SELECT
  coalesce(o.natural_key, n.natural_key)  AS natural_key,
  coalesce(o.nk_hash, n.nk_hash)          AS nk_hash,
  CAST(NULL AS TIMESTAMP)                 AS effectiveStartDate,
  CAST(NULL AS TIMESTAMP)                 AS effectiveEndDate,
  CAST(NULL AS BIGINT)                    AS ver_hash,
  o.old_sk, n.new_sk,
  CASE WHEN o.nk_hash IS NULL THEN 'ORPHAN_NEW'
       WHEN n.nk_hash IS NULL THEN 'ORPHAN_OLD'
       ELSE 'MATCHED' END                 AS map_status,
  current_timestamp()                     AS created_at
FROM {ctx.stg(t)} o
FULL OUTER JOIN (SELECT `{sk}` AS new_sk, nk_hash, {tgt_nk} AS natural_key
                 FROM {ctx.tgt(t)}) n
  ON o.nk_hash = n.nk_hash"""

    elif path(p) == "A":
        start, end = p["effective_start_col"], p["effective_end_col"]
        tie = ""
        if p["use_status_tiebreaker"] and p["record_status_col"]:
            tie = (f"\n  AND {status_class_expr(p['record_status_col'], 'o')}"
                   f" = {status_class_expr(p['record_status_col'], 'n')}")
        end_sel = f"n.`{end}`" if end else "CAST(NULL AS TIMESTAMP)"
        stmt = f"""
CREATE OR REPLACE TABLE {ctx.km(t)}
TBLPROPERTIES ('ri_repair.version_match' = 'A-exact') AS
SELECT
  coalesce(o.natural_key, n.natural_key)  AS natural_key,
  coalesce(o.nk_hash, n.nk_hash)          AS nk_hash,
  coalesce(n.`{start}`, o.`{start}`)      AS effectiveStartDate,
  {end_sel}                               AS effectiveEndDate,
  n.ver_hash,
  o.old_sk, n.new_sk,
  CASE WHEN o.nk_hash IS NULL THEN 'ORPHAN_NEW'
       WHEN n.nk_hash IS NULL THEN 'ORPHAN_OLD'
       ELSE 'MATCHED' END                 AS map_status,
  current_timestamp()                     AS created_at
FROM {ctx.stg(t)} o
FULL OUTER JOIN (SELECT `{sk}` AS new_sk, nk_hash, ver_hash,
                 {tgt_nk} AS natural_key, `{start}`{f", `{end}`" if end else ""}{f", `{p['record_status_col']}`" if p['use_status_tiebreaker'] and p['record_status_col'] else ""}
                 FROM {ctx.tgt(t)}) n
  ON o.nk_hash = n.nk_hash
 AND o.`{start}` = n.`{start}`{tie}"""

    else:  # Path B — window overlap; AMBIGUOUS when one legacy row spans several target windows
        start, end = p["effective_start_col"], p["effective_end_col"]
        assert end, f"{t}: Path B requires effective_end_col"
        stmt = f"""
CREATE OR REPLACE TABLE {ctx.km(t)}
TBLPROPERTIES ('ri_repair.version_match' = 'B-event-date') AS
WITH tgt AS (
  SELECT `{sk}` AS new_sk, nk_hash, {tgt_nk} AS natural_key,
         `{start}` AS t_start, {window_end_expr(end)} AS t_end, `{end}` AS t_end_raw
  FROM {ctx.tgt(t)}),
matched AS (
  SELECT o.natural_key, o.nk_hash,
         n.t_start AS effectiveStartDate, n.t_end_raw AS effectiveEndDate,
         CAST(NULL AS BIGINT) AS ver_hash,
         o.old_sk, n.new_sk,
         CASE WHEN n.new_sk IS NULL THEN 'ORPHAN_OLD'
              WHEN count(n.new_sk) OVER (PARTITION BY o.old_sk) > 1 THEN 'AMBIGUOUS'
              ELSE 'MATCHED' END AS map_status
  FROM {ctx.stg(t)} o
  LEFT JOIN tgt n
    ON  o.nk_hash = n.nk_hash
   AND o.`{start}` < n.t_end
   AND n.t_start < {window_end_expr(end, 'o')})
SELECT *, current_timestamp() AS created_at FROM matched
UNION ALL
SELECT n.natural_key, n.nk_hash, n.t_start, n.t_end_raw,
       CAST(NULL AS BIGINT), CAST(NULL AS BIGINT) AS old_sk, n.new_sk,
       'ORPHAN_NEW', current_timestamp()
FROM tgt n LEFT ANTI JOIN {ctx.stg(t)} o ON o.nk_hash = n.nk_hash"""

    ctx.exec_mut(stmt, f"build keymap for {t}")

    if not ctx.dry:
        dist = ctx.query(f"SELECT map_status, count(*) c FROM {ctx.km(t)} GROUP BY 1").collect()
        now = datetime.datetime.utcnow()
        for r in dist:
            audit_rows.append((RUN_ID, t, r.map_status, r.c, now))
        print(f"{t}: " + ", ".join(f"{r.map_status}={r.c}" for r in dist))
        orphan_old = next((r.c for r in dist if r.map_status == "ORPHAN_OLD"), 0)
        total = sum(r.c for r in dist)
        if total and orphan_old / total > 0.01:
            print(f"  ⚠️ {t}: ORPHAN_OLD > 1% — reload dropped members/versions. "
                  f"Fix the dimension BEFORE touching any consumer (A5/B5).")

record_rows(ctx, "keymap_audit", audit_rows, audit_schema)

# COMMAND ----------

# MAGIC %md ## 4. Outcome

# COMMAND ----------

if errors:
    raise Exception("02 finished with gate failures — fix providers, re-run:\n" + "\n".join(errors))
print("03 complete. All gates green, key-maps built. Next: 04_consumer_hash (mode=classify).")
