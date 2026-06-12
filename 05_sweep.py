# Databricks notebook source
# MAGIC %md
# MAGIC # 05 — SK Sweep (§6 C4–C5, §7 D6–D7) — idempotent, providers-first
# MAGIC
# MAGIC Re-keys consumer FK columns **via the hash join** (never via old_sk→new_sk — that's
# MAGIC the overlapping-range hazard). Guarded MERGE (`fk <> new_sk`) makes every sweep a
# MAGIC no-op on re-run and yields touched-row counts from MERGE metrics.
# MAGIC
# MAGIC Ordering: consumers that are themselves providers (hubs) sweep first, by `topo_level`,
# MAGIC then facts. A hub's **own SK is never swept** — only the FK columns inside it.
# MAGIC
# MAGIC Refuses to run unless the latest 04 validation run in scope is green
# MAGIC (`require_validation=false` to override — don't, outside rehearsal).
# MAGIC
# MAGIC ⚠️ Mutates target tables. Frozen-pipeline window + rehearsal-first are mandatory (§8, §9).

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

w = create_widgets({
    "require_validation": "true",
    "orphan_sk": "-1",            # unknown-member SK
    "apply_orphan_sk": "false",   # 'true' = point NULL-hash rows at orphan_sk (C4 last step)
})
ctx = Ctx(w)
providers = providers_by_name(ctx)
consumers = load_consumers(ctx)
assert consumers, "No consumers in scope."

def is_scd2(p):  return p["archetype"] in ("SCD2", "HUB_SCD2")
def path(p):     return (p["version_match_path"] or "").upper()

# COMMAND ----------

# MAGIC %md ## 0. Gate on latest validation run

# COMMAND ----------

if w["require_validation"].lower() == "true":
    last = spark.sql(f"""
      SELECT run_id FROM {ctx.cfg('validation_results')}
      ORDER BY run_at DESC LIMIT 1""").collect()
    assert last, "No validation runs found — run 04 first."
    last_run = last[0].run_id
    in_scope = {(c["consumer_table"].lower(), c["fk_col"].lower()) for c in consumers}
    rows = spark.sql(f"""
      SELECT consumer_table, fk_col, check_name, verdict
      FROM {ctx.cfg('validation_results')} WHERE run_id = '{last_run}'""").collect()
    validated = {(r.consumer_table.lower(), r.fk_col.lower()) for r in rows}
    fails = [r for r in rows if r.verdict == "FAIL"
             and (r.consumer_table.lower(), r.fk_col.lower()) in in_scope]
    missing = in_scope - validated
    assert not fails, ("Latest validation has FAILs in scope:\n" +
                       "\n".join(f"  {r.consumer_table}.{r.fk_col}: {r.check_name}" for r in fails))
    assert not missing, (f"Roles in scope but absent from latest validation run {last_run}: "
                         f"{sorted(missing)} — re-run 04 covering them.")
    print(f"Validation gate OK (run {last_run}).")

# COMMAND ----------

# MAGIC %md ## 1. Sweep order — hubs by topo level, then facts

# COMMAND ----------

def order_key(c):
    hub = providers.get(c["consumer_table"].lower())
    lvl = hub["topo_level"] if hub else 10**6   # facts last
    return (lvl, c["consumer_table"], c["fk_col"])

ordered = sorted(consumers, key=order_key)
print("Sweep order:")
for c in ordered:
    print(f"  {c['consumer_table']}.{c['fk_col']} -> {c['provider_table']}")

# COMMAND ----------

# MAGIC %md ## 2. Guarded hash-join MERGE per role

# COMMAND ----------

sweep_schema = T.StructType([T.StructField(n, t) for n, t in [
    ("run_id", T.StringType()), ("consumer_table", T.StringType()),
    ("fk_col", T.StringType()), ("provider_table", T.StringType()),
    ("action", T.StringType()), ("rows_updated", T.LongType()),
    ("post_check_violations", T.LongType()), ("run_at", T.TimestampType())]])
sweep_rows = []
errors = []
orphan_sk = int(w["orphan_sk"])

for c in ordered:
    p = providers[c["provider_table"].lower()]
    t, fk, pt, sk = c["consumer_table"], c["fk_col"], p["provider_table"], p["sk_col"]
    nk_col, ver_col = role_hash_cols(fk)
    cls = (c["classification"] or "").upper()
    if cls == "RELOADED":
        print(f"skip sweep {t}.{fk}: RELOADED (SKs already correct; hash present for the spine)")
        continue
    if cls != "LEGACY_KEYED":
        errors.append(f"{t}.{fk}: classification={cls or 'NOT SET'} — cannot sweep")
        continue

    if is_scd2(p) and path(p) == "A":
        join = f"f.`{ver_col}` = d.ver_hash"
    elif is_scd2(p):  # Path B — event date picks the version
        ev = c["event_date_col"]
        if not ev:
            errors.append(f"{t}.{fk}: Path B but event_date_col not set")
            continue
        join = (f"f.`{nk_col}` = d.nk_hash"
                f" AND f.`{ev}` >= d.`{p['effective_start_col']}`"
                f" AND f.`{ev}` <  {window_end_expr(p['effective_end_col'], 'd')}")
    else:  # SCD1
        join = f"f.`{nk_col}` = d.nk_hash"

    ctx.exec_mut(f"""
MERGE INTO {ctx.tgt(t)} f
USING {ctx.tgt(pt)} d
  ON {join}
WHEN MATCHED AND f.`{fk}` <> d.`{sk}`
  THEN UPDATE SET f.`{fk}` = d.`{sk}`""", f"sweep {t}.{fk}")

    updated = -1
    if not ctx.dry:
        m = last_merge_metrics(ctx.tgt(t))
        updated = int(m.get("numTargetRowsUpdated", -1))
        print(f"  {t}.{fk}: rows updated = {updated}")

    # orphans -> unknown member (idempotent guard), only when explicitly enabled
    if w["apply_orphan_sk"].lower() == "true":
        ctx.exec_mut(f"""
UPDATE {ctx.tgt(t)} SET `{fk}` = {orphan_sk}
WHERE `{nk_col}` IS NULL AND `{fk}` IS NOT NULL AND `{fk}` <> {orphan_sk}""",
            f"orphans -> {orphan_sk} on {t}.{fk}")

    # C5 post-sweep: SK anti-join to provider must be 0 (excluding orphan_sk)
    post = -1
    if not ctx.dry:
        post = scalar(ctx.query(f"""
          SELECT count(*) FROM {ctx.tgt(t)} f
          LEFT ANTI JOIN {ctx.tgt(pt)} d ON f.`{fk}` = d.`{sk}`
          WHERE f.`{fk}` IS NOT NULL AND f.`{fk}` <> {orphan_sk}"""))
        if post:
            errors.append(f"{t}.{fk}: POST-SWEEP anti-join = {post} (expected 0)")
        print(f"  {t}.{fk}: post-sweep SK anti-join = {post}")

    sweep_rows.append((RUN_ID, t, fk, pt, "SWEPT", updated, post,
                       datetime.datetime.utcnow()))

record_rows(ctx, "sweep_results", sweep_rows, sweep_schema)

# COMMAND ----------

# MAGIC %md ## 3. Outcome

# COMMAND ----------

display(spark.sql(f"""
  SELECT consumer_table, fk_col, provider_table, action, rows_updated, post_check_violations
  FROM {ctx.cfg('sweep_results')} WHERE run_id = '{RUN_ID}'
  ORDER BY consumer_table, fk_col"""))

if errors:
    raise Exception("05 finished with issues:\n" + "\n".join(errors))
print("""05 complete. Remaining checklist (§8):
  - re-run 04 (battery must still pass post-sweep)
  - REBUILD GOLD from repaired silver, re-run RI battery in gold
  - resume pipelines (silver loads first, then gold refresh)
  - keep: keymap schema (permanent), hash columns (forever), snapshots (per retention)
  - prevention: populate nk_hash at ingest in silver load logic from now on""")
