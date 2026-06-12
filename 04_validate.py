# Databricks notebook source
# MAGIC %md
# MAGIC # 04 — Validation Battery (§6 C3, §7 D5) — THE HARD GATE
# MAGIC
# MAGIC Read-only. Per consumer × FK role:
# MAGIC 1. **coverage** (INFO): NULL-hash rows = the orphan report. Reconcile against expected orphans.
# MAGIC 2. **member RI** (PASS/FAIL): every populated `nk_hash` exists in the provider.
# MAGIC 3. **version RI** (PASS/FAIL): Path A — every `ver_hash` exists; Path B — every
# MAGIC    (hash, event_date) lands in a window (provider's no-overlap gate in 02 already
# MAGIC    guarantees it can't land in more than one).
# MAGIC 4. **measure reconciliation** (PASS/FAIL, optional): SUM(measures) per member,
# MAGIC    legacy vs target — catches values shifted between members, which RI checks can't see.
# MAGIC 5. **hub row consistency** (INFO): hub rows resolving some roles but orphaning others.
# MAGIC
# MAGIC The notebook **raises** if any FAIL — do not proceed to 05 until green + business sign-off.

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

w = create_widgets({
    "measure_tolerance": "0.01",   # abs diff tolerated per member in measure reconciliation
})
ctx = Ctx(w)
providers = providers_by_name(ctx)
consumers = load_consumers(ctx)
assert consumers, "No consumers in scope."

res_schema = T.StructType([T.StructField(n, t) for n, t in [
    ("run_id", T.StringType()), ("consumer_table", T.StringType()),
    ("fk_col", T.StringType()), ("provider_table", T.StringType()),
    ("check_name", T.StringType()), ("violations", T.LongType()),
    ("verdict", T.StringType()), ("detail", T.StringType()),
    ("run_at", T.TimestampType())]])
results = []

def rec(c, check, violations, verdict, detail=""):
    results.append((RUN_ID, c["consumer_table"], c["fk_col"], c["provider_table"],
                    check, int(violations), verdict, detail, datetime.datetime.utcnow()))
    mark = {"PASS": "PASS", "FAIL": "FAIL ❌", "INFO": "info"}[verdict]
    print(f"{mark:<8} {check:<24} {c['consumer_table']}.{c['fk_col']} (violations={violations}) {detail}")

def is_scd2(p):  return p["archetype"] in ("SCD2", "HUB_SCD2")
def path(p):     return (p["version_match_path"] or "").upper()

# COMMAND ----------

# MAGIC %md ## Per-role checks

# COMMAND ----------

for c in consumers:
    p = providers.get(c["provider_table"].lower())
    t, fk = c["consumer_table"], c["fk_col"]
    if not p:
        rec(c, "provider_configured", 1, "FAIL",
            f"provider {c['provider_table']} not in config_providers")
        continue
    pt = p["provider_table"]
    nk_col, ver_col = role_hash_cols(fk)
    have = {x.lower() for x in table_columns(ctx.tgt(t))}
    if nk_col.lower() not in have:
        rec(c, "hash_column_present", 1, "FAIL", f"{nk_col} missing — run 03 populate")
        continue

    # (1) coverage — orphan report
    nulls = scalar(ctx.query(f"""
      SELECT count(*) FROM {ctx.tgt(t)}
      WHERE `{nk_col}` IS NULL AND `{fk}` IS NOT NULL"""))
    rec(c, "coverage_null_hash", nulls, "INFO",
        "reconcile against expected orphans (ORPHAN_OLD / NOT_IN_LEGACY_DIM evidence)")

    # (2) member-level RI
    v = scalar(ctx.query(f"""
      SELECT count(*) FROM {ctx.tgt(t)} f
      LEFT ANTI JOIN {ctx.tgt(pt)} d ON f.`{nk_col}` = d.nk_hash
      WHERE f.`{nk_col}` IS NOT NULL"""))
    rec(c, "member_ri", v, "PASS" if v == 0 else "FAIL")

    # (3) version-level RI
    if is_scd2(p):
        if path(p) == "A":
            if ver_col.lower() in have:
                v = scalar(ctx.query(f"""
                  SELECT count(*) FROM {ctx.tgt(t)} f
                  LEFT ANTI JOIN {ctx.tgt(pt)} d ON f.`{ver_col}` = d.ver_hash
                  WHERE f.`{ver_col}` IS NOT NULL"""))
                rec(c, "version_ri_pathA", v, "PASS" if v == 0 else "FAIL")
                v2 = scalar(ctx.query(f"""
                  SELECT count(*) FROM {ctx.tgt(t)}
                  WHERE `{nk_col}` IS NOT NULL AND `{ver_col}` IS NULL"""))
                rec(c, "ver_hash_coverage", v2, "PASS" if v2 == 0 else "FAIL",
                    "member hash set but version hash missing")
            else:
                rec(c, "version_ri_pathA", 1, "FAIL", f"{ver_col} missing — run 03 populate")
        else:  # Path B — needs the fact's event date
            ev = c["event_date_col"]
            if not ev:
                rec(c, "version_ri_pathB", 1, "FAIL",
                    "event_date_col not set in config_consumers — required for Path B")
            else:
                # 02's no_window_overlap gate guarantees <=1 window per (hash, date);
                # the remaining failure mode is 0 windows -> anti-join on the as-of predicate.
                v = scalar(ctx.query(f"""
                  SELECT count(*) FROM {ctx.tgt(t)} f
                  LEFT ANTI JOIN {ctx.tgt(pt)} d
                    ON  f.`{nk_col}` = d.nk_hash
                   AND f.`{ev}` >= d.`{p['effective_start_col']}`
                   AND f.`{ev}` <  {window_end_expr(p['effective_end_col'], 'd')}
                  WHERE f.`{nk_col}` IS NOT NULL AND f.`{ev}` IS NOT NULL"""))
                rec(c, "version_ri_pathB_window", v, "PASS" if v == 0 else "FAIL",
                    "rows whose event date falls in NO provider window (D4 edge case — triage each)")
                v2 = scalar(ctx.query(f"""
                  SELECT count(*) FROM {ctx.tgt(t)}
                  WHERE `{nk_col}` IS NOT NULL AND `{ev}` IS NULL"""))
                rec(c, "event_date_nulls", v2, "PASS" if v2 == 0 else "FAIL",
                    f"`{ev}` NULL on hashed rows — Path B cannot resolve these")

    # (4) measure reconciliation per member vs legacy (optional)
    measures = list(c["measure_cols"] or [])
    if measures and (c["classification"] or "").upper() == "LEGACY_KEYED":
        tol = float(w["measure_tolerance"])
        sums_l = ", ".join(f"sum(f.`{m}`) AS l_{i}" for i, m in enumerate(measures))
        sums_t = ", ".join(f"sum(`{m}`) AS t_{i}" for i, m in enumerate(measures))
        diff = " OR ".join(
            f"abs(coalesce(l.l_{i},0) - coalesce(tg.t_{i},0)) > {tol}"
            for i in range(len(measures)))
        v = scalar(ctx.query(f"""
          WITH legacy AS (
            SELECT km.nk_hash, {sums_l}
            FROM {ctx.src(t)} f
            JOIN (SELECT DISTINCT old_sk, nk_hash FROM {ctx.km(pt)}
                  WHERE old_sk IS NOT NULL) km
              ON f.`{fk}` = km.old_sk
            GROUP BY km.nk_hash),
          target AS (
            SELECT `{nk_col}` AS nk_hash, {sums_t}
            FROM {ctx.tgt(t)} WHERE `{nk_col}` IS NOT NULL GROUP BY 1)
          SELECT count(*) FROM legacy l
          FULL OUTER JOIN target tg ON l.nk_hash = tg.nk_hash
          WHERE {diff}""", f"measure recon {t}.{fk}"))
        rec(c, "measure_reconciliation", v, "PASS" if v == 0 else "FAIL",
            f"members where SUM({measures}) differs legacy vs target")

# COMMAND ----------

# MAGIC %md ## Hub row-level consistency (D5) — all roles of one row resolve together

# COMMAND ----------

by_table = {}
for c in consumers:
    by_table.setdefault(c["consumer_table"], []).append(c)

for t, roles in by_table.items():
    if t.lower() not in providers or len(roles) < 1:
        continue  # only hubs (consumer tables that are themselves providers)
    have = {x.lower() for x in table_columns(ctx.tgt(t))}
    conds = []
    for c in roles:
        nk_col, _ = role_hash_cols(c["fk_col"])
        if nk_col.lower() in have:
            conds.append(f"(`{nk_col}` IS NULL AND `{c['fk_col']}` IS NOT NULL)")
    if not conds:
        continue
    v = scalar(ctx.query(
        f"SELECT count(*) FROM {ctx.tgt(t)} WHERE {' OR '.join(conds)}"))
    pseudo = dict(roles[0])
    pseudo["fk_col"] = "*ALL_ROLES*"
    rec(pseudo, "hub_row_consistency", v, "INFO",
        "hub rows with at least one unresolved role — half-repaired logical records")

# COMMAND ----------

# MAGIC %md ## Verdict — hard gate

# COMMAND ----------

record_rows(ctx, "validation_results", results, res_schema)
fails = [r for r in results if r[6] == "FAIL"]
display(spark.sql(f"""
  SELECT consumer_table, fk_col, check_name, violations, verdict, detail
  FROM {ctx.cfg('validation_results')} WHERE run_id = '{RUN_ID}'
  ORDER BY verdict, consumer_table, fk_col"""))

if fails:
    raise Exception(
        f"VALIDATION FAILED — {len(fails)} failing check(s). DO NOT SWEEP.\n" +
        "\n".join(f"  {f[1]}.{f[2]}: {f[4]} ({f[5]})" for f in fails))
print(f"Validation GREEN (run_id={RUN_ID}). Obtain business sign-off, then run 05_sweep.")
