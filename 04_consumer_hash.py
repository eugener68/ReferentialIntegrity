# Databricks notebook source
# MAGIC %md
# MAGIC # 04 — Consumer Classification + Hash Population (Use Cases C, D-consumer)
# MAGIC
# MAGIC Two modes (widget `mode`):
# MAGIC - **classify** (read-only, run first): C1 evidence per consumer × FK role — how the
# MAGIC   consumer's current FK values land in the provider's key-map. Records evidence and a
# MAGIC   *suggestion*; the actual `classification` must be set by a human (attestation, §6 C1),
# MAGIC   via `classifications_json` in `00_setup`. Only **`repair_status=SELECTED`**
# MAGIC   rows run (when `repair_mode=opt_in`).
# MAGIC - **populate**: adds + populates `<fk>_nk_hash` (and `<fk>_ver_hash` for Path-A providers)
# MAGIC   per role, from key-map (LEGACY_KEYED, C2a) or current provider (RELOADED, C2b).
# MAGIC   **SKs are never touched here.** NULL hash = orphan report; nothing is invented.
# MAGIC
# MAGIC Roles classified MIXED (or unclassified) are skipped with a warning — STOP and
# MAGIC establish per-row provenance for those.

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

w = load_package_settings(require_saved=True)
ctx = Ctx(w)
mode = w["mode"].lower()
assert mode in ("classify", "populate"), mode

if mode == "classify":
    apply_setup_config(ctx, w, sections=("repair_selection",))
elif mode == "populate":
    apply_setup_config(ctx, w, sections=("classifications", "consumer_overrides", "repair_selection"))

providers = providers_by_name(ctx)
consumers = load_consumers(ctx, repair_phase="repair")
assert consumers, ("No consumers queued for repair — set repair_selection_json to SELECTED "
                   "(or repair_mode=opt_out). Query config_consumers for DISCOVERED rows.")
warnings, errors = [], []

def prov(c):
    p = providers.get(c["provider_table"].lower())
    assert p, f"{c['consumer_table']}.{c['fk_col']}: provider {c['provider_table']} not configured"
    return p

def is_scd2(p):  return p["archetype"] in ("SCD2", "HUB_SCD2")
def path(p):     return (p["version_match_path"] or "").upper()

# COMMAND ----------

# MAGIC %md ## Mode: classify — C1 evidence per consumer × role

# COMMAND ----------

if mode == "classify":
    ev_schema = T.StructType([T.StructField(n, t) for n, t in [
        ("run_id", T.StringType()), ("consumer_table", T.StringType()),
        ("fk_col", T.StringType()), ("provider_table", T.StringType()),
        ("map_status", T.StringType()), ("cnt", T.LongType()),
        ("suggested", T.StringType()), ("run_at", T.TimestampType())]])
    ev_rows = []
    thr = float(w["suggest_threshold"])

    for c in consumers:
        p = prov(c)
        t, fk = c["consumer_table"], c["fk_col"]
        dist = ctx.query(f"""
          SELECT coalesce(km.map_status, 'NOT_IN_LEGACY_DIM') AS status, count(*) c
          FROM {ctx.tgt(t)} f
          LEFT JOIN (SELECT DISTINCT old_sk, map_status FROM {ctx.km(p['provider_table'])}
                     WHERE old_sk IS NOT NULL) km
            ON f.`{fk}` = km.old_sk
          WHERE f.`{fk}` IS NOT NULL
          GROUP BY 1""", f"classify {t}.{fk}").collect()
        counts = {r.status: r.c for r in dist}
        total = sum(counts.values()) or 1
        in_legacy = counts.get("MATCHED", 0) + counts.get("ORPHAN_OLD", 0) + counts.get("AMBIGUOUS", 0)
        not_in = counts.get("NOT_IN_LEGACY_DIM", 0)
        if in_legacy / total >= thr:
            sug = "LEGACY_KEYED?"
        elif not_in / total > (1 - thr):
            sug = "RELOADED?"
        else:
            sug = "MIXED?"
        print(f"{t}.{fk} -> {p['provider_table']}: "
              + ", ".join(f"{k}={v}" for k, v in counts.items())
              + f"  | suggestion: {sug} (HUMAN MUST CONFIRM — ranges overlap, data alone can't prove this)")
        now = datetime.datetime.utcnow()
        for s, cnt in counts.items():
            ev_rows.append((RUN_ID, t, fk, p["provider_table"], s, cnt, sug, now))

    record_rows(ctx, "classification_evidence", ev_rows, ev_schema)

# COMMAND ----------

# COMMAND ----------

# MAGIC %md ## Mode: populate — C2a / C2b hash population per role (SK untouched)

# COMMAND ----------

if mode == "populate":
    for c in consumers:
        p = prov(c)
        t, fk = c["consumer_table"], c["fk_col"]
        cls = (c["classification"] or "").upper()
        if cls not in ("LEGACY_KEYED", "RELOADED"):
            warnings.append(f"SKIPPED {t}.{fk}: classification={cls or 'NOT SET'} "
                            f"(MIXED/unset -> establish provenance first)")
            continue

        nk_col, ver_col = role_hash_cols(fk)
        path_a = is_scd2(p) and path(p) == "A"
        cols = {nk_col: "BIGINT"}
        if path_a:
            cols[ver_col] = "BIGINT"
        ensure_columns(ctx, ctx.tgt(t), cols)
        guard = "" if w["recompute_hashes"].lower() == "true" else f" AND f.`{nk_col}` IS NULL"

        if cls == "LEGACY_KEYED":
            # C2a — via key-map on old_sk. Path B: member grain (MATCHED+AMBIGUOUS),
            # version resolution happens later via event date (§6 C2a note).
            if path_a:
                src = (f"SELECT old_sk, nk_hash, ver_hash FROM {ctx.km(p['provider_table'])} "
                       f"WHERE map_status = 'MATCHED' AND old_sk IS NOT NULL")
                sets = f"f.`{nk_col}` = km.nk_hash, f.`{ver_col}` = km.ver_hash"
            else:
                src = (f"SELECT DISTINCT old_sk, nk_hash FROM {ctx.km(p['provider_table'])} "
                       f"WHERE map_status IN ('MATCHED','AMBIGUOUS') AND old_sk IS NOT NULL")
                sets = f"f.`{nk_col}` = km.nk_hash"
            ctx.exec_mut(f"""
MERGE INTO {ctx.tgt(t)} f
USING ({src}) km
  ON f.`{fk}` = km.old_sk{guard}
WHEN MATCHED THEN UPDATE SET {sets}""", f"C2a populate {t}.{fk} (legacy-keyed)")
        else:
            # C2b — already-reloaded: hash from CURRENT provider on the SK itself.
            sets = f"f.`{nk_col}` = d.nk_hash" + (f", f.`{ver_col}` = d.ver_hash" if path_a else "")
            ver_sel = ", ver_hash" if path_a else ""
            ctx.exec_mut(f"""
MERGE INTO {ctx.tgt(t)} f
USING (SELECT `{p['sk_col']}` AS sk, nk_hash{ver_sel} FROM {ctx.tgt(p['provider_table'])}) d
  ON f.`{fk}` = d.sk{guard}
WHEN MATCHED THEN UPDATE SET {sets}""", f"C2b populate {t}.{fk} (reloaded)")

        if not ctx.dry:
            nulls = scalar(ctx.query(
                f"SELECT count(*) FROM {ctx.tgt(t)} WHERE `{nk_col}` IS NULL AND `{fk}` IS NOT NULL"))
            total = scalar(ctx.query(f"SELECT count(*) FROM {ctx.tgt(t)}"))
            print(f"{t}.{fk}: NULL {nk_col} = {nulls} of {total} rows (orphan report — do not invent values)")

# COMMAND ----------

# MAGIC %md ## Outcome

# COMMAND ----------

for x in warnings:
    print("⚠️ ", x)
if mode == "classify":
    display(spark.sql(f"""
      SELECT consumer_table, fk_col, provider_table, map_status, cnt, suggested
      FROM {ctx.cfg('classification_evidence')} WHERE run_id = '{RUN_ID}'
      ORDER BY consumer_table, fk_col, map_status"""))
    print("Review evidence, set repair_status=SELECTED + classifications_json in 00_setup, "
          "re-save, set mode=populate, then re-run this notebook.")
else:
    if errors:
        raise Exception("\n".join(errors))
    print("04 populate complete. Next: 05_validate (the hard gate).")
