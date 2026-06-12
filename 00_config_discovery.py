# Databricks notebook source
# MAGIC %md
# MAGIC # 00 — Config & Consumer Discovery
# MAGIC
# MAGIC Creates the repair schemas and config/result tables, lets you register **providers**
# MAGIC (dims / SCD2s / hubs with their natural keys), then **auto-discovers consumers**:
# MAGIC every table in the target schema carrying a column with the same name as a
# MAGIC provider's surrogate key (mirrored-name assumption). Edge cases (role-played or
# MAGIC renamed FK columns) are surfaced to a review table and added manually.
# MAGIC
# MAGIC Read-only on silver/gold. Safe to run anytime.

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

w = create_widgets()
ctx = Ctx(w)
cat = w["target_catalog"]

# COMMAND ----------

# MAGIC %md ## 1. Schemas + config & result tables (idempotent)

# COMMAND ----------

for schema, comment in [
    (w["config_schema"],  "RI repair: config + audit/evidence tables"),
    (w["staging_schema"], "RI repair: legacy snapshots (retire after sign-off + retention)"),
    (w["keymap_schema"],  "RI repair: key-maps (KEEP PERMANENTLY - lineage/audit record)"),
]:
    ctx.exec_mut(f"CREATE SCHEMA IF NOT EXISTS `{cat}`.`{schema}` COMMENT '{comment}'",
                 f"schema {schema}")

# COMMAND ----------

ddl = {
"config_providers": f"""
CREATE TABLE IF NOT EXISTS {ctx.cfg('config_providers')} (
  provider_table     STRING  NOT NULL,  -- table name (mirrored source<->target)
  archetype          STRING  NOT NULL,  -- 'SCD1' | 'SCD2' | 'HUB_SCD2'
  sk_col             STRING  NOT NULL,  -- surrogate key column (e.g. keyAccount)
  nk_cols            ARRAY<STRING> NOT NULL,  -- natural key columns (USER-FED, ordered)
  nk_type_overrides  MAP<STRING,STRING>,      -- col -> 'date'|'timestamp'|'bigint'|'decimal(p,s)'
  effective_start_col STRING,           -- SCD2/HUB only
  effective_end_col   STRING,           -- SCD2/HUB only (NULL end = open)
  record_status_col   STRING,           -- optional
  use_status_tiebreaker BOOLEAN,        -- only if (nk,start) not unique; categorical match only
  version_match_path  STRING,           -- 'A' | 'B' (set by 01 diagnostic or manually)
  topo_level          INT,              -- 0 = leaf dim, 1+ = hubs (providers-first ordering)
  enabled             BOOLEAN,
  notes               STRING,
  updated_at          TIMESTAMP
) USING DELTA""",

"config_consumers": f"""
CREATE TABLE IF NOT EXISTS {ctx.cfg('config_consumers')} (
  consumer_table   STRING NOT NULL,
  fk_col           STRING NOT NULL,   -- FK column in the consumer (usually == provider sk_col)
  provider_table   STRING NOT NULL,
  event_date_col   STRING,            -- fact event date (Path B); hubs default to own start col
  measure_cols     ARRAY<STRING>,     -- optional: enables measure reconciliation in 04
  classification   STRING,            -- 'LEGACY_KEYED' | 'RELOADED' | 'MIXED' (set after 03 evidence)
  discovered_by    STRING,            -- 'AUTO' | 'MANUAL'
  excluded         BOOLEAN,
  exclusion_reason STRING,
  notes            STRING,
  updated_at       TIMESTAMP
) USING DELTA""",

"discovery_edge_cases": f"""
CREATE TABLE IF NOT EXISTS {ctx.cfg('discovery_edge_cases')} (
  run_id STRING, provider_table STRING, sk_col STRING,
  candidate_table STRING, candidate_column STRING,
  reason STRING, status STRING,   -- 'REVIEW' -> set 'ADDED' or 'IGNORED' after triage
  found_at TIMESTAMP
) USING DELTA""",

"version_diagnostics": f"""
CREATE TABLE IF NOT EXISTS {ctx.cfg('version_diagnostics')} (
  run_id STRING, provider_table STRING, status STRING, version_rows BIGINT,
  matched_pct DOUBLE, suggested_path STRING, run_at TIMESTAMP
) USING DELTA""",

"gate_results": f"""
CREATE TABLE IF NOT EXISTS {ctx.cfg('gate_results')} (
  run_id STRING, table_name STRING, gate STRING,
  violations BIGINT, passed BOOLEAN, run_at TIMESTAMP
) USING DELTA""",

"keymap_audit": f"""
CREATE TABLE IF NOT EXISTS {ctx.cfg('keymap_audit')} (
  run_id STRING, provider_table STRING, map_status STRING, cnt BIGINT, run_at TIMESTAMP
) USING DELTA""",

"classification_evidence": f"""
CREATE TABLE IF NOT EXISTS {ctx.cfg('classification_evidence')} (
  run_id STRING, consumer_table STRING, fk_col STRING, provider_table STRING,
  map_status STRING, cnt BIGINT, suggested STRING, run_at TIMESTAMP
) USING DELTA""",

"validation_results": f"""
CREATE TABLE IF NOT EXISTS {ctx.cfg('validation_results')} (
  run_id STRING, consumer_table STRING, fk_col STRING, provider_table STRING,
  check_name STRING, violations BIGINT, verdict STRING,  -- 'PASS'|'FAIL'|'INFO'
  detail STRING, run_at TIMESTAMP
) USING DELTA""",

"sweep_results": f"""
CREATE TABLE IF NOT EXISTS {ctx.cfg('sweep_results')} (
  run_id STRING, consumer_table STRING, fk_col STRING, provider_table STRING,
  action STRING, rows_updated BIGINT, post_check_violations BIGINT, run_at TIMESTAMP
) USING DELTA""",
}

for name, stmt in ddl.items():
    ctx.exec_mut(stmt, f"create {name}")

# COMMAND ----------

# MAGIC %md ## 2. Register providers (USER INPUT — natural keys fed here)

# COMMAND ----------

def upsert_provider(provider_table, archetype, sk_col, nk_cols,
                    nk_type_overrides=None, effective_start_col=None,
                    effective_end_col=None, record_status_col=None,
                    use_status_tiebreaker=False, version_match_path=None,
                    topo_level=0, enabled=True, notes=None):
    assert archetype in ("SCD1", "SCD2", "HUB_SCD2"), archetype
    if archetype in ("SCD2", "HUB_SCD2"):
        assert effective_start_col, f"{provider_table}: SCD2/HUB requires effective_start_col"
    schema = T.StructType([
        T.StructField("provider_table", T.StringType()),
        T.StructField("archetype", T.StringType()),
        T.StructField("sk_col", T.StringType()),
        T.StructField("nk_cols", T.ArrayType(T.StringType())),
        T.StructField("nk_type_overrides", T.MapType(T.StringType(), T.StringType())),
        T.StructField("effective_start_col", T.StringType()),
        T.StructField("effective_end_col", T.StringType()),
        T.StructField("record_status_col", T.StringType()),
        T.StructField("use_status_tiebreaker", T.BooleanType()),
        T.StructField("version_match_path", T.StringType()),
        T.StructField("topo_level", T.IntegerType()),
        T.StructField("enabled", T.BooleanType()),
        T.StructField("notes", T.StringType()),
        T.StructField("updated_at", T.TimestampType()),
    ])
    row = [(provider_table, archetype, sk_col, list(nk_cols),
            nk_type_overrides or {}, effective_start_col, effective_end_col,
            record_status_col, use_status_tiebreaker, version_match_path,
            topo_level, enabled, notes, datetime.datetime.utcnow())]
    spark.createDataFrame(row, schema).createOrReplaceTempView("_prov_upsert")
    spark.sql(f"""
      MERGE INTO {ctx.cfg('config_providers')} t
      USING _prov_upsert s ON lower(t.provider_table) = lower(s.provider_table)
      WHEN MATCHED THEN UPDATE SET *
      WHEN NOT MATCHED THEN INSERT *""")
    print(f"provider upserted: {provider_table} ({archetype}, sk={sk_col}, nk={nk_cols})")

# COMMAND ----------

# EXAMPLES — uncomment, adapt, run. Natural keys are the one thing only you know.
#
# upsert_provider("dimAccount", "SCD2", "keyAccount",
#                 nk_cols=["accountNumber"],
#                 effective_start_col="effectiveStartDate",
#                 effective_end_col="effectiveEndDate",
#                 record_status_col="recordStatus",
#                 topo_level=0)
#
# upsert_provider("dimCustomer", "SCD1", "keyCustomer",
#                 nk_cols=["customerCode"], topo_level=0)
#
# upsert_provider("dimRate", "SCD2", "keyRate",
#                 nk_cols=["rateCode", "rateClass"],
#                 nk_type_overrides={"rateClass": "bigint"},
#                 effective_start_col="effectiveStartDate",
#                 effective_end_col="effectiveEndDate", topo_level=0)
#
# upsert_provider("hubCustomerAccount", "HUB_SCD2", "keyCustomerAccount",
#                 nk_cols=["accountNumber", "customerCode"],
#                 effective_start_col="effectiveStartDate",
#                 effective_end_col="effectiveEndDate",
#                 record_status_col="recordStatus",
#                 topo_level=1)   # references level-0 dims

display(spark.table(ctx.cfg("config_providers").replace("`", "")))

# COMMAND ----------

# MAGIC %md ## 3. Auto-discover consumers by surrogate-key column name
# MAGIC For each provider, every table in the target schema with a column named exactly
# MAGIC like the provider's `sk_col` is registered as a consumer of that provider
# MAGIC (insert-only MERGE — manual edits/exclusions are never overwritten).

# COMMAND ----------

providers = load_providers(ctx)
prov_idx = providers_by_name(ctx)
assert providers, "No enabled providers registered — fill section 2 first."

cols_df = spark.sql(f"""
  SELECT table_name, column_name
  FROM `{cat}`.information_schema.columns
  WHERE table_schema = '{w["target_schema"]}'
""").collect()
all_cols = [(r.table_name, r.column_name) for r in cols_df]

discovered = []
for p in providers:
    sk = p["sk_col"].lower()
    for tbl, col in all_cols:
        if col.lower() == sk and tbl.lower() != p["provider_table"].lower():
            # hubs consuming dims: default event date = the hub's own validity start
            hub = prov_idx.get(tbl.lower())
            ev = hub["effective_start_col"] if hub else None
            discovered.append((tbl, col, p["provider_table"], ev))

print(f"discovered {len(discovered)} consumer x role pairs")
if discovered:
    schema = T.StructType([
        T.StructField("consumer_table", T.StringType()),
        T.StructField("fk_col", T.StringType()),
        T.StructField("provider_table", T.StringType()),
        T.StructField("event_date_col", T.StringType()),
    ])
    spark.createDataFrame(discovered, schema).createOrReplaceTempView("_disc")
    ctx.exec_mut(f"""
      MERGE INTO {ctx.cfg('config_consumers')} t
      USING _disc s
        ON lower(t.consumer_table) = lower(s.consumer_table)
       AND lower(t.fk_col) = lower(s.fk_col)
      WHEN NOT MATCHED THEN INSERT
        (consumer_table, fk_col, provider_table, event_date_col, measure_cols,
         classification, discovered_by, excluded, exclusion_reason, notes, updated_at)
      VALUES (s.consumer_table, s.fk_col, s.provider_table, s.event_date_col, NULL,
              NULL, 'AUTO', false, NULL, NULL, current_timestamp())""",
      "register discovered consumers (insert-only)")

# COMMAND ----------

# MAGIC %md ## 4. Edge-case scan — near-miss FK column names
# MAGIC Role-played or renamed FKs (e.g. `keyAccountShipTo`, `parentAccountKey`) won't match
# MAGIC exactly. Columns whose name *contains* the SK stem but isn't the SK are reported
# MAGIC here for human triage; add real ones via `add_consumer()` below.

# COMMAND ----------

def sk_stem(sk_col):
    s = re.sub(r"^(key|sk|id)_?", "", sk_col, flags=re.I)
    s = re.sub(r"_?(key|sk|id)$", "", s, flags=re.I)
    return s.lower()

registered = {(c["consumer_table"].lower(), c["fk_col"].lower())
              for c in load_consumers(ctx, include_excluded=True)}
edge_rows = []
for p in providers:
    stem = sk_stem(p["sk_col"])
    if len(stem) < 4:   # too generic to scan
        continue
    for tbl, col in all_cols:
        cl = col.lower()
        if (stem in cl and cl != p["sk_col"].lower()
                and re.search(r"(^|_)(key|sk|id)|(key|sk|id)$", cl)
                and (tbl.lower(), cl) not in registered
                and tbl.lower() != p["provider_table"].lower()):
            edge_rows.append((RUN_ID, p["provider_table"], p["sk_col"], tbl, col,
                              f"column contains stem '{stem}' + key marker but != sk_col",
                              "REVIEW", datetime.datetime.utcnow()))

if edge_rows:
    schema = T.StructType([T.StructField(n, t) for n, t in [
        ("run_id", T.StringType()), ("provider_table", T.StringType()),
        ("sk_col", T.StringType()), ("candidate_table", T.StringType()),
        ("candidate_column", T.StringType()), ("reason", T.StringType()),
        ("status", T.StringType()), ("found_at", T.TimestampType())]])
    record_rows(ctx, "discovery_edge_cases", edge_rows, schema)
print(f"{len(edge_rows)} edge-case candidates recorded for review")
display(spark.sql(f"SELECT * FROM {ctx.cfg('discovery_edge_cases')} WHERE status='REVIEW'"))

# COMMAND ----------

def add_consumer(consumer_table, fk_col, provider_table, event_date_col=None,
                 measure_cols=None, notes="manual edge case"):
    spark.sql(f"""
      MERGE INTO {ctx.cfg('config_consumers')} t
      USING (SELECT '{consumer_table}' ct, '{fk_col}' fk) s
        ON lower(t.consumer_table)=lower(s.ct) AND lower(t.fk_col)=lower(s.fk)
      WHEN NOT MATCHED THEN INSERT
        (consumer_table, fk_col, provider_table, event_date_col, measure_cols,
         classification, discovered_by, excluded, exclusion_reason, notes, updated_at)
      VALUES ('{consumer_table}', '{fk_col}', '{provider_table}',
              {f"'{event_date_col}'" if event_date_col else 'NULL'},
              {("array(" + ",".join(f"'{m}'" for m in measure_cols) + ")") if measure_cols else "NULL"},
              NULL, 'MANUAL', false, NULL, '{notes}', current_timestamp())""")
    print(f"consumer added: {consumer_table}.{fk_col} -> {provider_table}")

def exclude_consumer(consumer_table, fk_col, reason):
    spark.sql(f"""
      UPDATE {ctx.cfg('config_consumers')}
      SET excluded = true, exclusion_reason = '{reason}', updated_at = current_timestamp()
      WHERE lower(consumer_table)=lower('{consumer_table}') AND lower(fk_col)=lower('{fk_col}')""")
    print(f"consumer excluded: {consumer_table}.{fk_col} ({reason})")

# add_consumer("factShipments", "keyAccountShipTo", "dimAccount", event_date_col="shipDate")
# exclude_consumer("tmpAccountLoad", "keyAccount", "scratch table, not a real consumer")

# COMMAND ----------

# MAGIC %md ## 5. Review

# COMMAND ----------

display(spark.sql(f"""
  SELECT c.consumer_table, c.fk_col, c.provider_table, p.archetype,
         c.event_date_col, c.classification, c.discovered_by, c.excluded
  FROM {ctx.cfg('config_consumers')} c
  LEFT JOIN {ctx.cfg('config_providers')} p
    ON lower(c.provider_table)=lower(p.provider_table)
  ORDER BY c.provider_table, c.consumer_table, c.fk_col"""))
