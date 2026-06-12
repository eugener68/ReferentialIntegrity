# Databricks notebook source
# MAGIC %md
# MAGIC # 00 — Setup (single widget panel for the whole package)
# MAGIC
# MAGIC **Widget panel:** labels are **`01_target_catalog` … `28_apply_orphan_sk`** (numbered
# MAGIC to match **RUNBOOK.md sections 1–28**). Open both side by side and fill top to bottom.
# MAGIC Re-run this notebook after pulling updates to refresh widget labels.
# MAGIC
# MAGIC **Operator runbook:** **`RUNBOOK.md`** — one section per widget, same order as the panel.
# MAGIC
# MAGIC **Edit the widgets below, then run this notebook once** (and again whenever you
# MAGIC change config). Settings are saved to `{target_catalog}.{config_schema}.package_settings`
# MAGIC and loaded automatically by every other notebook.
# MAGIC
# MAGIC **Run All is safe** — widget values are preserved across re-runs. After pulling
# MAGIC widget renames, run `reset_setup_widgets()` once in a cell to rebuild the panel.
# MAGIC
# MAGIC **Expected after a successful run:** schema `{config_schema}` contains table
# MAGIC `package_settings` (one row). Other config tables are created by **`01`**, not here.
# MAGIC
# MAGIC ### Placeholder table names (examples only — use **your** silver table names)
# MAGIC
# MAGIC | Placeholder | Archetype | What it stands for |
# MAGIC |---|---|---|
# MAGIC | `Dim_provider` | SCD1 | SCD1 dimension (key provider) |
# MAGIC | `SCD2_provider` | SCD2 | SCD2 table containing key(s) other SCD2/Hub tables reference |
# MAGIC | `HubSCD2_provider` | HUB_SCD2 | Provides its own SK and consumes dim FKs |
# MAGIC | `Fact_consumer` | — | Fact referencing a dim or hub |
# MAGIC | `SCD2_consumer` | — | Table referencing a provider (e.g. hub → dim) |
# MAGIC | `HubSCD2_consumer` | — | Fact referencing a hub |
# MAGIC
# MAGIC `sk_*` / `nkCol*` / `effectiveStartDate` are placeholder **column** names too — mirror
# MAGIC your real column names. Discovery matches consumers when `fk_col` equals the provider's `sk_col`.
# MAGIC
# MAGIC ### JSON widgets (compact one-line JSON arrays)
# MAGIC
# MAGIC **`providers_json`** — register key-providing tables (required before discovery):
# MAGIC ```json
# MAGIC [{"provider_table":"Dim_provider","archetype":"SCD1","sk_col":"sk_Dim_provider","nk_cols":["nkCol1"],"topo_level":0},{"provider_table":"SCD2_provider","archetype":"SCD2","sk_col":"sk_SCD2_provider","nk_cols":["nkCol1"],"effective_start_col":"effectiveStartDate","effective_end_col":"effectiveEndDate","topo_level":0},{"provider_table":"HubSCD2_provider","archetype":"HUB_SCD2","sk_col":"sk_HubSCD2_provider","nk_cols":["nkCol1","nkCol2"],"effective_start_col":"effectiveStartDate","effective_end_col":"effectiveEndDate","topo_level":1}]
# MAGIC ```
# MAGIC
# MAGIC **`manual_consumers_json`** — role-played / renamed FKs not found by auto-discovery:
# MAGIC ```json
# MAGIC [{"consumer_table":"Fact_consumer","fk_col":"sk_SCD2_provider_roleB","provider_table":"SCD2_provider","event_date_col":"eventDateCol"}]
# MAGIC ```
# MAGIC
# MAGIC **`exclude_consumers_json`** — false-positive auto-discovered consumers:
# MAGIC ```json
# MAGIC [{"consumer_table":"scratch_load","fk_col":"sk_SCD2_provider","reason":"not a real consumer"}]
# MAGIC ```
# MAGIC
# MAGIC **`consumer_overrides_json`** — event dates / measures on auto-discovered rows (after discovery):
# MAGIC ```json
# MAGIC [{"consumer_table":"Fact_consumer","fk_col":"sk_SCD2_provider","event_date_col":"eventDateCol","measure_cols":["measureCol1"]},{"consumer_table":"SCD2_consumer","fk_col":"sk_SCD2_provider","event_date_col":"effectiveStartDate"}]
# MAGIC ```
# MAGIC
# MAGIC **`repair_selection_json`** — batch queue updates (alternative to `01b_repair_triage`):
# MAGIC ```json
# MAGIC [{"consumer_table":"Fact_consumer","fk_col":"sk_SCD2_provider","repair_status":"SELECTED"}]
# MAGIC ```
# MAGIC
# MAGIC **`repair_mode`** — `opt_in` (default): only `SELECTED`/`VERIFIED` rows run 04–06.
# MAGIC
# MAGIC **`classifications_json`** — attestation after 04 classify (before 04 populate):
# MAGIC ```json
# MAGIC [{"consumer_table":"Fact_consumer","fk_col":"sk_SCD2_provider","classification":"LEGACY_KEYED","note":"not reloaded per migration log"},{"consumer_table":"HubSCD2_consumer","fk_col":"sk_HubSCD2_provider","classification":"LEGACY_KEYED"}]
# MAGIC ```
# MAGIC
# MAGIC Pick repair queue via **`01b_repair_triage`** (multiselect), **`repair_selection_json`**, or SQL.
# MAGIC
# MAGIC Pipeline toggles (`mode`, `dry_run`, `target_schema`, filters, …) are also here —
# MAGIC change `mode` to `populate` when ready for hash population, without editing notebook 04.

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

w = ensure_setup_widgets()
ctx = Ctx(w)
cat = w["target_catalog"]

# COMMAND ----------

# MAGIC %md ## Save settings to Delta

# COMMAND ----------

if not cat or cat == "target_catalog":
    raise ValueError(
        "Set widget 01_target_catalog to your real Unity Catalog name before saving "
        "(placeholder 'target_catalog' is not valid)."
    )

spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{cat}`.`{w['config_schema']}` "
          f"COMMENT 'RI repair: config + audit/evidence tables'")
if w["dry_run"].lower() == "true":
    print("NOTE: 10_dry_run=true does not block saving package_settings (config always persists).")
save_package_settings(ctx, w)
fqn = verify_package_settings(w)

print("Package settings saved.")
print(f"  table: {fqn}")
print(f"  providers: {len(parse_json_widget(w, 'providers_json'))}")
print(f"  manual consumers: {len(parse_json_widget(w, 'manual_consumers_json'))}")
print(f"  classifications: {len(parse_json_widget(w, 'classifications_json'))}")
display(spark.sql(f"SHOW TABLES IN `{cat}`.`{w['config_schema']}`"))
print("\nNext: run 01_config_discovery (creates remaining config tables + applies JSON config).")
