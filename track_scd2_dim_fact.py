# Databricks notebook source
# MAGIC %md
# MAGIC # Track — SCD2 dimension + fact (linear runbook)
# MAGIC
# MAGIC Same flow as **SCD1 track**, plus:
# MAGIC - **02** records Path A vs B from version diagnostic
# MAGIC - **`consumer_overrides_json`** must include **`event_date_col`** for Path B facts
# MAGIC
# MAGIC **Profile:** `scd2_dim_fact` — use **`00_setup_scd2`**.

# COMMAND ----------

# MAGIC %md
# MAGIC ## STOP 1 — Fill setup widgets in **`00_setup_scd2`**
# MAGIC
# MAGIC Include **`effective_start_col` / `effective_end_col`** in **`15_providers_json`**.
# MAGIC Set **`consumer_overrides_json`** with **`event_date_col`** if Path B is likely.

# COMMAND ----------

# MAGIC %run ./00_setup_scd2

# COMMAND ----------

# MAGIC %run ./01_config_discovery

# COMMAND ----------

# MAGIC %md ## STOP 2 — Pick consumers in **`01b_repair_triage`** (`apply_changes=true`)

# COMMAND ----------

# MAGIC %run ./01b_repair_triage

# COMMAND ----------

# MAGIC %run ./02_snapshot_diagnostic

# COMMAND ----------

# MAGIC %md ## STOP 2b — Review Path A/B
# MAGIC
# MAGIC Check **02** output: `suggested Path A` or `B`. If wrong, set **`version_match_path`** in
# MAGIC **`providers_json`** or SQL on `config_providers`, then re-run **03**.

# COMMAND ----------

# MAGIC %run ./02b_wip_clone

# COMMAND ----------

# MAGIC %run ./03_provider_hash_keymap

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

patch_package_settings({"mode": "classify"})

# COMMAND ----------

# MAGIC %run ./04_consumer_hash

# COMMAND ----------

# MAGIC %md ## STOP 3 — Attest **`classifications_json`** in **`00_setup_scd2`**, re-run setup

# COMMAND ----------

patch_package_settings({"mode": "populate"})

# COMMAND ----------

# MAGIC %run ./04_consumer_hash

# COMMAND ----------

# MAGIC %run ./05_validate

# COMMAND ----------

# MAGIC %run ./06_sweep

# COMMAND ----------

# MAGIC %run ./05_validate

# COMMAND ----------

# MAGIC %md ## STOP 4 — **`wip_row_keys_json`** in **`00_setup_scd2`**, re-run setup

# COMMAND ----------

# MAGIC %run ./07_promote

# COMMAND ----------

patch_package_settings({"validation_target": "prod"})

# COMMAND ----------

# MAGIC %run ./05_validate

# COMMAND ----------

print("Track complete (SCD2 dim + fact).")
