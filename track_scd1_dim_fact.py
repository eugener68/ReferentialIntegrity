# Databricks notebook source
# MAGIC %md
# MAGIC # Track — SCD1 dimension + fact (linear runbook)
# MAGIC
# MAGIC Run **top to bottom**. Human **STOP** cells require action before continuing.
# MAGIC
# MAGIC | Phase | What |
# MAGIC |-------|------|
# MAGIC | A | Setup + discovery + triage |
# MAGIC | B | Legacy snapshot + WIP clone |
# MAGIC | C | Provider hash + key-map |
# MAGIC | D | Classify → attest → populate |
# MAGIC | E | Validate → sweep → validate |
# MAGIC | F | Promote → prod validate |
# MAGIC
# MAGIC **Prerequisites:** Git folder synced; federation to legacy SQL Server works.
# MAGIC
# MAGIC **Profile:** `scd1_dim_fact` — use **`00_setup_scd1`** for the widget panel.

# COMMAND ----------

# MAGIC %md
# MAGIC ## STOP 1 — Fill setup widgets
# MAGIC
# MAGIC 1. Open **`00_setup_scd1`** in another tab (or run the next cell once).
# MAGIC 2. Set **`01_target_catalog`**, **`02_target_schema`** (e.g. `gold`), **`15_providers_json`**.
# MAGIC 3. Set **`12_repair_target_mode`** = `wip_clone` (default).
# MAGIC 4. Run **`00_setup_scd1`** until "Package settings saved" appears.
# MAGIC 5. **Do not continue** until that succeeds.

# COMMAND ----------

# MAGIC %run ./00_setup_scd1

# COMMAND ----------

# MAGIC %md ## Phase A — Discovery

# COMMAND ----------

# MAGIC %run ./01_config_discovery

# COMMAND ----------

# MAGIC %md
# MAGIC ## STOP 2 — Pick consumers (interactive)
# MAGIC
# MAGIC 1. Open **`01b_repair_triage`**.
# MAGIC 2. Check the fact(s) you want to repair → set **`apply_changes=true`** → re-run.
# MAGIC 3. Confirm `repair_status=SELECTED` in the output table.
# MAGIC 4. Return here and run the next cell.

# COMMAND ----------

# MAGIC %run ./01b_repair_triage

# COMMAND ----------

# MAGIC %md ## Phase B — Snapshot + WIP clone

# COMMAND ----------

# MAGIC %run ./02_snapshot_diagnostic

# COMMAND ----------

# MAGIC %run ./02b_wip_clone

# COMMAND ----------

# MAGIC %md
# MAGIC ## STOP 3 — Note WIP run_id
# MAGIC
# MAGIC Copy the printed **`WIP clone run_id`** from **02b** output. Save to widget **`14_wip_run_id`**
# MAGIC in **`00_setup_scd1`** if you will re-run later stages without re-cloning.

# COMMAND ----------

# MAGIC %md ## Phase C — Provider hash + key-map

# COMMAND ----------

# MAGIC %run ./03_provider_hash_keymap

# COMMAND ----------

# MAGIC %md ## Phase D — Classify consumers

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

patch_package_settings({"mode": "classify"})

# COMMAND ----------

# MAGIC %run ./04_consumer_hash

# COMMAND ----------

# MAGIC %md
# MAGIC ## STOP 4 — Human attestation
# MAGIC
# MAGIC 1. Review **`classification_evidence`** output above.
# MAGIC 2. In **`00_setup_scd1`**, fill **`27_classifications_json`** per consumer×FK:
# MAGIC    ```json
# MAGIC    [{"consumer_table":"your_fact","fk_col":"your_sk_col","classification":"LEGACY_KEYED"}]
# MAGIC    ```
# MAGIC 3. Re-run **`00_setup_scd1`**, then continue below.

# COMMAND ----------

patch_package_settings({"mode": "populate"})

# COMMAND ----------

# MAGIC %run ./04_consumer_hash

# COMMAND ----------

# MAGIC %md ## Phase E — Validate + sweep + re-validate (WIP)

# COMMAND ----------

# MAGIC %run ./05_validate

# COMMAND ----------

# MAGIC %run ./06_sweep

# COMMAND ----------

# MAGIC %run ./05_validate

# COMMAND ----------

# MAGIC %md
# MAGIC ## STOP 5 — Promote prep
# MAGIC
# MAGIC 1. In **`00_setup_scd1`**, fill **`34_wip_row_keys_json`** for each consumer fact:
# MAGIC    ```json
# MAGIC    [{"table":"your_fact","row_key_cols":["your_primary_key_col"]}]
# MAGIC    ```
# MAGIC 2. Re-run **`00_setup_scd1`**, then continue.

# COMMAND ----------

# MAGIC %md ## Phase F — Promote + prod validation

# COMMAND ----------

# MAGIC %run ./07_promote

# COMMAND ----------

patch_package_settings({"validation_target": "prod"})

# COMMAND ----------

# MAGIC %run ./05_validate

# COMMAND ----------

print("""
Track complete (SCD1 dim + fact).

Sign-off checklist:
  - 05 on prod: no FAIL rows for your consumer(s)
  - wip_clones registry: status PROMOTED
  - Optional: DROP old WIP tables after retention period
""")
