# Databricks notebook source
# MAGIC %md
# MAGIC # Track — Fact consumer only (linear runbook)
# MAGIC
# MAGIC Use when the **provider dimension is already repaired** (hashes + key-map exist) and you
# MAGIC only need to fix **fact FK columns**.
# MAGIC
# MAGIC **Skips:** **02** legacy snapshot, **03** provider hash/key-map (clone still includes provider
# MAGIC for sweep joins).
# MAGIC
# MAGIC **Profile:** `fact_consumer` — use **`00_setup_fact`**.

# COMMAND ----------

# MAGIC %md
# MAGIC ## STOP 1 — Fill **`00_setup_fact`**
# MAGIC
# MAGIC Provider must already be in **`config_providers`**. Set **`consumer_filter`** to your fact.

# COMMAND ----------

# MAGIC %run ./00_setup_fact

# COMMAND ----------

# MAGIC %run ./01_config_discovery

# COMMAND ----------

# MAGIC %md ## STOP 2 — Pick consumers in **`01b_repair_triage`**

# COMMAND ----------

# MAGIC %run ./01b_repair_triage

# COMMAND ----------

# MAGIC %run ./02b_wip_clone

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

patch_package_settings({"mode": "classify"})

# COMMAND ----------

# MAGIC %run ./04_consumer_hash

# COMMAND ----------

# MAGIC %md ## STOP 3 — Attest **`classifications_json`**, re-run **`00_setup_fact`**

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

# MAGIC %md ## STOP 4 — **`wip_row_keys_json`**, re-run **`00_setup_fact`**

# COMMAND ----------

# MAGIC %run ./07_promote

# COMMAND ----------

patch_package_settings({"validation_target": "prod"})

# COMMAND ----------

# MAGIC %run ./05_validate

# COMMAND ----------

print("Track complete (fact consumer only).")
