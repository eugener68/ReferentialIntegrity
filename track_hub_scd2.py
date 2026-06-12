# Databricks notebook source
# MAGIC %md
# MAGIC # Track — SCD2 dim + HUB_SCD2 + fact(s) (linear runbook)
# MAGIC
# MAGIC For tables where a **hub** is both:
# MAGIC - a **provider** (facts reference its SK), and
# MAGIC - a **consumer** (hub rows reference dim/SCD2 SKs as FK columns)
# MAGIC
# MAGIC **Sweep order (06):** dim FK roles on hub first (`topo_level`), then facts referencing hub.
# MAGIC The hub's **own SK is never swept** — only FK columns inside it.
# MAGIC
# MAGIC **Profile:** `hub_scd2_wave` — use **`00_setup_hub`**. Detail: **`RUNBOOK_STAGES.md` → Hub SCD2**.

# COMMAND ----------

# MAGIC %md
# MAGIC ## STOP 1 — Fill setup widgets in **`00_setup_hub`**
# MAGIC
# MAGIC **`15_providers_json`** — include dim **and** hub, e.g.:
# MAGIC ```json
# MAGIC [
# MAGIC   {"provider_table":"dimAccount","archetype":"SCD2","sk_col":"keyAccount","nk_cols":["accountNumber"],"effective_start_col":"effectiveStartDate","effective_end_col":"effectiveEndDate","topo_level":0},
# MAGIC   {"provider_table":"hubAccountContact","archetype":"HUB_SCD2","sk_col":"keyAccountContact","nk_cols":["accountNumber","contactId"],"effective_start_col":"effectiveStartDate","effective_end_col":"effectiveEndDate","topo_level":1}
# MAGIC ]
# MAGIC ```
# MAGIC
# MAGIC Use **`09_provider_filter`** / **`10_consumer_filter`** to scope the pilot if needed.

# COMMAND ----------

# MAGIC %run ./00_setup_hub

# COMMAND ----------

# MAGIC %run ./01_config_discovery

# COMMAND ----------

# MAGIC %md
# MAGIC ## STOP 2 — Pick consumers in **`01b_repair_triage`**
# MAGIC
# MAGIC SELECT **all** roles you intend to repair:
# MAGIC 1. **Hub × dim FK** rows — e.g. `hubAccountContact.keyAccount -> dimAccount`
# MAGIC 2. **Fact × hub** rows — e.g. `factSales.keyAccountContact -> hubAccountContact`
# MAGIC 3. **Fact × dim** rows (if any direct dim FKs on facts)
# MAGIC
# MAGIC Set **`apply_changes=true`** and re-run **01b**.

# COMMAND ----------

# MAGIC %run ./01b_repair_triage

# COMMAND ----------

# MAGIC %run ./02_snapshot_diagnostic

# COMMAND ----------

# MAGIC %md
# MAGIC ## STOP 2b — Review Path A/B (dim **and** hub)
# MAGIC
# MAGIC **02** runs version diagnostic for **each** SCD2/HUB provider. Confirm Path A/B per table.
# MAGIC Hubs need their **own** path choice independent of the dim they reference.

# COMMAND ----------

# MAGIC %run ./02b_wip_clone

# COMMAND ----------

# MAGIC %run ./03_provider_hash_keymap

# COMMAND ----------

# MAGIC %md
# MAGIC ## STOP 2c — Hub FK event dates (before populate)
# MAGIC
# MAGIC For Path B, set **`28_consumer_overrides_json`** for **hub internal FK roles** — often
# MAGIC `event_date_col` = hub's **`effective_start_col`** (version valid when hub version started):
# MAGIC ```json
# MAGIC [{"consumer_table":"hubAccountContact","fk_col":"keyAccount","event_date_col":"effectiveStartDate"}]
# MAGIC ```
# MAGIC Re-run **`00_setup_hub`**, then continue.

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

patch_package_settings({"mode": "classify"})

# COMMAND ----------

# MAGIC %run ./04_consumer_hash

# COMMAND ----------

# MAGIC %md
# MAGIC ## STOP 3 — Attest **`27_classifications_json`** per role (hub + facts)
# MAGIC
# MAGIC One JSON object **per consumer_table × fk_col** (not per table). Re-run **`00_setup_hub`**.

# COMMAND ----------

patch_package_settings({"mode": "populate"})

# COMMAND ----------

# MAGIC %run ./04_consumer_hash

# COMMAND ----------

# MAGIC %run ./05_validate

# COMMAND ----------

# MAGIC %md
# MAGIC ## STOP 3b — Hub row consistency (INFO)
# MAGIC
# MAGIC If **05** prints **`hub_row_consistency`** INFO > 0, some hub rows resolve one FK role
# MAGIC but orphan another — fix populate/sweep for the failing role before **06**.

# COMMAND ----------

# MAGIC %run ./06_sweep

# COMMAND ----------

# MAGIC %md
# MAGIC Confirm **06** sweep order: hub FK roles before facts. Check `rows updated` per role.

# COMMAND ----------

# MAGIC %run ./05_validate

# COMMAND ----------

# MAGIC %md ## STOP 4 — **`34_wip_row_keys_json`** for hub + facts, re-run **`00_setup_hub`**

# COMMAND ----------

# MAGIC %run ./07_promote

# COMMAND ----------

patch_package_settings({"validation_target": "prod"})

# COMMAND ----------

# MAGIC %run ./05_validate

# COMMAND ----------

print("Track complete (HUB_SCD2 wave).")
