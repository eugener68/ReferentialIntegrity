# Databricks notebook source
# MAGIC %md
# MAGIC # 00 — Setup (fact consumer profile)
# MAGIC
# MAGIC **Profile:** `fact_consumer` — repair **facts only** when the provider dim is already
# MAGIC hashed, key-mapped, and validated. Skips **02** snapshots and **03** provider prep.
# MAGIC
# MAGIC Providers must already exist in `config_providers` from a prior wave.
# MAGIC
# MAGIC **Linear alternative:** **`track_fact_consumer`**
# MAGIC
# MAGIC **Runbook:** `RUNBOOK_STAGES.md` → profile **fact_consumer**

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

PROFILE = "fact_consumer"
w = ensure_setup_widgets()
w["setup_profile"] = PROFILE
apply_profile_hidden_defaults(w, PROFILE)
ctx = Ctx(w)
run_setup_save(ctx, w)
