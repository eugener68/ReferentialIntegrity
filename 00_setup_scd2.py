# Databricks notebook source
# MAGIC %md
# MAGIC # 00 — Setup (SCD2 dim + fact profile)
# MAGIC
# MAGIC **Profile:** `scd2_dim_fact` — includes Path A/B diagnostic widgets and
# MAGIC **`consumer_overrides_json`** (`event_date_col` for Path B facts).
# MAGIC
# MAGIC **Linear alternative:** **`track_scd2_dim_fact`**
# MAGIC
# MAGIC **Runbook:** `RUNBOOK_STAGES.md` → profile **scd2_dim_fact**

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

PROFILE = "scd2_dim_fact"
w = ensure_setup_widgets()
w["setup_profile"] = PROFILE
apply_profile_hidden_defaults(w, PROFILE)
ctx = Ctx(w)
run_setup_save(ctx, w)
