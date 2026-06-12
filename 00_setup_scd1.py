# Databricks notebook source
# MAGIC %md
# MAGIC # 00 — Setup (SCD1 dim + fact profile)
# MAGIC
# MAGIC **Profile:** `scd1_dim_fact` — ~27 widgets that matter (not all 37).
# MAGIC
# MAGIC Use when repairing an **SCD1 dimension** and **one or more facts** that reference it.
# MAGIC No Path A/B widgets, no `consumer_overrides_json` unless you have renamed FKs.
# MAGIC
# MAGIC **Linear alternative:** run **`track_scd1_dim_fact`** top-to-bottom after filling widgets here.
# MAGIC
# MAGIC **Runbook:** `RUNBOOK_STAGES.md` → profile **scd1_dim_fact** + stage guides.

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

PROFILE = "scd1_dim_fact"
w = ensure_setup_widgets()
w["setup_profile"] = PROFILE
apply_profile_hidden_defaults(w, PROFILE)
ctx = Ctx(w)
run_setup_save(ctx, w)
