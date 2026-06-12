# Databricks notebook source
# MAGIC %md
# MAGIC # 00 — Setup (HUB_SCD2 wave profile)
# MAGIC
# MAGIC **Profile:** `hub_scd2_wave` — **SCD2 dim(s) + HUB_SCD2 + fact(s)** in one topological wave.
# MAGIC
# MAGIC Register **every provider** in **`15_providers_json`**:
# MAGIC - Leaf dims/SCD2s → `"topo_level": 0`
# MAGIC - Hubs → `"archetype": "HUB_SCD2"`, `"topo_level": 1` (or 2 if hub references hub)
# MAGIC
# MAGIC **`28_consumer_overrides_json`** — event dates for Path B on **hub FK roles** and facts.
# MAGIC
# MAGIC **Linear alternative:** **`track_hub_scd2`**
# MAGIC
# MAGIC **Runbook:** `RUNBOOK_STAGES.md` → **Hub SCD2 (HUB_SCD2) waves**

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

PROFILE = "hub_scd2_wave"
w = ensure_setup_widgets()
w["setup_profile"] = PROFILE
apply_profile_hidden_defaults(w, PROFILE)
ctx = Ctx(w)
run_setup_save(ctx, w)
