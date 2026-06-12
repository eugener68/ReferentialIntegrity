# Databricks notebook source
# MAGIC %md
# MAGIC # 01b — Repair queue triage (multiselect widgets)
# MAGIC
# MAGIC Run **after `01_config_discovery`**. Loads discovered consumers from
# MAGIC `config_consumers` and lets you pick **one or more** rows to queue for repair
# MAGIC using a Databricks **multiselect** widget.
# MAGIC
# MAGIC ### How to use (two-step — Databricks widget limitation)
# MAGIC
# MAGIC 1. **Run All** (or run through cell 2) — builds the consumer list + multiselect.
# MAGIC 2. In the widget panel, **uncheck** `— check consumers to queue —`, then **check**
# MAGIC    the consumer×FK roles to repair (e.g. `transaction_fact::account_key`).
# MAGIC 3. Set widget **`apply_changes`** → `true` and widget **`mark_unselected_discovered`**
# MAGIC    → `skip` if remaining `DISCOVERED` rows should become `SKIPPED`.
# MAGIC 4. **Run All again** — writes `repair_status=SELECTED` on checked rows.
# MAGIC
# MAGIC Checked rows must be re-run to persist — widgets do not trigger writes on click alone.
# MAGIC
# MAGIC Respects `consumer_filter` from `00_setup` / `package_settings`. Rows already
# MAGIC **`FIXED`** or **`EXCLUDED`** are omitted from the picker.
# MAGIC
# MAGIC > **Scale:** multiselect works well up to ~200 choices. For larger registries use SQL
# MAGIC > or `repair_selection_json` in `00_setup`.

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

w = load_package_settings(require_saved=True)
ctx = Ctx(w)

# COMMAND ----------

# MAGIC %md ## 1. Current registry (all non-excluded consumers in filter scope)

# COMMAND ----------

display(spark.sql(f"""
  SELECT consumer_table, fk_col, provider_table, repair_status, classification,
         discovered_by, excluded, selected_at, fixed_at
  FROM {ctx.cfg('config_consumers')}
  WHERE NOT excluded
  ORDER BY repair_status, provider_table, consumer_table, fk_col"""))

# COMMAND ----------

# MAGIC %md ## 2. Build multiselect picker

# COMMAND ----------

candidates = load_triage_candidates(ctx)
print(f"{len(candidates)} triage candidates (excluding FIXED / EXCLUDED)")

if not candidates:
    print("Nothing to triage — run 01_config_discovery first, or all rows are FIXED/EXCLUDED.")
else:
    if len(candidates) > 200:
        print("⚠️  More than 200 candidates — multiselect may be awkward; consider SQL / repair_selection_json.")

    choices = [consumer_choice_key(d["consumer_table"], d["fk_col"]) for d in candidates]
    defaults = [consumer_choice_key(d["consumer_table"], d["fk_col"])
                for d in candidates if d["repair_status"] == REPAIR_SELECTED]
    defaults = [d for d in defaults if d in choices]
    picker_choices = [TRIAGE_PICK_NONE] + choices
    default_str = ",".join(defaults) if defaults else TRIAGE_PICK_NONE

    # Triage-only widgets (do not call removeAll — package_settings loaded from Delta)
    triage_widgets = {"repair_pick", "mark_unselected_discovered", "apply_changes"}
    for name in list(_widget_names()):
        if name in triage_widgets:
            dbutils.widgets.remove(name)

    dbutils.widgets.multiselect(
        "repair_pick",
        default_str,
        picker_choices,
        "Consumers to queue (SELECTED)",
    )
    dbutils.widgets.dropdown(
        "mark_unselected_discovered",
        "keep",
        ["keep", "skip"],
        "Other DISCOVERED rows",
    )
    dbutils.widgets.dropdown(
        "apply_changes",
        "false",
        ["false", "true"],
        "Apply changes (set true + re-run)",
    )

    print("Picker labels (key -> readable):")
    for d in candidates:
        key = consumer_choice_key(d["consumer_table"], d["fk_col"])
        print(f"  {key}  ==  {consumer_choice_label(d)}")

# COMMAND ----------

# MAGIC %md ## 3. Apply selection (when `apply_changes=true`)

# COMMAND ----------

if not candidates:
    pass
elif dbutils.widgets.get("apply_changes").strip().lower() != "true":
    picked = [x.strip() for x in dbutils.widgets.get("repair_pick").split(",")
              if x.strip() and x.strip() != TRIAGE_PICK_NONE]
    print(f"Preview only — {len(picked)} checked. Set apply_changes=true and re-run to persist.")
else:
    picked = [x.strip() for x in dbutils.widgets.get("repair_pick").split(",")
              if x.strip() and x.strip() != TRIAGE_PICK_NONE]
    mark_skip = dbutils.widgets.get("mark_unselected_discovered").strip().lower() == "skip"
    apply_triage_selection(ctx, picked, mark_others_skipped=mark_skip)
    display(spark.sql(f"""
      SELECT consumer_table, fk_col, provider_table, repair_status, selected_at
      FROM {ctx.cfg('config_consumers')}
      WHERE repair_status IN ('SELECTED', 'SKIPPED')
      ORDER BY repair_status, consumer_table, fk_col"""))
    print("Next: 02_snapshot_diagnostic → 02b_wip_clone → 03 … (or 04 classify if rehearsing classify-only).")
