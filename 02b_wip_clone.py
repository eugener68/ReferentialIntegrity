# Databricks notebook source
# MAGIC %md
# MAGIC # 02b — WIP Shallow Clones (safe repair workspace)
# MAGIC
# MAGIC Creates **writable shallow clones** in `{wip_schema}` for every in-scope **provider**
# MAGIC and **SELECTED** consumer. All mutating steps **03–06** write to these clones when
# MAGIC `repair_target_mode=wip_clone` (default). Production `{target_schema}` stays untouched
# MAGIC until **07_promote**.
# MAGIC
# MAGIC **Prerequisites:** `00_setup` → `01` → `01b` (or `repair_selection_json`) with consumers
# MAGIC marked `SELECTED`.
# MAGIC
# MAGIC **Skip** when `repair_target_mode=in_place` (break-glass — mutates prod directly).
# MAGIC
# MAGIC Clone names: `{logical_table}__{run_id}` — unique per run, registered in
# MAGIC `{config_schema}.wip_clones`.

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

w = load_package_settings(require_saved=True)
ctx = Ctx(w)

if repair_target_mode(w) == "in_place":
    print("repair_target_mode=in_place — no WIP clones needed (03–06 mutate production).")
    dbutils.notebook.exit("skipped")

ensure_wip_schema(ctx)
providers, consumers, scope = collect_wip_clone_scope(ctx)
prov_idx = providers_by_name(ctx)
assert scope, "Nothing to clone — enable providers and SELECT at least one consumer."

logical_names = [t for _, t in scope]
print(f"WIP clone run_id = {RUN_ID}")
print(f"Tables ({len(scope)}): {', '.join(logical_names)}")
print(f"Target: `{w['wip_schema']}`  |  Source: `{w['target_schema']}` (prod, read-only here)")

# COMMAND ----------

# MAGIC %md ## 1. Supersede prior ACTIVE clones for same logical tables

# COMMAND ----------

supersede_active_clones(ctx, logical_names)

# COMMAND ----------

# MAGIC %md ## 2. Shallow clone each table

# COMMAND ----------

clone_rows = []
for table_kind, logical_table in scope:
    wip_fqn, wip_name = create_wip_shallow_clone(
        ctx, RUN_ID, logical_table, table_kind, prov_idx)
    clone_rows.append((logical_table, wip_name, wip_fqn))

# COMMAND ----------

# MAGIC %md ## 3. Registry summary

# COMMAND ----------

display(spark.sql(f"""
  SELECT run_id, logical_table, table_kind, wip_table_name, wip_fqn,
         clone_status, row_key_cols, cloned_at
  FROM {ctx.cfg('wip_clones')}
  WHERE run_id = '{RUN_ID}'
  ORDER BY table_kind, logical_table"""))

print(f"""
02b complete. WIP run_id = {RUN_ID}

Save this run_id in widget wip_run_id if you re-run 03–06 later without re-cloning
(empty wip_run_id uses the latest ACTIVE clone set).

Next (wip_clone mode):
  03_provider_hash_keymap → 04 classify → 04 populate → 05_validate → 06_sweep → 05 (wip)
  → 07_promote → 05 with validation_target=prod
""")
