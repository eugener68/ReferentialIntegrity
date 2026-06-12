# Databricks notebook source
# MAGIC %md
# MAGIC # 07 — Promote WIP → Production
# MAGIC
# MAGIC After **05 green on WIP** and **06 sweep on WIP**, cut repaired data back to production
# MAGIC `{target_schema}`. Mode is controlled by widget **`promote_mode`**:
# MAGIC
# MAGIC | Mode | Behaviour | Best when |
# MAGIC |------|-----------|-----------|
# MAGIC | **`merge_columns`** (default) | `MERGE` prod ← wip on **row key**; update FK + hash columns only | Prod kept loading; surgical fix |
# MAGIC | **`swap_table`** | Rename prod → backup; shallow-clone wip → prod logical name | Freeze window; full table replace |
# MAGIC | **`repoint_view`** | `CREATE OR REPLACE VIEW {prefix}{table}` → wip clone | BI reads through views; instant rollback |
# MAGIC
# MAGIC **`wip_row_keys_json`** — required for **`merge_columns`** on **consumers** (providers
# MAGIC default to their `sk_col`). Example:
# MAGIC ```json
# MAGIC [{"table":"transaction_fact","row_key_cols":["transaction_id"]}]
# MAGIC ```
# MAGIC
# MAGIC ⚠️ Rows loaded into **prod after the clone was taken** are not in wip — merge leaves
# MAGIC them unchanged; re-clone + re-run or freeze before cutover.

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

w = load_package_settings(require_saved=True)
ctx = Ctx(w)
mode = (w.get("promote_mode") or "merge_columns").lower()
assert mode in ("merge_columns", "swap_table", "repoint_view"), mode

if repair_target_mode(w) == "in_place":
    print("repair_target_mode=in_place — production was already mutated in 03–06; promote skipped.")
    dbutils.notebook.exit("skipped")

run_id, clones = load_active_wip_clones(ctx)
providers = providers_by_name(ctx)
consumers = load_consumers(ctx, repair_phase="repair")
view_prefix = w.get("promote_view_prefix") or "v_"
errors = []

print(f"Promote run_id={run_id}  mode={mode}  tables={len(clones)}")

# COMMAND ----------

# MAGIC %md ## 0. Pre-check — prod rows not in wip (loads since clone)

# COMMAND ----------

for cl in clones:
    lt = cl["logical_table"]
    row_keys = cl["row_key_cols"]
    if not row_keys:
        if mode == "merge_columns":
            print(f"  skip drift check {lt}: no row_key_cols (not needed for {mode})")
        continue
    prod_fqn, wip_fqn = ctx.prod(lt), cl["wip_fqn"]
    join = merge_join_predicate(row_keys)
    drift = scalar(ctx.query(f"""
      SELECT count(*) FROM {prod_fqn} t
      LEFT ANTI JOIN {wip_fqn} s ON {join}"""))
    if drift:
        print(f"  ⚠️  {lt}: {drift} prod row(s) not in wip clone (loaded after clone?)")
    else:
        print(f"  OK  {lt}: all prod rows match wip keys (no drift)")

# COMMAND ----------

# MAGIC %md ## 1. Promote each clone

# COMMAND ----------

for cl in clones:
    lt = cl["logical_table"]
    wip_fqn = cl["wip_fqn"]
    prod_fqn = ctx.prod(lt)
    kind = cl["table_kind"]
    print(f"\n--- {lt} ({kind}) ---")

    if mode == "merge_columns":
        row_keys = cl["row_key_cols"]
        if not row_keys:
            try:
                row_keys = resolve_row_key_cols(ctx, lt, providers, kind)
            except ValueError as exc:
                errors.append(str(exc))
                continue
        update_cols = promote_update_columns(ctx, lt, providers, consumers)
        prod_cols = existing_columns(ctx, prod_fqn)
        wip_cols = existing_columns(ctx, wip_fqn)
        cols = [c for c in update_cols if c.lower() in prod_cols and c.lower() in wip_cols]
        if not cols:
            errors.append(f"{lt}: no promotable columns found")
            continue
        sets = ", ".join(f"t.`{c}` = s.`{c}`" for c in cols)
        join = merge_join_predicate(row_keys)
        ctx.exec_mut(f"""
MERGE INTO {prod_fqn} t
USING {wip_fqn} s
  ON {join}
WHEN MATCHED THEN UPDATE SET {sets}""", f"merge_columns promote {lt}")
        if not ctx.dry:
            m = last_merge_metrics(prod_fqn)
            print(f"  merged columns {cols}; rows updated = {m.get('numTargetRowsUpdated', '?')}")

    elif mode == "swap_table":
        backup = f"{lt}__prod_backup_{run_id.replace('-', '_')}"
        backup_fqn = ctx.prod(backup)
        ctx.exec_mut(f"ALTER TABLE {prod_fqn} RENAME TO `{backup}`",
                     f"backup prod {lt} -> {backup}")
        ctx.exec_mut(f"CREATE TABLE {prod_fqn} SHALLOW CLONE {wip_fqn}",
                     f"promote {lt}: wip -> prod")
        print(f"  prod backed up as {backup_fqn}")

    elif mode == "repoint_view":
        view_name = f"{view_prefix}{lt}"
        view_fqn = fq(w["target_catalog"], w["target_schema"], view_name)
        ctx.exec_mut(f"""
CREATE OR REPLACE VIEW {view_fqn} AS SELECT * FROM {wip_fqn}""",
                     f"repoint view {view_name} -> wip")
        print(f"  view {view_fqn} -> {wip_fqn}")

    if not ctx.dry:
        esc = run_id.replace("'", "''")
        ctx.exec_infra(f"""
          UPDATE {ctx.cfg('wip_clones')}
          SET clone_status = '{WIP_PROMOTED}', promoted_at = current_timestamp(),
              promote_mode = '{mode}', notes = concat_ws(' | ', notes, 'promoted by {RUN_ID}')
          WHERE run_id = '{esc}' AND logical_table = '{lt.replace("'", "''")}'
            AND clone_status = '{WIP_ACTIVE}'""", f"mark {lt} PROMOTED")

# COMMAND ----------

# MAGIC %md ## 2. Outcome

# COMMAND ----------

if errors:
    raise Exception("07 promote finished with issues:\n" + "\n".join(errors))

print(f"""
07 promote complete (mode={mode}, run_id={run_id}).

Post-cutover validation:
  1. In 00_setup set validation_target=prod (or leave auto with repair_target_mode=in_place
     only if validating prod in place — prefer validation_target=prod).
  2. Re-run 05_validate on production tables.

Optional: DROP old wip clones after sign-off:
  -- DROP TABLE {clones[0]['wip_fqn']} ... (see wip_clones registry)
""")
