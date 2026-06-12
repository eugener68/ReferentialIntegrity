# Hash-Spine RI Repair — Parametrized Notebook Package

Implements `hash_spine_repair_plan.md` for all four archetypes — **SCD1 dim, SCD2 dim,
fact, hub SCD2** — driven entirely by two Delta config tables. One codebase, no
per-table notebooks.

**Operator guide:** **[RUNBOOK.md](RUNBOOK.md)** — **section 1 = widget `01_*`, section 28 =
widget `28_*`**. Open side by side with `00_setup` and fill top to bottom.

## Assumptions baked in

- Table and column names are **mirrored** between source (Lakehouse Federation foreign
  catalog) and target (Unity Catalog). Surrogate key names identical on both sides.
- Natural keys and all run parameters are set in **one widget panel** (`00_setup`) and
  saved to `ri_repair.package_settings` — no uncomment/edit in downstream notebooks.
- Consumers are **auto-discovered**: any table in the target schema carrying a column
  named exactly like a provider's `sk_col` (e.g. `sk_SCD2_provider`) is registered as a
  consumer of that provider. Near-miss column names (role-played/renamed FKs) land in
  `ri_repair.discovery_edge_cases` for manual triage via `manual_consumers_json` /
  `exclude_consumers_json` in `00_setup`.
- SQL Server is read-only throughout. Hashes exist only in the target estate.

## Notebooks & run order

| # | Notebook | Mutates silver? | Does |
|---|---|---|---|
| — | `_common` | — | **Library only — do not run.** Loaded via `%run` from every notebook below. |
| 00 | `00_setup` | no | **Single widget panel** — catalogs, pipeline toggles, JSON config; saves to `package_settings` |
| 01 | `01_config_discovery` | no | Schemas, config/result tables, applies JSON config, SK-name consumer discovery, edge-case scan |
| 01b | `01b_repair_triage` | no | **Multiselect widget** — pick discovered consumer×FK rows → `SELECTED` / `SKIPPED` |
| 02 | `02_snapshot_diagnostic` | no (staging only) | Legacy snapshots with in-flight `nk_hash`, collision gates, §2.2 version diagnostic → Path A/B per SCD2/hub |
| 03 | `03_provider_hash_keymap` | **yes** | `nk_hash`/`ver_hash` on providers, uniqueness + window-overlap gates, key-map builds + audit |
| 04 | `04_consumer_hash` | **yes** (populate mode) | `mode=classify`: C1 evidence per consumer×role (read-only). Human attests via `classifications_json`. `mode=populate`: per-role hash columns via key-map (C2a) or current dim (C2b). SKs untouched |
| 05 | `05_validate` | no | Full battery: coverage, member RI, version RI (A/B), measure reconciliation, hub row consistency. **Raises on any FAIL — hard gate** |
| 06 | `06_sweep` | **yes** | Guarded hash-join MERGE per role, hubs-before-facts, orphan handling, C5 post-checks. Refuses to run unless latest 05 run is green |

Run **00_setup** → **01_config_discovery** → **01b_repair_triage** → 02 → 03 → 04(classify) → *classifications* → 04(populate) → 05 → 06 → re-run 05.

## Repair queue (`config_consumers.repair_status`)

Discovery **registers** every matching consumer; it does **not** repair them. Status is
stored on `ri_repair.config_consumers` (one row per consumer table × FK column × provider):

| Status | Meaning |
|---|---|
| `DISCOVERED` | Auto-registered (default when `repair_mode=opt_in`) |
| `SELECTED` | Queued for this repair wave — **only these run in 04–06** |
| `SKIPPED` | Reviewed, intentionally not repaired |
| `VERIFIED` | 05 validation green for this role |
| `FIXED` | 06 sweep + post-check green |
| `EXCLUDED` | `excluded=true` (false positive) |
| `NOT_APPLICABLE` | Optional manual marker — use `RELOADED` + 05 for hash-only completion |

**Default `repair_mode=opt_in`:** new rows start as `DISCOVERED`; populate / validate /
sweep run only on `SELECTED` and `VERIFIED`. Set `repair_mode=opt_out` for legacy
“everything discovered except SKIPPED/FIXED” behaviour.

### Choosing what to fix

1. **`01b_repair_triage`** — multiselect widget checklist (best for ≤~200 rows):
   run once to build the list, check rows, set `apply_changes=true`, re-run.
2. **`repair_selection_json`** in `00_setup` (batch update via re-run setup + 01):
   ```json
   [{"consumer_table":"Fact_consumer","fk_col":"sk_SCD2_provider","repair_status":"SELECTED"}]
   ```
3. **SQL** on `config_consumers` (SQL editor or a `%sql` cell in any notebook):
   ```sql
   UPDATE catalog.ri_repair.config_consumers
   SET repair_status = 'SELECTED', selected_at = current_timestamp()
   WHERE consumer_table = '...' AND fk_col = '...';
   ```
4. **`consumer_filter`** — still limits which **table names** run in 04–06 this wave.

Triage view after 01:
```sql
SELECT consumer_table, fk_col, provider_table, repair_status, classification, excluded
FROM catalog.ri_repair.config_consumers
ORDER BY repair_status, provider_table, consumer_table;
```

### Triage in Databricks (this repo)

All triage is **in-workspace** — there is no separate UI app for this package.

| Method | When to use |
|---|---|
| **`01b_repair_triage`** | Default for most cases — multiselect checklist (≤~200 rows) |
| **`repair_selection_json`** | Batch updates from `00_setup` without using the picker |
| **SQL `UPDATE`** | Large registries or scripted waves |

**`01b_repair_triage`** uses native **`multiselect`** widgets: check one or more
consumer×FK roles, set `apply_changes=true`, re-run to persist `SELECTED`.

Widget limitations:

- **Two-run workflow** — selections apply on re-run, not on click alone.
- **~200 choices** — multiselect gets awkward beyond that; use SQL or
  `repair_selection_json` instead.

## Configuration (`00_setup` widgets)

All parameters live in **`00_setup`**. Edit widgets, run the notebook to save, then run
the pipeline notebooks (they load from `ri_repair.package_settings` automatically).

### Environment & scope

| Widget | Default | Meaning |
|---|---|---|
| `target_catalog` / `target_schema` | `target_catalog` / `silver` | repair target; set `target_schema=rehearsal_silver` for rehearsal (§9) |
| `source_catalog` / `source_schema` | `legacy_src` / `dbo` | foreign catalog (read-only) |
| `config_schema` / `staging_schema` / `keymap_schema` | `ri_repair` / `staging` / `keymap` | repair artifacts |
| `provider_filter` / `consumer_filter` | `*` | comma-separated scoping (e.g. pilot: `SCD2_provider` + `Fact_consumer`) |
| `dry_run` | `false` | print mutating SQL instead of executing (reads still run) |

### Pipeline toggles (same widget panel)

| Widget | Default | Used by |
|---|---|---|
| `refresh_snapshots`, `auto_set_path`, `path_a_threshold` | `false`, `true`, `0.99` | 02 |
| `recompute_hashes`, `build_keymaps` | `false`, `true` | 03 |
| `mode`, `suggest_threshold` | `classify`, `0.95` | 04 |
| `repair_mode` | `opt_in` | `opt_in` = only `SELECTED`/`VERIFIED` repaired; `opt_out` = legacy |
| `measure_tolerance` | `0.01` | 05 |
| `require_validation`, `orphan_sk`, `apply_orphan_sk` | `true`, `-1`, `false` | 06 |

### JSON config widgets

| Widget | Purpose |
|---|---|
| `providers_json` | Natural keys per provider dim/hub (**required**) |
| `manual_consumers_json` | Role-played / renamed FK columns |
| `exclude_consumers_json` | False-positive auto-discovered consumers |
| `consumer_overrides_json` | `event_date_col`, `measure_cols` on discovered facts |
| `repair_selection_json` | Set `SELECTED` / `SKIPPED` on discovered consumer×FK rows |
| `classifications_json` | `LEGACY_KEYED` / `RELOADED` attestation (after 04 classify) |

### Placeholder naming (examples only)

Examples use **role-based placeholders** — replace every name with your real silver
table and column names (mirrored from legacy SQL Server).

| Placeholder | Archetype | What it stands for |
|---|---|---|
| `Dim_provider` | SCD1 | SCD1 dimension (key provider) |
| `SCD2_provider` | SCD2 | SCD2 table containing key(s) other SCD2/Hub tables reference |
| `HubSCD2_provider` | HUB_SCD2 | Provides its own SK and consumes dim FKs |
| `Fact_consumer` | — | Fact referencing a dim or hub |
| `SCD2_consumer` | — | Table referencing a provider (e.g. hub → dim) |
| `HubSCD2_consumer` | — | Fact referencing a hub |

### Example `providers_json` (one line in the widget)

```json
[{"provider_table":"Dim_provider","archetype":"SCD1","sk_col":"sk_Dim_provider","nk_cols":["nkCol1"],"topo_level":0},{"provider_table":"SCD2_provider","archetype":"SCD2","sk_col":"sk_SCD2_provider","nk_cols":["nkCol1"],"effective_start_col":"effectiveStartDate","effective_end_col":"effectiveEndDate","topo_level":0},{"provider_table":"HubSCD2_provider","archetype":"HUB_SCD2","sk_col":"sk_HubSCD2_provider","nk_cols":["nkCol1","nkCol2"],"effective_start_col":"effectiveStartDate","effective_end_col":"effectiveEndDate","topo_level":1}]
```

Optional provider fields: `nk_type_overrides`, `use_status_tiebreaker`, `version_match_path`, `enabled`, `notes`.

Discovery registers every table whose columns include the provider's `sk_col` — e.g.
`Fact_consumer.sk_SCD2_provider`, `SCD2_consumer.sk_SCD2_provider` (hub → dim), and FK
columns inside `HubSCD2_provider`. Hubs are both provider and consumer: swept before
downstream facts.

After discovery, fill in `consumer_overrides_json` where needed:
`event_date_col` (required for Path-B providers; hubs default to their own
`effective_start_col`), `measure_cols` (optional, enables measure reconciliation).

## Edge cases

- **Role-played / renamed FKs** (e.g. `sk_SCD2_provider_roleB`): surfaced by
  the stem-scan into `discovery_edge_cases`; add real ones in `manual_consumers_json`.
  Each FK role gets its own hash columns (`<fk_col>_nk_hash`, `<fk_col>_ver_hash`).
- **recordStatus**: never used for current/expired matching; only a categorical
  deleted/active tiebreaker, and only if `use_status_tiebreaker=true` *and* the
  `(nk, startDate)` uniqueness gate demanded it.
- **MIXED-provenance consumers**: 04/06 refuse them. Establish per-row provenance first.

## Rehearsal & production (plan §8–§9)

00–02, 04-classify and 05 are read-only on silver — safe on live prod.
For the mutating half: shallow-clone in-scope tables into `rehearsal_silver`, set
`target_schema=rehearsal_silver` in `00_setup`, rerun 03→06 (don't VACUUM sources while
clones exist), record timings, get sign-off. Production run = same runbook inside a
frozen-pipeline window, key-maps rebuilt once inside the freeze, gold rebuilt parallel-and-swap.

## Result/audit tables (in `ri_repair`)

`package_settings`, `config_providers`, `config_consumers`, `discovery_edge_cases`, `version_diagnostics`,
`gate_results`, `keymap_audit`, `classification_evidence`, `validation_results`,
`sweep_results` — every run appends with a `run_id`, so the whole repair is replayable
evidence for sign-off.
