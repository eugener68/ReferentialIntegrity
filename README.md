# Hash-Spine RI Repair — Parametrized Notebook Package

Implements `hash_spine_repair_plan.md` for all four archetypes — **SCD1 dim, SCD2 dim,
fact, hub SCD2** — driven entirely by two Delta config tables. One codebase, no
per-table notebooks.

## Assumptions baked in

- Table and column names are **mirrored** between source (Lakehouse Federation foreign
  catalog) and target (Unity Catalog). Surrogate key names identical on both sides.
- Natural keys are **user-fed per provider** (`upsert_provider()` in notebook 00) — the
  one input only you can supply.
- Consumers are **auto-discovered**: any table in the target schema carrying a column
  named exactly like a provider's SK (e.g. `keyAccount`) is registered as a consumer of
  that provider. Near-miss column names (role-played/renamed FKs) land in
  `ri_repair.discovery_edge_cases` for manual triage via `add_consumer()` /
  `exclude_consumer()`.
- SQL Server is read-only throughout. Hashes exist only in the target estate.

## Notebooks & run order

| # | Notebook | Mutates silver? | Does |
|---|---|---|---|
| 00 | `00_config_discovery` | no | Schemas, config/result tables, provider registration (NKs), SK-name consumer discovery, edge-case scan |
| 01 | `01_snapshot_diagnostic` | no (staging only) | Legacy snapshots with in-flight `nk_hash`, collision gates, §2.2 version diagnostic → Path A/B per SCD2/hub |
| 02 | `02_provider_hash_keymap` | **yes** | `nk_hash`/`ver_hash` on providers, uniqueness + window-overlap gates, key-map builds + audit |
| 03 | `03_consumer_hash` | **yes** (populate mode) | `mode=classify`: C1 evidence per consumer×role (read-only). Human attests via `set_classification()`. `mode=populate`: per-role hash columns via key-map (C2a) or current dim (C2b). SKs untouched |
| 04 | `04_validate` | no | Full battery: coverage, member RI, version RI (A/B), measure reconciliation, hub row consistency. **Raises on any FAIL — hard gate** |
| 05 | `05_sweep` | **yes** | Guarded hash-join MERGE per role, hubs-before-facts, orphan handling, C5 post-checks. Refuses to run unless latest 04 run is green |

Run 00 → 01 → 02 → 03(classify) → *attest* → 03(populate) → 04 → *sign-off* → 05 → re-run 04.

## Key widgets (all notebooks)

| Widget | Default | Meaning |
|---|---|---|
| `target_catalog` / `target_schema` | `target_catalog` / `silver` | repair target; set `target_schema=rehearsal_silver` to run the identical code against shallow clones (§9) |
| `source_catalog` / `source_schema` | `legacy_src` / `dbo` | foreign catalog (read-only) |
| `config_schema` / `staging_schema` / `keymap_schema` | `ri_repair` / `staging` / `keymap` | repair artifacts |
| `provider_filter` / `consumer_filter` | `*` | comma-separated scoping (e.g. pilot: one dim + one fact) |
| `dry_run` | `false` | print mutating SQL instead of executing (reads still run) |

Notebook-specific: `refresh_snapshots`, `auto_set_path`, `path_a_threshold` (01);
`recompute_hashes`, `build_keymaps` (02); `mode`, `suggest_threshold` (03);
`measure_tolerance` (04); `require_validation`, `orphan_sk`, `apply_orphan_sk` (05).

## Config seeding example (notebook 00)

```python
upsert_provider("dimAccount", "SCD2", "keyAccount",
                nk_cols=["accountNumber"],
                effective_start_col="effectiveStartDate",
                effective_end_col="effectiveEndDate",
                record_status_col="recordStatus",
                topo_level=0)

upsert_provider("hubCustomerAccount", "HUB_SCD2", "keyCustomerAccount",
                nk_cols=["accountNumber", "customerCode"],
                effective_start_col="effectiveStartDate",
                effective_end_col="effectiveEndDate",
                topo_level=1)        # hubs above the dims they reference
```

Discovery then finds every table containing `keyAccount` (accountDetails,
accountActivities, AutoPay, the hub itself, …) and registers each as a consumer
row. Hubs are handled automatically on both sides: their own SK makes them a
provider (level 1+); the dim FKs inside them make them consumers (swept before facts).

After discovery, fill in per consumer row where needed:
`event_date_col` (required for Path-B providers; hubs default to their own
`effective_start_col`), `measure_cols` (optional, enables measure reconciliation),
`excluded` + reason for false positives.

## Edge cases

- **Role-played / renamed FKs** (`keyAccountShipTo`, `parentAccountKey`): surfaced by
  the stem-scan into `discovery_edge_cases`; add real ones with
  `add_consumer(table, fk_col, provider, event_date_col=...)`. Each FK role gets its own
  hash columns (`<fk_col>_nk_hash`, `<fk_col>_ver_hash`), so role-playing just works.
- **recordStatus**: never used for current/expired matching; only a categorical
  deleted/active tiebreaker, and only if `use_status_tiebreaker=true` *and* the
  `(nk, startDate)` uniqueness gate demanded it.
- **MIXED-provenance consumers**: 03/05 refuse them. Establish per-row provenance first.

## Rehearsal & production (plan §8–§9)

00–01, 02's key-maps, 03-classify and 04 are read-only on silver — safe on live prod.
For the mutating half: shallow-clone in-scope tables into `rehearsal_silver`, rerun
02→05 with `target_schema=rehearsal_silver` (don't VACUUM sources while clones exist),
record timings, get sign-off. Production run = same runbook inside a frozen-pipeline
window, key-maps rebuilt once inside the freeze, gold rebuilt parallel-and-swap.

## Result/audit tables (in `ri_repair`)

`config_providers`, `config_consumers`, `discovery_edge_cases`, `version_diagnostics`,
`gate_results`, `keymap_audit`, `classification_evidence`, `validation_results`,
`sweep_results` — every run appends with a `run_id`, so the whole repair is replayable
evidence for sign-off.
