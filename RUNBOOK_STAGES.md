# Hash-Spine RI Repair — Stage-by-Stage Operator Guide

Companion to **`RUNBOOK.md`** (widget reference). Use this when running the pipeline without
an assistant: each section answers *what*, *prerequisites*, *expected output*, *sanity SQL*,
and *what to do if it fails*.

---

## Choose your path first

| If you are repairing… | Setup notebook | Track notebook (linear) | Profile |
|----------------------|----------------|-------------------------|---------|
| **SCD1 dim + fact(s)** | `00_setup_scd1` | **`track_scd1_dim_fact`** | `scd1_dim_fact` |
| **SCD2 dim + fact(s)** | `00_setup_scd2` | **`track_scd2_dim_fact`** | `scd2_dim_fact` |
| **SCD2 dim + hub + fact(s)** | `00_setup_hub` | **`track_hub_scd2`** | `hub_scd2_wave` |
| **Fact only** (dim already done) | `00_setup_fact` | **`track_fact_consumer`** | `fact_consumer` |
| **Everything / custom** | `00_setup` | Run numbered notebooks manually | `full` |

**Recommended for new operators:** pick one **track notebook** and run top-to-bottom.
Fill widgets in the matching **setup** notebook at each **STOP** cell.

**Default safety:** `repair_target_mode=wip_clone` — production unchanged until **07_promote**.

---

## Hub SCD2 (`HUB_SCD2`) waves

Use when a **hub/link table** has its **own surrogate key** (consumed by facts) **and**
**FK columns** pointing at dims or other SCD2s. Hubs are **both provider and consumer**.

**Setup:** `00_setup_hub` · **Track:** **`track_hub_scd2`** · **Profile:** `hub_scd2_wave`

> **Not the same as `scd2_dim_fact`:** that profile is dim → fact only. If you have a hub
> in the graph, use **`hub_scd2_wave`** (or `full`).

### Topological order

Process providers/consumers by dependency depth — encoded as **`topo_level`** in
`providers_json`:

| Level | Typical tables | Role in repair |
|-------|----------------|----------------|
| **0** | SCD1/SCD2 leaf dims | Base keys — repair hub FK roles to these first |
| **1** | `HUB_SCD2` referencing level-0 only | Provider for facts; consumer of dim FKs |
| **2+** | Hub referencing another hub | After lower hubs' FK roles are green |
| *(facts)* | Not in `providers_json` | Auto-discovered; swept **last** |

**06_sweep** uses `topo_level`: hub FK columns sweep before facts. The hub's **own SK**
(`sk_col` on the hub table) is **never swept** — only FK columns *inside* the hub, then
facts that reference the hub SK.

### Register providers (`15_providers_json`)

```json
[
  {"provider_table":"dimAccount","archetype":"SCD2","sk_col":"keyAccount","nk_cols":["accountNumber"],"effective_start_col":"effectiveStartDate","effective_end_col":"effectiveEndDate","topo_level":0},
  {"provider_table":"hubAccountContact","archetype":"HUB_SCD2","sk_col":"keyAccountContact","nk_cols":["accountNumber","contactId"],"effective_start_col":"effectiveStartDate","effective_end_col":"effectiveEndDate","topo_level":1}
]
```

| Field | Hub rule |
|-------|----------|
| `archetype` | Must be **`HUB_SCD2`** — not `SCD2` (wrong validation/sweep rules) |
| `topo_level` | **≥ 1**; higher than every dim/SCD2 the hub references |
| `nk_cols` | Hub's **own** natural key (not the dim's NK) |
| `effective_*` | Required — hub gets Path A/B diagnostic in **02** like SCD2 |

### Discovery & triage (**01** / **01b**)

**01** registers multiple consumer rows per hub — **one row per FK column**:

| `consumer_table` | `fk_col` | `provider_table` | Meaning |
|------------------|----------|------------------|---------|
| `hubAccountContact` | `keyAccount` | `dimAccount` | Hub → dim FK role |
| `factSales` | `keyAccountContact` | `hubAccountContact` | Fact → hub FK role |

In **01b**, SELECT **every role** you will repair (hub internal FKs **and** fact→hub roles).

Renamed/role-played hub FKs → **`16_manual_consumers_json`**.

### Path A/B (**02**)

Version diagnostic runs for **each** SCD2/HUB provider separately. The hub may need a
different path than the dim it references — review both before **03**.

### Consumer overrides (**28_consumer_overrides_json`)

For **Path B**, each consumer×FK role needs an **`event_date_col`**.

| Role | Typical `event_date_col` |
|------|-------------------------|
| FK **inside hub** → dim | Hub's **`effective_start_col`** (version valid when hub version started) |
| **Fact** → hub | Fact's business event date (e.g. `transactionDate`) |
| **Fact** → dim (direct) | Fact's event date |

Example:

```json
[
  {"consumer_table":"hubAccountContact","fk_col":"keyAccount","event_date_col":"effectiveStartDate"},
  {"consumer_table":"factSales","fk_col":"keyAccountContact","event_date_col":"saleDate","measure_cols":["amount"]}
]
```

### Classify & populate (**04**)

Run **classify** then **populate** **per role** — same as facts. Attest
**`27_classifications_json`** with one entry per `consumer_table` + `fk_col` (hub roles
and fact roles separately).

### Validation (**05**) — hub-specific

| Check | Verdict | Meaning |
|-------|---------|---------|
| `member_ri`, `version_ri` | PASS/FAIL | Per FK role (same as facts) |
| `hub_row_consistency` | INFO | Hub row resolves some FK roles but orphans others — half-repaired logical records |

If `hub_row_consistency` > 0, fix the failing role before **06** (don't sign off).

### Sweep (**06**)

Expected order in **06** output:

```
Sweep order:
  hubAccountContact.keyAccount -> dimAccount      ← lower topo_level first
  factSales.keyAccountContact -> hubAccountContact
```

Post-sweep anti-join must be **0** per role (or orphans explicitly accepted via
**`32_apply_orphan_sk`**).

### WIP clone (**02b**)

Clones **all** in-scope providers (dim + hub) **and** SELECTED consumer **tables**
(hub table + facts). Hub must be cloned because **03** adds `nk_hash` on the hub as a
provider and **04/06** mutate hub FK columns.

### Common hub mistakes

| Mistake | Symptom |
|---------|---------|
| Hub registered as `SCD2` | Missing `hub_row_consistency`; wrong sweep behavior |
| `topo_level: 0` on hub | Facts swept before hub FK roles fixed |
| Only fact selected in **01b** | Hub FK roles skipped — facts still broken |
| One `classifications_json` entry for whole hub | **04** skips roles — need one entry per `fk_col` |
| Same Path A/B assumed for dim and hub | Version RI fails on hub role — re-check **02** |

### Hub wave checklist

- [ ] Dim + hub in **`15_providers_json`** with correct `topo_level` and `HUB_SCD2`
- [ ] **01b**: hub FK roles + fact roles all `SELECTED`
- [ ] **02**: Path A/B reviewed for **each** provider table
- [ ] **`28_consumer_overrides_json`**: event dates for Path B hub roles + facts
- [ ] **04**: classify + populate for **every** role
- [ ] **05**: no FAIL; `hub_row_consistency` understood if INFO > 0
- [ ] **06**: sweep order dim-roles-on-hub → facts; anti-join 0
- [ ] **`34_wip_row_keys_json`**: row keys for hub (if promoted) and each fact

---

## Profile widget cheat sheet

### `scd1_dim_fact` (27 focus widgets)

Fill: catalog/schema, `providers_json`, filters, WIP settings, repair queue JSON,
`classifications_json`, orphan/promote settings, `wip_row_keys_json`.

Ignore unless debugging: `auto_set_path`, `path_a_threshold`, `mode` (track sets via
`patch_package_settings`), `consumer_overrides_json`, `promote_view_prefix`.

### `scd2_dim_fact` (+4 widgets)

Also fill: `auto_set_path`, `path_a_threshold`, `consumer_overrides_json` (event dates for Path B).

### `hub_scd2_wave` (same widgets as SCD2 + multi-provider JSON)

Same visible widgets as **`scd2_dim_fact`**. Additionally:

- **`15_providers_json`**: **dim(s) + hub(s)** with `HUB_SCD2` and `topo_level` 0 / 1 / 2
- **`28_consumer_overrides_json`**: event dates for **hub FK roles** (often hub `effective_start_col`) and facts
- **01b**: select hub internal FK roles **and** fact→hub roles

See **Hub SCD2 (HUB_SCD2) waves** above.

### `fact_consumer` (~22 widgets)

Skip `providers_json` if provider already registered. Skip **02** and **03** notebooks.
Still run **02b** (clone includes provider for sweep joins).

---

## Stage 00 — Setup

**Notebooks:** `00_setup_scd1` | `00_setup_scd2` | `00_setup_hub` | `00_setup_fact` | `00_setup`

### Purpose

Save all configuration to `{catalog}.ri_repair.package_settings`. Create config + WIP schemas.

### Prerequisites

- Unity Catalog catalog name known (not placeholder `target_catalog`)
- Legacy federation catalog configured (for **02**, not needed for **00** itself)

### What to enter (minimum)

| Widget | Example |
|--------|---------|
| `01_target_catalog` | `recon_tgt` |
| `02_target_schema` | `gold` (if SKs generated in gold) |
| `03_source_catalog` | your federation catalog |
| `15_providers_json` | one SCD1/SCD2 provider object (see RUNBOOK §15) |
| `12_repair_target_mode` | `wip_clone` |

### Expected output

```
Package settings saved.
  table: `recon_tgt`.`ri_repair`.package_settings
  profile: scd1_dim_fact
=== Setup profile: scd1_dim_fact — ...
```

### Green / red

| Green | Red |
|-------|-----|
| `Verified: ... package_settings (1 active config row)` | `target_catalog` placeholder error |
| Profile guide lists focus widgets | Empty `providers_json` when running **01** next |

### Next

**01_config_discovery** (or continue track)

---

## Stage 01 — Config & discovery

**Notebook:** `01_config_discovery`

### Purpose

Create config/audit tables; upsert providers from JSON; auto-discover consumers by matching
`fk_col` name to provider `sk_col`.

### Prerequisites

**00** saved with real catalog + non-empty `providers_json`

### Expected output

```
Discovery scan: catalog=..., target_schema=gold (N tables, M columns)
discovered K consumer x role pairs
```

Display: `config_providers`, `config_consumers` registry.

### Sanity SQL

```sql
SELECT provider_table, archetype, sk_col, nk_cols, enabled
FROM recon_tgt.ri_repair.config_providers;

SELECT consumer_table, fk_col, provider_table, repair_status
FROM recon_tgt.ri_repair.config_consumers
ORDER BY consumer_table, fk_col;
```

### Common failures

| Symptom | Fix |
|---------|-----|
| `discovered 0 pairs` | Wrong `target_schema`; or `sk_col` on provider ≠ column name on fact |
| `config_consumers` empty but discovered > 0 | Old code + `dry_run=true` — pull latest repo |
| Provider upsert fails on SCD2 | Add `effective_start_col` to `providers_json` |

### Next

**01b_repair_triage** (or track STOP 2)

---

## Stage 01b — Repair triage

**Notebook:** `01b_repair_triage`

### Purpose

Multiselect UI to mark consumer×FK rows `SELECTED` or `SKIPPED`.

### Prerequisites

**01** completed; candidates in `config_consumers`

### Steps

1. Run once — builds multiselect list
2. Check rows to repair
3. Set widget **`apply_changes=true`**
4. Re-run

### Expected output

```
triage applied: N SELECTED, M others -> SKIPPED
```

For **hub waves**, confirm SELECTED rows include **both**:
- `hubTable.dimFkCol -> dimProvider` (FK inside hub)
- `factTable.hubSkCol -> hubProvider` (fact → hub)

### Green / red

| Green | Red |
|-------|-----|
| Your fact appears with `repair_status=SELECTED` | Empty multiselect — no DISCOVERED rows |
| | `apply_changes=false` — preview only, nothing saved |

### Next

**02** (full track) or **02b** (fact_consumer track)

---

## Stage 02 — Legacy snapshot + diagnostic

**Notebook:** `02_snapshot_diagnostic`

### Purpose

- CTAS legacy tables from federation → `staging.legacy_*` with `nk_hash`
- Collision gates on legacy + **prod** target
- SCD2/HUB: version diagnostic → suggest Path A or B

### Prerequisites

Federation read access; providers enabled

### Mutates prod?

**No** — writes `staging` and `ri_repair` audit tables only. Reads **prod** for collision/diagnostic.

### Expected output

```
PASS  hash_collision  legacy_account_dim (violations=0)
PASS  hash_collision  account_dim (violations=0)
account_dim: matched=..., suggested Path B
02 complete. Review paths above, then run 02b_wip_clone ...
```

### Sanity SQL

```sql
SELECT count(*) FROM recon_tgt.staging.legacy_account_dim;
SELECT map_status, count(*) FROM recon_tgt.keymap.account_dim_keymap GROUP BY 1;  -- after 03
```

### Common failures

| Symptom | Fix |
|---------|-----|
| Snapshot fails | Federation catalog/table name mismatch |
| hash_collision > 0 | NK normalization wrong — check `nk_cols` / `nk_type_overrides` |
| Path A suggested but business says B | Set `version_match_path` manually in config |

### Next

**02b_wip_clone** (wip_clone mode)

---

## Stage 02b — WIP shallow clones

**Notebook:** `02b_wip_clone`

### Purpose

Create writable clones: `{wip_schema}.{table}__{run_id}` for each provider + SELECTED consumer.
Register in `ri_repair.wip_clones`.

### Prerequisites

**01b** with at least one `SELECTED` consumer; `repair_target_mode=wip_clone`

### Expected output

```
WIP clone run_id = 20240612T153045_a1b2c3
  clone: account_dim -> `recon_tgt`.`ri_wip`.`account_dim__20240612T153045_a1b2c3`
02b complete. WIP run_id = ...
```

**Save the run_id** to widget `14_wip_run_id` if re-running **03–06** later without re-cloning.

### Green / red

| Green | Red |
|-------|-----|
| Registry shows `clone_status=ACTIVE` for each table | `Nothing to clone` — no SELECTED consumers |
| | `in_place` mode — notebook exits (no clones) |

### Next

**03_provider_hash_keymap**

---

## Stage 03 — Provider hash + key-map

**Notebook:** `03_provider_hash_keymap`

### Purpose

1. Add/populate `nk_hash` (and `ver_hash` if Path A) on **WIP provider** tables
2. Uniqueness / window-overlap gates
3. Build `keymap.{provider}_keymap`

### Prerequisites

**02b** complete (wip_clone) or `in_place` + `accept_in_place_risk=true`

### Expected output

```
PASS  nk_hash_unique  account_dim (violations=0)
account_dim: MATCHED=..., ORPHAN_OLD=..., ORPHAN_NEW=...
03 complete. All gates green, key-maps built. Next: 04 ...
```

### Sanity SQL

```sql
SELECT map_status, count(*) FROM recon_tgt.keymap.account_dim_keymap GROUP BY 1;
SELECT count(*) FROM recon_tgt.ri_wip.account_dim__<run_id> WHERE nk_hash IS NULL;
```

### Common failures

| Symptom | Fix |
|---------|-----|
| SKIP keymap — gates failed | Fix dim data; duplicate NK windows |
| ORPHAN_OLD high | Reload dropped members — investigate dim vs legacy |
| No WIP clone error | Run **02b** first |

### Next

**04** classify (track sets `mode=classify` automatically)

---

## Stage 04 — Classify + populate

**Notebook:** `04_consumer_hash` (two passes)

### Purpose

| Pass | `mode` | Writes? |
|------|--------|---------|
| **Classify** | `classify` | Evidence only — maps fact FKs to key-map buckets |
| **Populate** | `populate` | Adds `<fk>_nk_hash` on **WIP facts** from key-map or current dim |

### Prerequisites

**03** key-maps built; consumers `SELECTED`; for populate: **`classifications_json`** attested

### Classify — expected output

```
transaction_fact.account_key -> account_dim: MATCHED=..., NOT_IN_LEGACY_DIM=..., suggestion: LEGACY_KEYED?
```

Review `classification_evidence` table.

### Human STOP — attestation

In setup notebook, fill **`27_classifications_json`**:

```json
[{"consumer_table":"transaction_fact","fk_col":"account_key","classification":"LEGACY_KEYED"}]
```

Re-run setup, then populate pass.

### Populate — expected output

```
transaction_fact.account_key: NULL account_key_nk_hash = N of M rows (orphan report)
```

Non-zero NULLs may be expected (`NOT_IN_LEGACY_DIM`, `ORPHAN_OLD`) — reconcile before **06**.

### Common failures

| Symptom | Fix |
|---------|-----|
| SKIPPED — classification not set | Fill `classifications_json` |
| SKIPPED — MIXED | Per-row provenance required — don't force |
| All NULL hashes | Wrong classification; or key-map has no MATCHED old_sk |

### Next

**05_validate**

---

## Stage 05 — Validation (hard gate)

**Notebook:** `05_validate`

### Purpose

Read-only battery: coverage, member RI, version RI, measures, hub consistency.
**Raises on any FAIL.**

### Prerequisites

**04** populate complete for in-scope roles

### Widget

| When | `validation_target` |
|------|---------------------|
| Pre-promote (WIP) | `auto` (default) |
| Post-promote (prod sign-off) | `prod` |

### Expected output

```
PASS     member_ri                transaction_fact.account_key (violations=0)
info     coverage_null_hash       transaction_fact.account_key (violations=10) ...
05 validation complete — all checks PASS (or raises)
```

INFO on `coverage_null_hash` is not a failure — reconcile orphans vs **06** `apply_orphan_sk`.

For **hubs**, also review **`hub_row_consistency`** (INFO): > 0 means some hub rows resolve
one FK role but not another — fix before sweep sign-off.

### Common failures

| Symptom | Fix |
|---------|-----|
| member_ri FAIL | Hashes point at missing dim members — back to **03/04** |
| hash_column_present FAIL | Re-run **04** populate |
| version_ri FAIL (SCD2 Path B) | Set `event_date_col` in `consumer_overrides_json` |
| hub_row_consistency INFO > 0 | Fix failing FK role on hub before **06**; check overrides for Path B |

### Next

**06_sweep** (if LEGACY_KEYED and member RI green)

---

## Stage 06 — SK sweep

**Notebook:** `06_sweep`

### Purpose

Guarded MERGE: update fact `account_key` (etc.) via hash join to **WIP provider**.
Optional: set orphan SK where hash is NULL.

### Prerequisites

Latest **05** green; `require_validation=true` (default)

### Widgets

| Widget | When |
|--------|------|
| `32_apply_orphan_sk` | `true` for known bad FKs (NULL hash → `-1`) |
| `31_orphan_sk` | Unknown member key (default `-1`) |

**Hub waves:** **06** lists sweep order — hub FK roles (by `topo_level`) before facts.
The hub's **own SK** is never updated; only FK columns inside the hub, then fact→hub roles.

### Expected output

```
transaction_fact.account_key: rows updated = 12345
transaction_fact.account_key: post-sweep SK anti-join = 0
06 complete. Next steps: ...
```

### Common failures

| Symptom | Fix |
|---------|-----|
| post-sweep anti-join > 0 | NULL hashes not swept — use `apply_orphan_sk` or fix data |
| Validation gate blocked | Re-run **05** |
| rows updated = 0 | Hashes NULL or already correct SKs |
| Fact swept before hub FK role | Fix `topo_level` on hub provider (dim=0, hub=1+) |

### Next

Re-run **05** on WIP, then **07_promote**

---

## Stage 07 — Promote WIP → prod

**Notebook:** `07_promote`

### Purpose

Cutover repaired columns/tables from WIP clones to production.

### Prerequisites

**05** + **06** green on WIP; **`wip_row_keys_json`** filled for `merge_columns`

### Modes (`33_promote_mode`)

| Mode | Use when |
|------|----------|
| `merge_columns` | Prod kept loading; update FK + hash cols only |
| `swap_table` | Freeze window; replace whole table |
| `repoint_view` | BI reads `{prefix}{table}` views |

### Expected output

```
Promote run_id=... mode=merge_columns
  merged columns ['account_key', 'account_key_nk_hash']; rows updated = ...
07 promote complete
```

### Pre-check warning

```
⚠️ transaction_fact: 500 prod row(s) not in wip clone (loaded after clone?)
```

Rows loaded after **02b** won't get repaired SKs — freeze or accept two populations.

### Next

**05** with `validation_target=prod`

---

## Post-repair sign-off checklist

- [ ] **05** on prod: zero FAIL for repaired consumer×FK roles
- [ ] `wip_clones.clone_status = PROMOTED`
- [ ] Business signed off on orphan counts (INFO in coverage check)
- [ ] Document `run_id` and promote mode in change record
- [ ] Optional: DROP old WIP tables after retention

---

## Quick troubleshooting index

| Symptom | Stage | See |
|---------|-------|-----|
| Empty consumers | 01 | Stage 01 |
| No WIP clone | 02b | Stage 02b |
| NOT_IN_LEGACY_DIM orphans | 04/06 | Stage 04 + 06 |
| POST-SWEEP anti-join > 0 | 06 | Stage 06 |
| Promote merge fails | 07 | Fill `34_wip_row_keys_json` |
| Hub / topo / row consistency | — | **Hub SCD2 (HUB_SCD2) waves** (top) |
| package_settings not found | 00 | Stage 00 |

---

## Related

- **`RUNBOOK.md`** — full widget reference (sections 00–36)
- **`README.md`** — architecture overview
- **`hash_spine_repair_plan.md`** — method theory (hub use case D5–D7)
