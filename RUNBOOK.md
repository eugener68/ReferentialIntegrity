# Hash-Spine RI Repair — Operator Runbook

Open **`00_setup`** and this document **side by side**. The widget panel is sorted
**alphabetically by Databricks**; each widget is prefixed **`01_` … `28_`** so panel
order matches this runbook.

**Section 1 below = first widget in the panel** (`01_target_catalog`).  
**Section 28 = last widget** (`28_apply_orphan_sk`).

After editing widgets: **Run `00_setup`** → settings save to
`{target_catalog}.{config_schema}.package_settings`. Other notebooks load from there.

---

## When to touch which widgets (workflow only)

| Step | Notebooks | Widget #s to change |
|------|-----------|---------------------|
| First-time setup | `00_setup` → `01` | **1–15** (environment + JSON + repair_mode) |
| Pick consumers | `01b` *(or* **15** + re-run `00`/`01`) | Usually **01b** only |
| Snapshots / key-maps | `02` → `03` | **8–9**, **16–20** if needed |
| Classify | `04` classify | **21–22** |
| Populate | `00_setup` → `04` populate | **20–21**, **23–24** |
| Validate & sweep | `05` → `06` → `05` | **25–28** |

Skip widgets whose default is fine. You do **not** fill all 28 on day one.

---

## Widget reference (panel order = sections 1–28)

### 1 — `01_target_catalog`

| | |
|---|---|
| **Default** | `target_catalog` |
| **Phase** | A — first run |
| **What** | Unity Catalog catalog containing silver tables you will repair |
| **Enter** | Your real catalog, e.g. `prod_dwh` — **not** the placeholder `target_catalog` |

**Important:** This value determines where `package_settings` is saved. Notebooks **01–06**
auto-find that table when it is the only `package_settings` in the config schema; otherwise
set widgets **01** and **05** in those notebooks to match.

---

### 2 — `02_target_schema`

| | |
|---|---|
| **Default** | `silver` |
| **Phase** | A; change for rehearsal |
| **What** | Schema within target catalog for repaired tables |
| **Enter** | `silver` (prod) or `rehearsal_silver` (shallow-clone rehearsal) |

---

### 3 — `03_source_catalog`

| | |
|---|---|
| **Default** | `legacy_src` |
| **Phase** | A |
| **What** | Lakehouse Federation catalog → legacy SQL Server (read-only) |
| **Enter** | Your federation catalog name |

---

### 4 — `04_source_schema`

| | |
|---|---|
| **Default** | `dbo` |
| **Phase** | A |
| **What** | Schema in foreign catalog; table/column names must **mirror** target |
| **Enter** | Usually `dbo` or your legacy schema |

---

### 5 — `05_config_schema`

| | |
|---|---|
| **Default** | `ri_repair` |
| **Phase** | A |
| **What** | Config tables, audit tables, `package_settings` |
| **Enter** | Leave default unless required by naming standards |

---

### 6 — `06_staging_schema`

| | |
|---|---|
| **Default** | `staging` |
| **Phase** | A |
| **What** | Legacy snapshots (`staging.legacy_<table>`) |
| **Enter** | Leave default |

---

### 7 — `07_keymap_schema`

| | |
|---|---|
| **Default** | `keymap` |
| **Phase** | A |
| **What** | Permanent key-map tables — keep after sign-off |
| **Enter** | Leave default |

---

### 8 — `08_provider_filter`

| | |
|---|---|
| **Default** | `*` |
| **Phase** | A / pilot |
| **What** | Comma-separated **provider table names** for notebooks **02–03**. `*` = all |
| **Example** | `dimAccount` for one-dim pilot |

---

### 9 — `09_consumer_filter`

| | |
|---|---|
| **Default** | `*` |
| **Phase** | A / pilot |
| **What** | Comma-separated **consumer table names** for notebooks **04–06**. `*` = all queued |
| **Example** | `factPayments,factOrders` |

---

### 10 — `10_dry_run`

| | |
|---|---|
| **Default** | `false` |
| **Phase** | Any mutating step |
| **What** | `true` = print mutating SQL, do not execute (reads still run) |
| **Use** | Rehearsal / inspect SQL on **03**, **04** populate, **06** |

---

### 11 — `11_providers_json` ⭐ required

| | |
|---|---|
| **Default** | `[]` |
| **Phase** | A — **must fill before `01`** |
| **What** | Registers every **provider** table — dims, SCD2s, and hubs that **issue surrogate keys** other tables reference |

#### What is a provider?

A **provider** is a silver table whose **surrogate key column** (`sk_col`) appears as a foreign key in other tables (facts, other dims, hubs). The repair pipeline must know:

1. **Which tables are providers** (vs pure consumers)
2. **How to identify rows** — the **natural key** columns (`nk_cols`) used to build hash fingerprints and key-maps
3. **How to handle versions** — SCD2/hub effective dates, Path A vs B (set later in **02**)

You enter this once per provider in widget **11**. On **`01_config_discovery`**, each object is upserted into `{target_catalog}.{config_schema}.config_providers`. Discovery then scans the target schema for any column whose **name matches a provider's `sk_col`** and registers those as consumers.

**If this widget is empty, `01` fails** — there is nothing to discover or repair against.

#### Choose the right `archetype`

| Archetype | Use when | Example |
|-----------|----------|---------|
| **`SCD1`** | One row per business entity; no version history | `dimCountry`, `dimStatus` |
| **`SCD2`** | Type-2 history; same NK can have many rows over time | `dimAccount`, `dimCustomer` |
| **`HUB_SCD2`** | Hub/link table with its **own** SK **and** FKs to dims; also versioned | `hubAccountContact`, bridge tables |

**Rule of thumb:** if other tables reference its SK **and** it has its own NK columns you hash on → it's a provider. Register hubs too (not just leaf dims).

#### Field reference

| Field | Required | Description |
|-------|----------|-------------|
| `provider_table` | ✅ | Silver table name (must match target schema exactly) |
| `archetype` | ✅ | `SCD1`, `SCD2`, or `HUB_SCD2` |
| `sk_col` | ✅ | Surrogate key column name. **Discovery matches consumers when `fk_col` = this name** |
| `nk_cols` | ✅ | Ordered array of natural-key columns used to hash rows. Order matters — use the same order everywhere |
| `topo_level` | ✅ | Dependency depth for **sweep order** in **06** (see below) |
| `effective_start_col` | SCD2 / HUB | Column marking version start (e.g. `effectiveStartDate`) |
| `effective_end_col` | SCD2 / HUB | Version end; NULL = open/current row |
| `record_status_col` | Optional | Status flag for tie-breaking when `(nk, start)` is not unique |
| `nk_type_overrides` | Optional | Map column → type for hashing: `"date"`, `"timestamp"`, `"bigint"`, `"decimal(p,s)"` |
| `use_status_tiebreaker` | Optional | `true` only when you need status-based disambiguation (categorical match) |
| `version_match_path` | Optional | `A` or `B` — usually leave unset; **02** diagnostic suggests this |
| `enabled` | Optional | `false` to skip this provider (default `true`) |
| `notes` | Optional | Free text for operators |

#### `topo_level` — dependency depth

Providers sit at different levels in your FK graph. **`topo_level` controls processing order** when a table is both provider and consumer (e.g. a hub):

| Level | Typical tables | Meaning |
|-------|----------------|---------|
| **0** | Leaf SCD1 / SCD2 dims | Base keys — nothing "below" them in the graph |
| **1** | Hubs referencing only level-0 dims | Process after their dim FKs are fixed |
| **2+** | Hubs referencing other hubs | Higher in the chain |

Facts don't appear here — they are auto-discovered as consumers. During **06_sweep**, tables that are also providers sweep **before** plain facts, sorted by `topo_level` ascending.

#### How `sk_col` drives discovery

Discovery assumes **mirrored column names**: if `dimAccount` has `keyAccount`, any target table with a column literally named `keyAccount` is registered as a consumer of `dimAccount`.

- Role-played or renamed FKs (e.g. `keyAccountShipTo` → same dim) **won't** auto-match → use widget **12** (`manual_consumers_json`).
- One provider can have many consumers; one consumer can reference multiple providers (one row per FK in `config_consumers`).

#### Examples

**Single SCD2 dim (most common pilot start):**

```json
[{"provider_table":"dimAccount","archetype":"SCD2","sk_col":"keyAccount","nk_cols":["accountNumber"],"effective_start_col":"effectiveStartDate","effective_end_col":"effectiveEndDate","topo_level":0}]
```

**SCD1 dim (no effective dates):**

```json
[{"provider_table":"dimCountry","archetype":"SCD1","sk_col":"keyCountry","nk_cols":["countryCode"],"topo_level":0}]
```

**Dim + hub in one wave** (hub references dim; hub gets `topo_level: 1`):

```json
[
  {"provider_table":"dimAccount","archetype":"SCD2","sk_col":"keyAccount","nk_cols":["accountNumber"],"effective_start_col":"effectiveStartDate","effective_end_col":"effectiveEndDate","topo_level":0},
  {"provider_table":"hubAccountContact","archetype":"HUB_SCD2","sk_col":"keyAccountContact","nk_cols":["accountNumber","contactId"],"effective_start_col":"effectiveStartDate","effective_end_col":"effectiveEndDate","topo_level":1}
]
```

**Composite / typed natural keys:**

```json
[{"provider_table":"dimProduct","archetype":"SCD2","sk_col":"keyProduct","nk_cols":["productCode","regionCode"],"nk_type_overrides":{"productCode":"bigint"},"effective_start_col":"validFrom","effective_end_col":"validTo","topo_level":0}]
```

#### Operator checklist

1. List every dim/SCD2/hub whose SK was reloaded or may be wrong.
2. Confirm **`sk_col`** names match what facts actually use (check one fact table in SQL).
3. Confirm **`nk_cols`** are the columns that **uniquely identify the business entity** in legacy and silver (not the SK).
4. Set **`topo_level`** — dims `0`, hubs `1+` based on what they reference.
5. Paste as **one compact JSON array** in the widget (no line breaks required).
6. Run **`00_setup`** → **`01`** → verify `config_providers` and discovered consumers.

#### Common mistakes

| Mistake | Symptom |
|---------|---------|
| Empty `[]` | `01` errors: *No enabled providers* |
| Wrong `sk_col` | Consumers not discovered; use **12** or fix name |
| Missing `effective_start_col` on SCD2 | Upsert fails at `01` |
| `nk_cols` order changed mid-run | Hashes/key-maps won't match — set **20** `recompute_hashes=true` |
| Hub registered as `SCD2` instead of `HUB_SCD2` | Wrong validation/sweep behavior for hub-specific rules |

---

### 12 — `12_manual_consumers_json`

| | |
|---|---|
| **Default** | `[]` |
| **Phase** | A — optional |
| **What** | FK roles auto-discovery misses (renamed / role-played columns) |

```json
[{"consumer_table":"factShipments","fk_col":"keyAccountShipTo","provider_table":"dimAccount","event_date_col":"shipDate"}]
```

---

### 13 — `13_exclude_consumers_json`

| | |
|---|---|
| **Default** | `[]` |
| **Phase** | A — optional |
| **What** | Permanent false positives (scratch tables, etc.) |

```json
[{"consumer_table":"tmpAccountLoad","fk_col":"keyAccount","reason":"not a real consumer"}]
```

---

### 14 — `14_repair_mode`

| | |
|---|---|
| **Default** | `opt_in` |
| **Phase** | A |
| **Values** | `opt_in` = only user-**SELECTED** consumers repaired (**prod**). `opt_out` = repair all discovered except SKIPPED |

---

### 15 — `15_repair_selection_json`

| | |
|---|---|
| **Default** | `[]` |
| **Phase** | B — or use **`01b_repair_triage`** instead |
| **What** | Batch-set `repair_status` on discovered consumer×FK rows |

```json
[{"consumer_table":"factPayments","fk_col":"keyAccount","repair_status":"SELECTED"}]
```

Valid status: `SELECTED`, `SKIPPED`, `DISCOVERED`. Then re-run **`00_setup`** + **`01`**.

---

### 16 — `16_refresh_snapshots`

| | |
|---|---|
| **Default** | `false` |
| **Phase** | C — notebook **02** |
| **What** | `true` = rebuild `staging.legacy_*` (normally once per freeze) |

---

### 17 — `17_auto_set_path`

| | |
|---|---|
| **Default** | `true` |
| **Phase** | C — notebook **02** |
| **What** | Write suggested SCD2 Path A/B into config from version diagnostic |

---

### 18 — `18_path_a_threshold`

| | |
|---|---|
| **Default** | `0.99` |
| **Phase** | C — notebook **02** |
| **What** | Min share of `VERSION_MATCHED` rows to suggest Path A (0–1) |

---

### 19 — `19_build_keymaps`

| | |
|---|---|
| **Default** | `true` |
| **Phase** | C — notebook **03** |
| **What** | `false` = skip key-map build (debug only) |

---

### 20 — `20_recompute_hashes`

| | |
|---|---|
| **Default** | `false` |
| **Phase** | C–E — notebooks **03**, **04** |
| **What** | `true` = recompute all hash rows (after NK config change) |

---

### 21 — `21_mode`

| | |
|---|---|
| **Default** | `classify` |
| **Phase** | D → **04** classify; E → **04** populate |
| **Values** | `classify` (read-only evidence) then `populate` (write hash columns) |

---

### 22 — `22_suggest_threshold`

| | |
|---|---|
| **Default** | `0.95` |
| **Phase** | D — notebook **04** classify |
| **What** | Key-map match share above which output suggests `LEGACY_KEYED?` |

---

### 23 — `23_classifications_json` ⭐ before populate

| | |
|---|---|
| **Default** | `[]` |
| **Phase** | E — after **04** classify, before **04** populate |
| **What** | Human attestation per consumer×FK |

```json
[{"consumer_table":"factPayments","fk_col":"keyAccount","classification":"LEGACY_KEYED","note":"not reloaded per migration log"}]
```

| Value | Meaning |
|-------|---------|
| `LEGACY_KEYED` | Broken SKs — populate from key-map, **sweep in 06** |
| `RELOADED` | SKs OK — populate from current dim, **no sweep** |
| `MIXED` | Blocked — resolve provenance first |

---

### 24 — `24_consumer_overrides_json`

| | |
|---|---|
| **Default** | `[]` |
| **Phase** | E — after **01**, before **04** populate |
| **What** | `event_date_col` (required Path B on facts), optional `measure_cols` for **05** |

```json
[{"consumer_table":"factPayments","fk_col":"keyAccount","event_date_col":"paymentDate","measure_cols":["amount"]}]
```

---

### 25 — `25_measure_tolerance`

| | |
|---|---|
| **Default** | `0.01` |
| **Phase** | F — notebook **05** |
| **What** | Max abs diff per member in optional measure reconciliation |

---

### 26 — `26_require_validation`

| | |
|---|---|
| **Default** | `true` |
| **Phase** | F — notebook **06** |
| **What** | `true` = **06** refuses to run unless latest **05** is green |

---

### 27 — `27_orphan_sk`

| | |
|---|---|
| **Default** | `-1` |
| **Phase** | F — notebook **06** |
| **What** | Unknown-member SK when applying orphans |

---

### 28 — `28_apply_orphan_sk`

| | |
|---|---|
| **Default** | `false` |
| **Phase** | F — notebook **06** |
| **What** | `true` = set rows with NULL hash to `orphan_sk` (last resort) |

---

## Phase B — `01b_repair_triage` (not in this widget list)

After **01**, run **`01b_repair_triage`**: multiselect checklist → `apply_changes=true` →
re-run. Alternative to widget **15**.

| Repair status | Meaning |
|---------------|---------|
| `DISCOVERED` | Registered, not queued |
| `SELECTED` | Queued for **04–06** |
| `SKIPPED` | Will not repair |
| `VERIFIED` / `FIXED` | Set automatically by **05** / **06** |

---

## End-to-end sequence (first pilot)

1. Fill **1–4**, **11** (minimum). Run **`00_setup`** (note the printed `table:` path).
2. Run **`01_config_discovery`** (auto-finds `package_settings` if unique; else set widgets **1** + **5**).
3. **`01b_repair_triage`** (or **15**) → queue consumers.
4. Run **02** → **03** (widgets **8–9**, **16–19** if needed).
5. **21**=`classify` → **`00_setup`** → **04** classify.
6. Fill **23** (and **24** if Path B). **21**=`populate` → **`00_setup`** → **04** populate.
7. **05** → sign-off → **06** → **05** again.

---

## Troubleshooting

| Symptom | Check |
|---------|--------|
| `config_consumers` empty after `01` | Check section 3 output: `discovered N pairs`. If N>0 but table empty on old code, `dry_run=true` skipped MERGE — pull latest (uses `exec_infra`). If N=0, wrong `target_schema` or `sk_col` mismatch |
| `Package settings not found` / `target_catalog`.`ri_repair` | Re-run **00_setup** with real catalog in widget **1**; pull latest code (auto-discovery). If multiple catalogs have `ri_repair.package_settings`, set widget **1** explicitly |
| Panel order wrong | Re-run **`00_setup`** after upgrade (widgets must show `01_`…`28_` prefix) |
| No consumers queued | **01b** or **15**; `repair_mode=opt_in` needs `SELECTED` |
| `providers_json` empty | Widget **11** before **01** |
| 04 skips rows | **23** not set or `MIXED` |
| 06 blocked | **05** failed or **26**=`true` |
| Changes ignored | Re-run **`00_setup`** after edits |

---

## Related docs

- **`README.md`** — architecture and notebook list  
- **`hash_spine_repair_plan.md`** — method detail  
- **`01b_repair_triage`** — consumer multiselect picker
