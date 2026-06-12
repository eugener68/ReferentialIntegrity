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
| **Enter** | Your real catalog, e.g. `prod_dwh` |

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
| **What** | JSON array: one object per key-providing dim / SCD2 / hub |

**Required fields:** `provider_table`, `archetype` (`SCD1`|`SCD2`|`HUB_SCD2`), `sk_col`,
`nk_cols` (array), `topo_level` (0=dim, 1+=hub).

**SCD2/HUB also:** `effective_start_col`, `effective_end_col`, optional `record_status_col`.

**Optional:** `nk_type_overrides`, `use_status_tiebreaker`, `version_match_path`, `enabled`, `notes`.

```json
[{"provider_table":"dimAccount","archetype":"SCD2","sk_col":"keyAccount","nk_cols":["accountNumber"],"effective_start_col":"effectiveStartDate","effective_end_col":"effectiveEndDate","topo_level":0}]
```

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

1. Fill **1–4**, **11** (minimum). Run **`00_setup`** → **`01_config_discovery`**.
2. **`01b_repair_triage`** (or **15**) → queue consumers.
3. Run **02** → **03** (widgets **8–9**, **16–19** if needed).
4. **21**=`classify` → **`00_setup`** → **04** classify.
5. Fill **23** (and **24** if Path B). **21**=`populate` → **`00_setup`** → **04** populate.
6. **05** → sign-off → **06** → **05** again.

---

## Troubleshooting

| Symptom | Check |
|---------|--------|
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
