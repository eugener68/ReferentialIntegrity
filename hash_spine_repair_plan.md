# Hash-Spine Surrogate Key Repair — Detailed Plan by Use Case

**Non-destructive restoration of referential integrity using durable `xxhash64` natural-key
columns, version-aware key-maps, validated before any surrogate key is touched.**

Covers four table archetypes:

| Archetype | Shape | Role in repair |
|---|---|---|
| **SCD1 dimension** | 1 row per member, own SK | Key provider |
| **SCD2 dimension** | N versioned rows per member, own SK, validity columns | Key provider |
| **Fact** | FK columns to one or more dims, no own SK consumed elsewhere | Key consumer |
| **Hub SCD2** | Versioned rows, **own SK** (consumed by facts/other hubs) **and multiple FKs** to dims and other SCD2s | Key provider **and** consumer |

---

## 0. Method Recap (why this works)

1. Surrogate keys were regenerated during reload; facts/hubs still hold old SKs.
   SK ranges overlap, so corruption is silent.
2. Natural keys survive reloads. We add an **immutable hash column** —
   `nk_hash = xxhash64(normalized natural key)` — to every key-providing table,
   and the same column(s) to every key-consuming table.
3. **Key-map tables** translate `old_sk → natural key → nk_hash → new_sk`
   (and, for SCD2, → the correct *version*).
4. Hashes are populated on consumers **without touching any SK**. Referential
   integrity is then proven by joining on hashes.
5. Only after validation passes are SKs **swept** — via the hash join (never via
   `old_sk → new_sk`, eliminating the overlapping-range hazard). The sweep is
   idempotent: running it twice is a no-op.
6. The hash columns stay forever as the reload-proof join spine.

```
            (1) add+populate nk_hash          (2) build key-maps
  legacy dims ────────► snapshots ────────► keymap.<dim>_keymap
  (federation)                                      │
                                                    │ (3) classify consumers
                                                    ▼
  facts / hubs ──(4) add+populate nk_hash per FK role──► NULL = orphan report
                                                    │
                                                    ▼
                              (5) RI validation on hash joins  ◄── HARD GATE
                                                    │
                                                    ▼
                              (6) idempotent SK sweep via hash (+version logic)
```

---

## 1. Foundations (do once, before any use case)

### 1.1 One normalization rule for natural keys — everywhere

> **Where hashes live — read this first.** SQL Server is **read-only**
> throughout this plan: no column is added, no hash is computed, nothing is
> written on the source side. All hashes are computed **in Databricks** by one
> expression applied to two inputs: (a) the legacy NK values, on the fly during
> the snapshot CTAS from the foreign catalog, and (b) the target NK values, when
> populating dims/hubs/facts. The supernatural key is **persisted only in the
> target estate**. The legacy-side hash exists only as a derived column inside
> the Databricks snapshot — strictly an optimization and a parity check (the
> key-map could equally join on the normalized `natural_key` string).

The single expression must be applied with **byte-identical normalization** to
both inputs, or legacy and target hashes for the same member won't match:

```sql
-- The ONLY allowed NK expression. Composite keys joined with '||',
-- NULL components replaced by a sentinel (NULL || 'x' = NULL in SQL).
xxhash64(
  coalesce(upper(trim(cast(nk_col_1 as string))), '~NULL~')
  || '||' ||
  coalesce(upper(trim(cast(nk_col_2 as string))), '~NULL~')
) AS nk_hash
```

Rationale: SQL Server's default collation is case-insensitive and ignores
trailing whitespace; Spark compares exactly. `upper(trim(...))` reconciles the two.
Define this expression **once per dimension** in a config table or notebook
function and reuse it verbatim for: legacy snapshot, target dim, key-map, every
consumer. Hand-retyping it per table is how this fails.

### 1.2 Decimal/date natural keys

`cast(... as string)` of decimals/dates can format differently across engines
(`1.0` vs `1.00`, `2024-01-01` vs `2024-01-01 00:00:00`). For non-string NK
components, normalize the representation explicitly, e.g.
`date_format(col, 'yyyy-MM-dd')`, `cast(cast(col as bigint) as string)`.
Verify with a sample before trusting any hash.

### 1.3 Collision stance

`xxhash64` over realistic dimension cardinalities (even hundreds of millions of
members) has negligible collision probability, but verify rather than assume —
it's one query per dim:

```sql
SELECT nk_hash, COUNT(DISTINCT natural_key) c
FROM <table> GROUP BY nk_hash HAVING COUNT(DISTINCT natural_key) > 1;
-- must return 0 rows
```

### 1.4 Schemas & medallion placement

`target_catalog` is your Databricks catalog containing the `bronze`, `silver`,
and `gold` schemas. Two **new schemas** are created alongside them — deliberately
*outside* the medallion trio, because snapshots and key-maps are migration/repair
artifacts with their own lifecycle, not part of the bronze→silver→gold data flow:

```sql
CREATE SCHEMA IF NOT EXISTS target_catalog.staging;   -- legacy snapshots (retire after sign-off + retention)
CREATE SCHEMA IF NOT EXISTS target_catalog.keymap;    -- key-maps (KEEP PERMANENTLY — lineage/audit record)
```

Throughout this document, **`target_catalog.silver.*` is the repair target** —
this plan assumes (per your architecture) that **all surrogate keys are minted
in silver**, where SCD processing and hub conformance happen. Layer mapping:

| Layer | Action |
|---|---|
| `bronze` | **Nothing.** Raw data carries no surrogate keys; never retrofit columns into bronze. |
| `silver` | **The repair target.** Hash columns, key-maps, classification, validation, and SK sweeps all operate on silver tables, exactly as written in this document. Prevention also lands here permanently: silver load logic computes `nk_hash` at ingest from now on, so gold inherits it for free. |
| `gold` | **Derived — never repaired directly.** Gold is rebuilt from silver: (1) **pause all gold-refresh jobs** before hash population begins; (2) repair and sweep silver per this document; (3) re-run the gold builds; (4) validate referential integrity **again in gold** (the §6/C3 battery on gold facts vs gold dims — silver-correct does not automatically prove the gold build logic didn't reintroduce a key mismatch); (5) resume schedules. |

**Operational rule:** freeze/quiesce both the silver load pipelines and the gold
refresh jobs for the whole duration of hash population → validation → sweep →
gold rebuild. A load running between validation and sweep invalidates the
validation; a gold refresh running mid-repair publishes a half-repaired state
to BI consumers.

### 1.5 Freeze a reference point

Snapshot every legacy dim/hub from the foreign catalog **once**, tag with
`snapshot_at`. All maps build from snapshots, never from live federation
(stability, performance, audit trail). The snapshot CTAS computes `nk_hash`
in flight — this is the only place a legacy-side hash exists, and it lives in
`target_catalog.staging`, not in SQL Server. Why bother hashing the snapshot at all,
rather than joining the key-map on natural-key strings? Three cheap reasons:
(1) building the key-map on `o.nk_hash = n.nk_hash` and getting the expected
MATCHED counts **proves the normalization expression** produces identical
values on both inputs — essential, since this expression becomes your permanent
join spine; (2) BIGINT joins outperform long composite-string joins across the
repeated audit re-runs; (3) the key-map carries `nk_hash` as a column anyway,
so the work is the same, just earlier and more testable.

---

## 2. Version Matching for SCD2 — the central design decision

### 2.1 The question you must answer per SCD2 table

A member-level hash (`xxhash64(NK)`) identifies the **member**, not the
**version**. To map old SK → new SK at *version* grain, you need a version
discriminator that is identical in the legacy table and the reloaded table.

**Your proposal — and when it's right:** use `effectiveStartDate` (+
`recordStatus` for uniqueness) as the version discriminator inside the key-map.
Within one system, `(NK, effectiveStartDate, recordStatus)` is indeed unique —
two versions of a member cannot share the same start date and status
simultaneously. **This works across systems if and only if the reload preserved
effectiveStartDate values**, i.e. effective dates are **source-derived business
dates** (contract start, price-valid-from, change timestamp carried in the
source), not **load-derived timestamps** (when the ETL happened to insert the row).

- Source-derived ⇒ a re-pull from source reproduces the same dates ⇒
  `(NK, effectiveStartDate)` is a stable cross-system version identity ⇒
  **Path A** below. Your idea is correct and is the preferred path.
- Load-derived, or the reload collapsed/merged/re-cut versions ⇒ dates differ
  between legacy and reloaded table ⇒ exact matching fails ⇒ **Path B**.

### 2.2 Diagnostic — run this per SCD2 table before choosing a path

```sql
-- Compare version inventories at (member, start date) grain.
WITH legacy AS (
  SELECT nk_hash, effectiveStartDate, COUNT(*) c
  FROM target_catalog.staging.legacy_<dim>
  GROUP BY nk_hash, effectiveStartDate
),
target AS (
  SELECT nk_hash, effectiveStartDate, COUNT(*) c
  FROM target_catalog.silver.<dim>
  GROUP BY nk_hash, effectiveStartDate
)
SELECT
  CASE WHEN l.nk_hash IS NULL THEN 'VERSION_ONLY_IN_TARGET'
       WHEN t.nk_hash IS NULL THEN 'VERSION_ONLY_IN_LEGACY'
       ELSE 'VERSION_MATCHED' END AS status,
  COUNT(*) AS version_rows
FROM legacy l
FULL OUTER JOIN target t
  ON l.nk_hash = t.nk_hash AND l.effectiveStartDate = t.effectiveStartDate
GROUP BY 1;
```

Interpretation:

| Result | Meaning | Path |
|---|---|---|
| ≥ ~99% `VERSION_MATCHED` | Effective dates survived the reload | **Path A** (exact version match) |
| `VERSION_ONLY_IN_TARGET` small surplus | New versions arrived after legacy snapshot (normal if source kept moving) | Path A, surplus is expected |
| Material mismatch both directions | Boundaries were re-cut or dates are load timestamps | **Path B** (event-date resolution) |

Run it; don't assume. Mixed populations are possible (some dims Path A, some Path B).

### 2.3 Path A — exact version identity via `(NK, effectiveStartDate [, recordStatus])`

The key-map carries the version discriminator, and optionally a **version-level
hash** for convenience:

```sql
ver_hash = xxhash64(<NK normalization expr> || '||' ||
                    date_format(effectiveStartDate, 'yyyy-MM-dd HH:mm:ss'))
```

Matching predicate, legacy → target:

```sql
ON  l.nk_hash            = t.nk_hash
AND l.effectiveStartDate = t.effectiveStartDate
-- add recordStatus ONLY as a tiebreaker if (nk, startDate) is not unique:
-- AND l.recordStatus    = t.recordStatus
```

> **⚠️ recordStatus caveat — important.** `recordStatus` (current/expired/active/
> deleted) is **time-variant**: the row that was `current` in the legacy snapshot
> may legitimately be `expired` in the reloaded table because newer versions
> arrived from source in between. Using `recordStatus` as a *matching* condition
> across snapshots taken at different times will wrongly orphan exactly those
> rows. Use it only:
> - as a **uniqueness tiebreaker** when your model genuinely allows two rows with
>   the same `(NK, effectiveStartDate)` distinguished only by status (e.g. an
>   active and a logically-deleted row), and then match it **categorically**
>   (deleted vs not-deleted), not on the current/expired axis; or
> - not at all, if `(NK, effectiveStartDate)` is already unique — verify:
>
> ```sql
> SELECT nk_hash, effectiveStartDate, COUNT(*)
> FROM target_catalog.silver.<dim> GROUP BY 1,2 HAVING COUNT(*) > 1;  -- 0 rows ⇒ drop recordStatus
> ```

With Path A, the key-map resolves old SK → new SK **deterministically at version
grain**, the fact/hub hash population can carry `ver_hash`, and **no event-date
logic is needed at sweep time**. This is the cleanest outcome.

### 2.4 Path B — boundaries re-cut: member hash + event-date resolution

If version identity didn't survive, the key-map matches versions by **window
overlap** (`l.start < t.end AND t.start < l.end`), flags one-to-many cases as
`AMBIGUOUS`, and the consumer-side resolution uses the **fact's event date**
against the *target* dim's validity windows:

```sql
ON  f.nk_hash    = d.nk_hash
AND f.event_date >= d.effectiveStartDate
AND f.event_date <  coalesce(d.effectiveEndDate, timestamp'9999-12-31')
```

Path B costs one extra predicate per consumer and needs a declared event-date
column per fact, but is robust to arbitrary boundary re-cuts.

### 2.5 Decision record

For each SCD2/Hub-SCD2 table, record the chosen path in the key-map table
properties (`'version_match' = 'A-exact' | 'B-event-date'`). Mixed estates are
fine; consumers just use the predicate matching their provider's path.

---

## 3. Key-Map Schema (one per key-providing table)

```sql
CREATE TABLE target_catalog.keymap.<provider>_keymap (
  natural_key        STRING    NOT NULL,  -- normalized, possibly composite
  nk_hash            BIGINT    NOT NULL,  -- member-level durable key
  -- version identity (NULL for SCD1):
  effectiveStartDate TIMESTAMP,           -- from TARGET row (Path A: == legacy)
  effectiveEndDate   TIMESTAMP,
  ver_hash           BIGINT,              -- Path A only: member+version hash
  -- the translation:
  old_sk             BIGINT,              -- legacy SK (what consumers hold now)
  new_sk             BIGINT,              -- target SK (what they should hold)
  map_status         STRING    NOT NULL,  -- see below
  created_at         TIMESTAMP NOT NULL
) USING DELTA;
```

| `map_status` | Meaning | Consumer effect |
|---|---|---|
| `MATCHED` | clean translation (version-exact on Path A) | drives repair |
| `ORPHAN_OLD` | legacy member/version missing after reload | consumer rows → unknown member; investigate the dim load |
| `ORPHAN_NEW` | exists only in target | none; informational |
| `AMBIGUOUS` | Path B only: legacy row overlaps several target windows | resolved per-consumer-row by event date |

---

## 4. Use Case A — SCD1 Dimension

*One row per member; `nk_hash` fully identifies the row.*

**A1. Add + populate the hash on the target dim** (column add is a metadata
operation; the UPDATE rewrites the table once):

```sql
ALTER TABLE target_catalog.silver.dim_customer ADD COLUMN nk_hash BIGINT;

UPDATE target_catalog.silver.dim_customer
SET nk_hash = xxhash64(coalesce(upper(trim(cast(customer_code as string))), '~NULL~'));
```

**A2. Uniqueness gate** (hash must be unique on SCD1 — duplicates mean the reload
double-inserted, or case-variants split; fix the dim first):

```sql
SELECT nk_hash, COUNT(*) FROM target_catalog.silver.dim_customer
GROUP BY nk_hash HAVING COUNT(*) > 1;   -- must be 0 rows
```

**A3. Snapshot the legacy dim** into `target_catalog.staging`, computing `nk_hash` in the
CTAS by applying the same hash expression to the legacy NK columns. (This runs
entirely in Databricks against the foreign catalog — SQL Server is read-only;
no column is created on the source.)

**A4. Build the key-map** (FULL OUTER JOIN so both orphan classes surface):

```sql
CREATE OR REPLACE TABLE target_catalog.keymap.dim_customer_keymap AS
SELECT
  coalesce(o.natural_key, n.natural_key)         AS natural_key,
  coalesce(o.nk_hash, n.nk_hash)                 AS nk_hash,
  CAST(NULL AS TIMESTAMP) effectiveStartDate,
  CAST(NULL AS TIMESTAMP) effectiveEndDate,
  CAST(NULL AS BIGINT)    ver_hash,
  o.old_sk, n.new_sk,
  CASE WHEN o.nk_hash IS NULL THEN 'ORPHAN_NEW'
       WHEN n.nk_hash IS NULL THEN 'ORPHAN_OLD'
       ELSE 'MATCHED' END                        AS map_status,
  current_timestamp()
FROM target_catalog.staging.legacy_dimcustomer o
FULL OUTER JOIN
     (SELECT customer_sk AS new_sk, nk_hash,
             upper(trim(cast(customer_code as string))) AS natural_key
      FROM target_catalog.silver.dim_customer) n
  ON o.nk_hash = n.nk_hash;
```

**A5. Audit:** status distribution; large `ORPHAN_OLD` ⇒ reload dropped members
⇒ fix the dimension before touching any consumer.

---

## 5. Use Case B — SCD2 Dimension

**B1. Run the §2.2 diagnostic → choose Path A or B.** Record the decision.

**B2. Add + populate hashes on the target dim:**

```sql
ALTER TABLE target_catalog.silver.dim_customer_scd2
  ADD COLUMNS (nk_hash BIGINT, ver_hash BIGINT);     -- ver_hash: Path A only

UPDATE target_catalog.silver.dim_customer_scd2
SET nk_hash  = xxhash64(<NK expr>),
    ver_hash = xxhash64(<NK expr> || '||' ||
               date_format(effectiveStartDate,'yyyy-MM-dd HH:mm:ss'));  -- Path A
```

**B3. Uniqueness gates:**

```sql
-- Path A: ver_hash unique
SELECT ver_hash, COUNT(*) FROM target_catalog.silver.dim_customer_scd2
GROUP BY ver_hash HAVING COUNT(*) > 1;               -- must be 0

-- Both paths: validity windows must not overlap per member
SELECT nk_hash, COUNT(*) AS overlapping_pairs
FROM target_catalog.silver.dim_customer_scd2 a
JOIN target_catalog.silver.dim_customer_scd2 b
  ON  a.nk_hash = b.nk_hash AND a.customer_sk < b.customer_sk
  AND a.effectiveStartDate < coalesce(b.effectiveEndDate, timestamp'9999-12-31')
  AND b.effectiveStartDate < coalesce(a.effectiveEndDate, timestamp'9999-12-31')
GROUP BY nk_hash;                                    -- must be 0 rows
```

Overlapping windows in the *target* dim are a reload bug; fix before mapping —
they will fan out every consumer join later.

**B4. Build the key-map.**

*Path A (exact version identity):*

```sql
CREATE OR REPLACE TABLE target_catalog.keymap.dim_customer_scd2_keymap AS
SELECT
  coalesce(o.natural_key, n.natural_key) AS natural_key,
  coalesce(o.nk_hash, n.nk_hash)         AS nk_hash,
  n.effectiveStartDate, n.effectiveEndDate,
  n.ver_hash,
  o.old_sk, n.new_sk,
  CASE WHEN o.nk_hash IS NULL THEN 'ORPHAN_NEW'
       WHEN n.nk_hash IS NULL THEN 'ORPHAN_OLD'
       ELSE 'MATCHED' END               AS map_status,
  current_timestamp()
FROM target_catalog.staging.legacy_dim_customer_scd2 o      -- carries effectiveStartDate
FULL OUTER JOIN
     (SELECT customer_sk new_sk, nk_hash, ver_hash, natural_key,
             effectiveStartDate, effectiveEndDate
      FROM target_catalog.silver.dim_customer_scd2) n
  ON  o.nk_hash = n.nk_hash
  AND o.effectiveStartDate = n.effectiveStartDate;
  -- + AND o.recordStatus_class = n.recordStatus_class   (only if needed per §2.3)
```

*Path B (window overlap):* as Path A but join on
`nk_hash` + window-overlap predicate, add the `AMBIGUOUS` status via
`COUNT(*) OVER (PARTITION BY o.old_sk) > 1`, and leave `ver_hash` NULL.

**B5. Audit** as in A5, plus: on Path A, the count of legacy versions with
`map_status='ORPHAN_OLD'` should reconcile with the §2.2 diagnostic's
`VERSION_ONLY_IN_LEGACY`.

---

## 6. Use Case C — Fact Tables (consumers)

Repeat per fact **per FK role** (a fact with `ship_to_customer_sk` and
`bill_to_customer_sk` is two roles → two hash columns).

**C1. Classify the fact first — mandatory gate.**
A fact reloaded *after* the dims already carries new SKs; joining it to the
key-map on `old_sk` will silently assign **wrong hashes that still pass RI**
(old/new SK ranges overlap). Classify:

```sql
SELECT coalesce(km.map_status, 'NOT_IN_LEGACY_DIM') AS status, COUNT(*)
FROM target_catalog.silver.fact_sales f
LEFT JOIN target_catalog.keymap.dim_customer_keymap km ON f.customer_sk = km.old_sk
GROUP BY 1;
```

| Classification | Evidence | Hash population source |
|---|---|---|
| **Legacy-keyed** (needs repair) | dominated by `MATCHED`/`ORPHAN_OLD`; team confirms it wasn't reloaded | **key-map** on `old_sk` (C2a) |
| **Already-reloaded** (SKs already correct) | known reloaded after dims; `NOT_IN_LEGACY_DIM` material | **current dim** on `sk` (C2b) |
| **Mixed / unclear** | conflicting evidence | STOP — establish per-row provenance (load timestamps, pipeline logs) before proceeding |

> The classification cannot be derived from the data alone when ranges overlap —
> it requires knowing the load history. Get the migration team to attest which
> facts were reloaded, then verify the attestation against this query.

**C2. Add + populate hash columns (SK untouched):**

```sql
ALTER TABLE target_catalog.silver.fact_sales
  ADD COLUMNS (customer_nk_hash BIGINT);   -- one per FK role; Path A SCD2 may also add customer_ver_hash
```

*C2a — legacy-keyed fact, via key-map:*

```sql
MERGE INTO target_catalog.silver.fact_sales f
USING (SELECT old_sk, nk_hash, ver_hash
       FROM target_catalog.keymap.dim_customer_scd2_keymap
       WHERE map_status = 'MATCHED' AND old_sk IS NOT NULL) km
  ON f.customer_sk = km.old_sk
WHEN MATCHED THEN UPDATE SET
  f.customer_nk_hash = km.nk_hash
  -- , f.customer_ver_hash = km.ver_hash      -- Path A
;
-- Rows not matched keep NULL hash = your orphan report. Do not invent values.
```

*Path B note:* on Path B, one `old_sk` may map to several target versions
(`AMBIGUOUS`); the **member** hash is still unique per `old_sk`, so C2a is safe —
version resolution happens at validation/sweep via event date, not here.
Deduplicate the key-map to member grain in the USING clause:
`SELECT DISTINCT old_sk, nk_hash FROM ... WHERE map_status IN ('MATCHED','AMBIGUOUS')`.

*C2b — already-reloaded fact, via current dim (NOT the key-map):*

```sql
MERGE INTO target_catalog.silver.fact_returns f
USING (SELECT customer_sk, nk_hash FROM target_catalog.silver.dim_customer) d
  ON f.customer_sk = d.customer_sk
WHEN MATCHED THEN UPDATE SET f.customer_nk_hash = d.nk_hash;
```

**C3. Validate referential integrity on hashes — the hard gate:**

```sql
-- (1) Coverage: NULL hashes are exactly the expected orphans, nothing more
SELECT COUNT(*) FROM target_catalog.silver.fact_sales WHERE customer_nk_hash IS NULL;

-- (2) Member-level RI: every populated hash exists in the dim
SELECT COUNT(*) FROM target_catalog.silver.fact_sales f
LEFT ANTI JOIN target_catalog.silver.dim_customer_scd2 d
  ON f.customer_nk_hash = d.nk_hash
WHERE f.customer_nk_hash IS NOT NULL;                       -- must be 0

-- (3) Version-level RI:
--   Path A: every ver_hash exists
SELECT COUNT(*) FROM target_catalog.silver.fact_sales f
LEFT ANTI JOIN target_catalog.silver.dim_customer_scd2 d
  ON f.customer_ver_hash = d.ver_hash
WHERE f.customer_ver_hash IS NOT NULL;                      -- must be 0
--   Path B: every (hash, event_date) lands in exactly one window
SELECT f.fact_id, COUNT(d.customer_sk) AS windows_hit
FROM target_catalog.silver.fact_sales f
LEFT JOIN target_catalog.silver.dim_customer_scd2 d
  ON  f.customer_nk_hash = d.nk_hash
  AND f.order_date >= d.effectiveStartDate
  AND f.order_date <  coalesce(d.effectiveEndDate, timestamp'9999-12-31')
WHERE f.customer_nk_hash IS NOT NULL
GROUP BY f.fact_id HAVING COUNT(d.customer_sk) <> 1;        -- must be 0 rows

-- (4) Measure reconciliation per member vs legacy fact (catches value SHIFTED
--     between members — the failure RI checks cannot see):
--     SUM(measure) GROUP BY nk_hash, compared legacy vs target via federation.
```

**C4. Sweep the SK — via hash, in place, idempotent.** Only after C3 is clean:

```sql
-- SCD1 / Path A SCD2 (deterministic single-row match):
MERGE INTO target_catalog.silver.fact_sales f
USING target_catalog.silver.dim_customer_scd2 d
  ON f.customer_ver_hash = d.ver_hash            -- SCD1: f.customer_nk_hash = d.nk_hash
WHEN MATCHED AND f.customer_sk <> d.customer_sk
  THEN UPDATE SET f.customer_sk = d.customer_sk;

-- Path B SCD2 (event date picks the version):
MERGE INTO target_catalog.silver.fact_sales f
USING target_catalog.silver.dim_customer_scd2 d
  ON  f.customer_nk_hash = d.nk_hash
  AND f.order_date >= d.effectiveStartDate
  AND f.order_date <  coalesce(d.effectiveEndDate, timestamp'9999-12-31')
WHEN MATCHED AND f.customer_sk <> d.customer_sk
  THEN UPDATE SET f.customer_sk = d.customer_sk;

-- Orphans last (NULL hash → unknown member):
UPDATE target_catalog.silver.fact_sales SET customer_sk = -1 WHERE customer_nk_hash IS NULL;
```

The `f.customer_sk <> d.customer_sk` guard makes the sweep a no-op on re-run and
gives you a free count of touched rows from the MERGE metrics.

**C5. Post-sweep check:** SK anti-join to the dim returns 0 (excluding `-1`), and
re-running C3 still passes.

---

## 7. Use Case D — Hub SCD2 (own SK + multiple FKs)

A Hub SCD2 is **both sides at once**:
- a **provider**: its own `hub_sk` is referenced by facts (and possibly other hubs);
- a **consumer**: it carries FKs to several dimensions and other SCD2s, each
  possibly broken independently.

Example shape:

```
hub_customer_account_scd2(
  hub_sk,                       -- provider key (referenced by fact_*)
  account_nk, customer_sk, product_sk, contract_sk,   -- consumer FKs
  effectiveStartDate, effectiveEndDate, recordStatus, ...
)
```

**D1. Order of operations — providers before consumers.** Build the dependency
graph: every hub depends on the dims/SCD2s it references; facts depend on dims,
SCD2s, and hubs. Process in topological order:

```
Level 0: leaf dims (SCD1 + SCD2)            → use cases A/B
Level 1: hubs referencing only Level 0      → this use case
Level 2: hubs referencing Level-1 hubs      → this use case again
Level 3: facts                              → use case C (a hub FK is just
                                              another role; its provider keymap
                                              is the hub's keymap)
```

A hub's **provider** side can be mapped immediately (its own NK doesn't depend on
anything), but its **consumer-side sweep** must wait until all referenced
providers are validated. Cycles (two hubs referencing each other) don't block
this method — hashes and key-maps have no ordering constraint; only the *sweeps*
should run providers-first.

**D2. Provider side — treat exactly as Use Case B:**
- Run the §2.2 version diagnostic on the hub itself (its own
  `effectiveStartDate`); choose Path A/B for the hub's own versions.
- Add `nk_hash` (+ `ver_hash` on Path A) computed from the **hub's own natural
  key** (e.g. `account_nk`).
- Snapshot the legacy hub; build `target_catalog.keymap.hub_customer_account_keymap`
  with `old_sk = legacy hub_sk`, `new_sk = current hub_sk`.
- This key-map is what Level-3 facts will use for their `hub_sk` FK role.

**D3. Consumer side — one hash column per FK role, treat each as Use Case C:**

```sql
ALTER TABLE target_catalog.silver.hub_customer_account_scd2 ADD COLUMNS (
  customer_nk_hash BIGINT,   -- + customer_ver_hash if dim_customer is SCD2 Path A
  product_nk_hash  BIGINT,
  contract_nk_hash BIGINT    -- contract is itself an SCD2 → version logic applies
);
```

- **Classify the hub per FK role** (C1): it is entirely possible the hub was
  reloaded against the new `dim_product` (FK already correct → populate hash from
  current dim) but still carries legacy `customer_sk` (→ populate from key-map).
  Classification is per-role, not per-table.
- Populate each role's hash from the appropriate source (C2a/C2b), one MERGE per
  role. SKs untouched throughout.

**D4. Version semantics for SCD2→SCD2 references — what is the “event date”?**
When a hub row references another SCD2 (e.g. `contract_sk`), the correct version
of the contract is the one valid **as of the hub row's own validity**, by
convention **as of the hub row's `effectiveStartDate`** (the referenced version
that was true when this hub version came into existence):

```sql
-- Path B resolution for an SCD2-typed FK inside a hub:
ON  h.contract_nk_hash      =  c.nk_hash
AND h.effectiveStartDate    >= c.effectiveStartDate
AND h.effectiveStartDate    <  coalesce(c.effectiveEndDate, timestamp'9999-12-31')
```

If the referenced provider is **Path A**, none of this is needed — the key-map
already resolved the exact version per `old_sk`, and the hub's `ver_hash` for
that role comes straight from the map.

> ⚠️ Edge case to audit explicitly: a hub version whose `effectiveStartDate`
> falls in **no** window of the referenced SCD2 (the referenced member didn't
> exist yet, or windows were re-cut past it). These surface in validation D5(3)
> with `windows_hit = 0`; decide per case (unknown member vs. window correction)
> — do not let the sweep decide silently.

**D5. Validate** — the full C3 battery, **per FK role**, plus one hub-specific
check: after all roles validate individually, verify **row-level consistency**
(every hub row resolves all its roles — a row that resolves customer but orphans
contract is one logical record half-repaired):

```sql
SELECT
  SUM(CASE WHEN customer_nk_hash IS NULL THEN 1 ELSE 0 END) AS customer_orphans,
  SUM(CASE WHEN product_nk_hash  IS NULL THEN 1 ELSE 0 END) AS product_orphans,
  SUM(CASE WHEN contract_nk_hash IS NULL THEN 1 ELSE 0 END) AS contract_orphans,
  SUM(CASE WHEN customer_nk_hash IS NULL
            OR product_nk_hash  IS NULL
            OR contract_nk_hash IS NULL THEN 1 ELSE 0 END)  AS rows_partially_orphaned
FROM target_catalog.silver.hub_customer_account_scd2;
```

**D6. Sweep** — per FK role, hash-join MERGE as in C4 (with D4's as-of predicate
for Path-B SCD2 roles). All roles can be swept in **one MERGE** once all are
validated (single table rewrite instead of N):

```sql
MERGE INTO target_catalog.silver.hub_customer_account_scd2 h
USING (
  SELECT h2.hub_sk AS target_row,
         dc.customer_sk AS new_customer_sk,
         dp.product_sk  AS new_product_sk,
         ct.contract_sk AS new_contract_sk
  FROM target_catalog.silver.hub_customer_account_scd2 h2
  LEFT JOIN target_catalog.silver.dim_customer_scd2 dc ON h2.customer_ver_hash = dc.ver_hash
  LEFT JOIN target_catalog.silver.dim_product        dp ON h2.product_nk_hash  = dp.nk_hash
  LEFT JOIN target_catalog.silver.dim_contract_scd2  ct
         ON  h2.contract_nk_hash   = ct.nk_hash
         AND h2.effectiveStartDate >= ct.effectiveStartDate
         AND h2.effectiveStartDate <  coalesce(ct.effectiveEndDate, timestamp'9999-12-31')
) r ON h.hub_sk = r.target_row
WHEN MATCHED THEN UPDATE SET
  h.customer_sk = coalesce(r.new_customer_sk, -1),
  h.product_sk  = coalesce(r.new_product_sk,  -1),
  h.contract_sk = coalesce(r.new_contract_sk, -1);
```

**D7. Only then** repair Level-3 facts that reference `hub_sk`, using the hub's
provider key-map from D2 — standard Use Case C.

> Note: the hub's **own SK** (`hub_sk`) is the provider key and is **never
> swept** — it was regenerated by the reload and is now the canonical value;
> it's the *facts referencing it* that get re-keyed via the hub's key-map.
> (If instead the hub itself was *not* reloaded but its dims were — then its own
> SK is fine by definition and only D3–D6 apply. Classify first.)

---

## 8. End-to-End Orchestration Checklist

> Run §9 (non-disruptive rehearsal) to green completion **before** executing
> the mutating steps (4, 7, 9–12) of this checklist against production.
> Steps 0, 2, 3, 5, 6 are read-only on prod tables and can start immediately.

```
□ 0.  Config table: per provider — NK columns, normalization expr, SCD type,
      validity cols; per consumer — FK roles, provider, event-date col, class.
□ 1.  FREEZE pipelines: pause silver loads AND gold-refresh jobs for the
      affected tables. Nothing below is valid if data moves underneath it.
□ 2.  Snapshots of all legacy providers (dims, SCD2s, hubs), tagged.
□ 3.  §2.2 version diagnostic per SCD2/hub → record Path A/B per table.
□ 4.  nk_hash (+ver_hash) added & populated on all SILVER providers.
      Uniqueness + window-overlap gates pass.
□ 5.  Key-maps built for all providers. Status distributions reviewed,
      ORPHAN_OLD explained or dim fixed & rebuilt.
□ 6.  Every consumer × FK role CLASSIFIED (legacy-keyed / already-reloaded /
      mixed→stop) with evidence recorded.
□ 7.  Hash columns added & populated on all SILVER consumers (facts, hubs),
      per role, from the correct source. SKs untouched.
□ 8.  Validation battery green per role: coverage, member RI, version RI,
      exactly-one-window, measure reconciliation, hub row-level consistency.
      ◄◄ HARD GATE — business sign-off here ►►
□ 9.  Sweeps in silver, providers-first: Level-0 consumers' roles → hubs →
      facts. Each sweep idempotent (guarded MERGE), re-runnable.
□ 10. Post-sweep in silver: SK anti-joins zero everywhere (excl. unknown
      member); re-run step-8 battery.
□ 11. REBUILD GOLD from repaired silver. Then re-run the RI battery in gold
      (gold facts vs gold dims) — silver-correct does not prove the gold build
      didn't reintroduce a mismatch. Spot-check BI reports vs legacy.
□ 12. RESUME pipelines (silver loads first, then gold refresh schedules).
□ 13. Keep: keymap schema (permanent), snapshots (until sign-off + retention),
      hash columns (forever — they are the new join spine).
□ 14. Pipeline changes: populate nk_hash at ingest in SILVER load logic on
      every provider AND consumer (gold inherits it); add post-load RI
      assertions; enforce dims-before-facts ordering.
```

---

## 9. Non-Disruptive Rehearsal — testing the whole method without touching prod

### 9.1 The key observation: most of this method is already read-only

Map every stage to what it actually writes:

| Stage | Writes to prod silver/gold? | Safe on live prod? |
|---|---|---|
| Legacy snapshots (§1.5) | No — writes only `target_catalog.staging` | ✅ Yes |
| Version diagnostics (§2.2) | No — pure SELECTs | ✅ Yes |
| Key-map builds (§3–§5) | No — writes only `target_catalog.keymap` | ✅ Yes |
| Consumer classification (§6 C1) | No — pure SELECTs | ✅ Yes |
| All audits & status reviews | No | ✅ Yes |
| **Hash column add + populate** | **Yes** (schema change + table rewrite) | ⚠️ Rehearse on clones first |
| **SK sweeps** | **Yes** (table rewrite) | ⚠️ Rehearse on clones first |
| **Gold rebuild** | **Yes** (gold refresh) | ⚠️ Rehearse in parallel schema |

So the entire investigative half of the project — snapshots, diagnostics,
key-maps, classification, every audit — **can run against live production
today, with no pipeline freeze and no risk**, because it only ever writes to
the two new schemas. You learn your real `MATCHED`/`ORPHAN_OLD`/`AMBIGUOUS`
numbers, your Path A/B decisions, and your per-fact classifications before
deciding anything. Only the mutating half needs a rehearsal environment.

(One nuance: audits run against moving data can drift slightly between runs if
pipelines are loading; that's noise, not risk. The *final* keymap build before
the real sweep is re-run inside the frozen window — see 9.5.)

### 9.2 Rehearsal environment via zero-copy clones

Delta **shallow clones** give you instantly-created, zero-copy, fully writable
copies of the prod tables. Writes to a clone (adding `nk_hash`, sweeping SKs)
create new files owned by the clone and **never modify the source table**:

```sql
CREATE SCHEMA IF NOT EXISTS target_catalog.rehearsal_silver;
CREATE SCHEMA IF NOT EXISTS target_catalog.rehearsal_gold;

-- one per table in repair scope; creation is a metadata operation (seconds)
CREATE TABLE target_catalog.rehearsal_silver.dim_customer_scd2
  SHALLOW CLONE target_catalog.silver.dim_customer_scd2;

CREATE TABLE target_catalog.rehearsal_silver.fact_sales
  SHALLOW CLONE target_catalog.silver.fact_sales;
-- ... etc. for every provider and consumer in scope
```

Notes:
- Shallow clone freezes the table **as of clone time** — a free consistency
  point for the rehearsal; pipelines can keep running on prod.
- ⚠️ Don't run `VACUUM` on the **source** tables while rehearsal clones exist —
  it can remove files the shallow clones still reference. Suspend vacuum jobs
  for the in-scope tables, or use `DEEP CLONE` (full copy, costs storage and
  time, but fully independent) if vacuum can't be paused.
- The rehearsal key-maps/snapshots can simply be the real ones from 9.1 — they
  live in `staging`/`keymap` and are read-only inputs to the rehearsal.

### 9.3 Rehearsal execution

Run the **entire mutating half** of the plan against the clones, exactly as
written, with one global substitution: `silver.` → `rehearsal_silver.`:

1. **Pilot first:** one small SCD1 dim + one fact referencing it, end-to-end
   (hash add → populate → validate → sweep → checks). Builds confidence in the
   normalization expression and the runbook before scaling.
2. **Full scope:** hash population on all cloned providers and consumers,
   per-role; full §6/§7 validation battery; sweeps in dependency order
   (dims → hubs → facts).
3. **Measure everything.** Record per-table wall-clock for hash population and
   sweep (each is a full table rewrite) — these numbers ARE your production
   freeze-window estimate, which is otherwise guesswork.
4. **Rebuild gold in parallel:** point the gold build jobs (parameterized run,
   not the scheduled instance) at `rehearsal_silver` inputs and
   `rehearsal_gold` outputs. Run the RI battery on `rehearsal_gold`. Point a
   copy of key BI reports/queries at `rehearsal_gold` and compare numbers
   against (a) current prod gold and (b) the legacy warehouse via federation —
   differences vs (a) are the corruption you're fixing; matches vs (b) are the
   proof of repair.
5. **Sign-off package:** validation outputs, measure reconciliations, BI
   comparisons, and the timing table — this is what the business approves
   before any prod mutation.
6. **Tear down:** `DROP SCHEMA target_catalog.rehearsal_silver CASCADE` (and
   gold). Keep the sign-off package; re-enable vacuum.

### 9.4 What the rehearsal can't fully prove

- **Drift:** prod has moved since clone time, so prod-run counts will differ
  slightly from rehearsal counts. Expected; the *shape* of results (zero
  RI violations, reconciled measures) is what must reproduce.
- **Concurrent-load behavior:** the rehearsal ran on frozen clones; production
  runs inside a real freeze window — which is why the freeze (checklist step 1)
  is non-negotiable even after a perfect rehearsal.

### 9.5 Production execution after a green rehearsal

The prod run is the rehearsed runbook plus three deltas:

1. **Freeze pipelines** (checklist §8 step 1), sized by the rehearsal timings
   plus margin.
2. **Rebuild key-maps once inside the frozen window** (cheap — minutes), so the
   maps reflect frozen-state dims, not week-old ones. All audits should match
   rehearsal shape.
3. **Gold via parallel-build-and-swap, not in-place refresh:** build the
   repaired gold into a parallel location, validate, then swap
   (`ALTER TABLE ... RENAME` pairs, or repoint views if BI reads through
   views — even cleaner: zero table renames, instant cutover, instant
   rollback by repointing back). Current BI consumers stay on the old gold
   until the moment of an atomic, reversible switch.

This sequencing means production silver is mutated only after the method was
proven twice (rehearsal + frozen-window audits), and production gold consumers
never see an intermediate state at all.

---

## 10. Why the SKs Can Eventually Be Demoted

After this repair, every table carries a deterministic, reload-proof identity.
The integer SKs become an internal performance detail. Long term you can:
- keep integer SKs for join performance and storage, regenerated freely as long
  as loads resolve them through `nk_hash` (the key-map pattern becomes the
  standard lookup step), or
- migrate joins to the hash columns outright and drop the historical key-maps
  from the hot path (keep them as lineage).

Either way, the class of incident that started this — "someone reloaded a
dimension without thinking" — becomes structurally harmless: a reload regenerates
SKs, the next consumer load resolves through hashes, nothing breaks.

---

*Companion to `surrogate_key_repair_notebook.py` and
`surrogate_key_repair_method.md`. This document supersedes the rebuild-based
fact repair in the earlier method doc with the non-destructive hash-spine
approach; the earlier doc's snapshot, normalization, and audit sections remain
applicable.*
