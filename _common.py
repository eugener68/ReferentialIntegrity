# Databricks notebook source
# MAGIC %md
# MAGIC # _common — shared library (do **not** run this notebook directly)
# MAGIC
# MAGIC This file is a **library notebook**, not part of the run sequence. Every numbered
# MAGIC notebook (`00_setup` … `06_sweep`) loads it automatically with `%run ./_common`.
# MAGIC You never need to open or run `_common` yourself.
# MAGIC
# MAGIC Defines:
# MAGIC - **one** widget set (notebook `00_setup`) persisted to `ri_repair.package_settings`
# MAGIC - the **single** NK normalization / hash expression (§1.1–1.2 of the plan)
# MAGIC - config-table accessors and small SQL utilities
# MAGIC
# MAGIC **Never** hand-retype the NK expression anywhere else — every notebook builds it
# MAGIC from `nk_hash_expr()` so legacy, target, key-map and consumers stay byte-identical.

# COMMAND ----------

import json
import re
import uuid
import datetime
from pyspark.sql import functions as F
from pyspark.sql import types as T

NULL_SENTINEL = "~NULL~"
RUN_ID = f"{datetime.datetime.utcnow().strftime('%Y%m%dT%H%M%S')}_{uuid.uuid4().hex[:8]}"

BASE_WIDGET_DEFAULTS = {
    "target_catalog": "target_catalog",     # Unity Catalog catalog with silver/gold
    "source_catalog": "legacy_src",         # Lakehouse Federation foreign catalog (read-only)
    "source_schema":  "dbo",                # schema in the foreign catalog (names mirrored)
    "target_schema":  "silver",             # repair target; set to rehearsal_silver for rehearsal
    "config_schema":  "ri_repair",          # config + audit/result tables
    "staging_schema": "staging",            # legacy snapshots
    "keymap_schema":  "keymap",             # key-maps (permanent)
    "provider_filter": "*",                 # '*' or comma-separated provider table names
    "consumer_filter": "*",                 # '*' or comma-separated consumer table names
    "dry_run": "false",                     # 'true' = print mutating SQL instead of executing
}

PIPELINE_WIDGET_DEFAULTS = {
    "refresh_snapshots": "false",           # 02: re-snapshot even if legacy_* exists
    "auto_set_path": "true",                # 02: write suggested Path A/B into config
    "path_a_threshold": "0.99",             # 02: min VERSION_MATCHED share for Path A
    "recompute_hashes": "false",            # 03/04: recompute all hash rows
    "build_keymaps": "true",                # 03: build key-map tables
    "mode": "classify",                     # 04: 'classify' | 'populate'
    "suggest_threshold": "0.95",            # 04: key-map share to suggest LEGACY_KEYED
    "measure_tolerance": "0.01",            # 05: measure reconciliation tolerance
    "require_validation": "true",           # 06: refuse sweep unless 05 is green
    "orphan_sk": "-1",                      # 06: unknown-member SK
    "apply_orphan_sk": "false",             # 06: point NULL-hash rows at orphan_sk
}

CONFIG_JSON_WIDGETS = {
    "providers_json": "[]",
    "manual_consumers_json": "[]",
    "exclude_consumers_json": "[]",
    "consumer_overrides_json": "[]",
    "classifications_json": "[]",
    "repair_selection_json": "[]",
}

REPAIR_MODE_WIDGET = {
    "repair_mode": "opt_in",   # opt_in: only SELECTED/VERIFIED are repaired; opt_out: legacy (all except SKIPPED/FIXED)
}

ALL_WIDGET_DEFAULTS = {
    **BASE_WIDGET_DEFAULTS,
    **PIPELINE_WIDGET_DEFAULTS,
    **REPAIR_MODE_WIDGET,
    **CONFIG_JSON_WIDGETS,
}

# Display order in 00_setup widget panel (matches RUNBOOK.md phases A→F, not alphabetical)
SETUP_WIDGET_ORDER = [
    # A — Environment & scope
    "target_catalog", "target_schema", "source_catalog", "source_schema",
    "config_schema", "staging_schema", "keymap_schema",
    "provider_filter", "consumer_filter", "dry_run",
    # A — Provider / consumer registry (JSON)
    "providers_json", "manual_consumers_json", "exclude_consumers_json",
    "repair_mode", "repair_selection_json",
    # C — Snapshots & provider key-maps (02–03)
    "refresh_snapshots", "auto_set_path", "path_a_threshold",
    "build_keymaps", "recompute_hashes",
    # D — Classify consumers (04)
    "mode", "suggest_threshold",
    # E — Attest & populate (04)
    "classifications_json", "consumer_overrides_json",
    # F — Validate & sweep (05–06)
    "measure_tolerance", "require_validation", "orphan_sk", "apply_orphan_sk",
]

assert set(SETUP_WIDGET_ORDER) == set(ALL_WIDGET_DEFAULTS), "SETUP_WIDGET_ORDER must list every widget exactly once"


def setup_widget_label(logical_key):
    """Databricks displays widgets sorted alphabetically — numeric prefix fixes panel order."""
    n = SETUP_WIDGET_ORDER.index(logical_key) + 1
    return f"{n:02d}_{logical_key}"


def logical_widget_key(widget_label):
    """Strip optional NN_ prefix from widget name -> logical key for package_settings."""
    if len(widget_label) >= 3 and widget_label[:2].isdigit() and widget_label[2] == "_":
        return widget_label[3:]
    return widget_label


def read_setup_widgets():
    """Read all setup widgets; returns dict keyed by logical name (unprefixed)."""
    out = {}
    for k in SETUP_WIDGET_ORDER:
        label = setup_widget_label(k)
        try:
            out[k] = dbutils.widgets.get(label).strip()
        except Exception:
            try:
                out[k] = dbutils.widgets.get(k).strip()  # legacy unprefixed widgets
            except Exception:
                out[k] = str(ALL_WIDGET_DEFAULTS[k])
    return out

# Consumer repair lifecycle (stored on config_consumers.repair_status)
REPAIR_DISCOVERED = "DISCOVERED"       # registered by discovery; not queued for repair
REPAIR_SELECTED = "SELECTED"           # user opted in for this wave
REPAIR_SKIPPED = "SKIPPED"             # user declined (still visible in registry)
REPAIR_VERIFIED = "VERIFIED"           # 05 validation green for this role
REPAIR_FIXED = "FIXED"                 # 06 sweep + post-check green
REPAIR_EXCLUDED = "EXCLUDED"           # excluded=true (false positive / out of scope)
REPAIR_NOT_APPLICABLE = "NOT_APPLICABLE"  # RELOADED — hash only, no SK sweep

REPAIR_STATUSES = frozenset({
    REPAIR_DISCOVERED, REPAIR_SELECTED, REPAIR_SKIPPED, REPAIR_VERIFIED,
    REPAIR_FIXED, REPAIR_EXCLUDED, REPAIR_NOT_APPLICABLE,
})
REPAIR_ACTIVE = frozenset({REPAIR_SELECTED, REPAIR_VERIFIED})  # populate / validate / sweep


def _widget_values():
    """Read widget values from getAll() — more reliable than get() on Serverless/Git notebooks."""
    out = {}
    try:
        for w in dbutils.widgets.getAll():
            out[w.name] = (w.value or "").strip()
    except Exception:
        pass
    return out


def _widget_names():
    return set(_widget_values().keys())


def get_or_create_widgets(defaults=None):
    """Create missing text widgets without resetting existing values."""
    d = dict(ALL_WIDGET_DEFAULTS)
    if defaults:
        d.update(defaults)
    existing = _widget_names()
    for k, v in d.items():
        if k not in existing:
            dbutils.widgets.text(k, str(v))
    return {k: dbutils.widgets.get(k).strip() for k in d}


def ensure_setup_widgets():
    """Create the 00_setup widget panel if missing; preserve values on re-run.

    Do NOT call removeAll() here — Run All must not wipe operator input before save.
    """
    existing = _widget_names()
    prefixed = {setup_widget_label(k) for k in SETUP_WIDGET_ORDER}
    # One-time upgrade: legacy unprefixed widget names → numbered panel
    if existing and not existing.intersection(prefixed):
        legacy = set(SETUP_WIDGET_ORDER) & existing
        if legacy:
            dbutils.widgets.removeAll()
            existing = set()
    for k in SETUP_WIDGET_ORDER:
        label = setup_widget_label(k)
        if label not in existing:
            dbutils.widgets.text(label, str(ALL_WIDGET_DEFAULTS[k]))
    return read_setup_widgets()


def reset_setup_widgets():
    """Wipe and recreate the widget panel with defaults (use after pulling widget renames)."""
    dbutils.widgets.removeAll()
    for k in SETUP_WIDGET_ORDER:
        dbutils.widgets.text(setup_widget_label(k), str(ALL_WIDGET_DEFAULTS[k]))
    return read_setup_widgets()


def init_setup_widgets():
    """Alias for ensure_setup_widgets (backward compatible)."""
    return ensure_setup_widgets()


def _read_bootstrap_key(key, widget_vals, defaults):
    """Resolve one bootstrap key from numbered widget, legacy name, or get()."""
    pref = setup_widget_label(key)
    for name in (pref, key):
        v = widget_vals.get(name, "").strip()
        if v:
            return v
        try:
            v = dbutils.widgets.get(name).strip()
            if v:
                return v
        except Exception:
            pass
    return defaults[key]


def ensure_bootstrap_widgets():
    """Create catalog/schema locator widgets in downstream notebooks (no removeAll).

    Widgets are per-notebook in Databricks. Notebooks 01–06 may show widgets 01 and 05;
    load_package_settings also auto-discovers package_settings when the catalog is unique.
    """
    existing = _widget_names()
    for k in ("target_catalog", "config_schema"):
        pref = setup_widget_label(k)
        if pref not in existing and k not in existing:
            dbutils.widgets.text(pref, str(ALL_WIDGET_DEFAULTS[k]))


def _bootstrap_settings():
    """Catalog/schema hint to locate package_settings (may be overridden by auto-discovery)."""
    ensure_bootstrap_widgets()
    widget_vals = _widget_values()
    d = dict(ALL_WIDGET_DEFAULTS)
    for k in ("target_catalog", "config_schema"):
        d[k] = _read_bootstrap_key(k, widget_vals, ALL_WIDGET_DEFAULTS)
    return d


def _discover_package_settings(config_schema):
    """Find all package_settings tables in the given config schema (Unity Catalog)."""
    sch = config_schema.replace("'", "''")
    return spark.sql(f"""
        SELECT table_catalog AS cat, table_schema AS sch
        FROM system.information_schema.tables
        WHERE lower(table_name) = 'package_settings'
          AND lower(table_schema) = lower('{sch}')
    """).collect()


def _load_active_package_settings(bootstrap):
    """Try bootstrap location, then auto-discover. Returns (fqn, config_json) or raises."""
    schema = bootstrap["config_schema"]
    cat = bootstrap["target_catalog"]

    def _try(catalog, sch):
        fqn = f"`{catalog}`.`{sch}`.package_settings"
        rows = spark.sql(
            f"SELECT config_json FROM {fqn} WHERE config_id = 'active'"
        ).collect()
        if rows:
            return fqn, rows[0].config_json
        raise LookupError("no active config_id row")

    # 1) Bootstrap catalog when not the placeholder default
    if cat and cat != ALL_WIDGET_DEFAULTS["target_catalog"]:
        try:
            return _try(cat, schema)
        except Exception:
            pass

    # 2) Auto-discover (fixes Serverless widget read issues and per-notebook defaults)
    discovered = _discover_package_settings(schema)
    if len(discovered) == 1:
        r = discovered[0]
        fqn, cfg = _try(r.cat, r.sch)
        print(f"Auto-located package_settings at {fqn} (widget 01_target_catalog was not used)")
        return fqn, cfg
    if len(discovered) > 1:
        cats = sorted({r.cat for r in discovered})
        raise Exception(
            f"Multiple package_settings tables in schema `{schema}`: {cats}. "
            f"Set widget 01_target_catalog to the correct catalog."
        )

    # 3) Last attempt with bootstrap even if placeholder (clear error path)
    try:
        return _try(cat, schema)
    except Exception as exc:
        raise exc


def package_settings_fqn(w):
    return fq(w["target_catalog"], w["config_schema"], "package_settings")


def ensure_package_settings_table(w):
    """Always executes — package_settings must exist even when dry_run=true."""
    fqn = package_settings_fqn(w)
    print(f"\n-- [EXEC] create package_settings\nCREATE TABLE IF NOT EXISTS {fqn} ...\n")
    spark.sql(f"""
CREATE TABLE IF NOT EXISTS {fqn} (
  config_id   STRING    NOT NULL,
  config_json STRING    NOT NULL,
  updated_at  TIMESTAMP NOT NULL
) USING DELTA""")


def save_package_settings(ctx, w):
    """Persist the full widget dict to Delta (single active row). Always executes."""
    ensure_package_settings_table(w)
    fqn = package_settings_fqn(w)
    schema = T.StructType([
        T.StructField("config_id", T.StringType()),
        T.StructField("config_json", T.StringType()),
        T.StructField("updated_at", T.TimestampType()),
    ])
    row = [("active", json.dumps(w, separators=(",", ":")), datetime.datetime.utcnow())]
    spark.createDataFrame(row, schema).createOrReplaceTempView("_pkg_save")
    print(f"\n-- [EXEC] save package_settings\nDELETE + INSERT INTO {fqn}\n")
    spark.sql(f"DELETE FROM {fqn} WHERE config_id = 'active'")
    spark.sql(f"INSERT INTO {fqn} SELECT config_id, config_json, updated_at FROM _pkg_save")


def verify_package_settings(w):
    """Fail fast if package_settings is missing or has no active row."""
    fqn = package_settings_fqn(w)
    tables = spark.sql(
        f"SHOW TABLES IN `{w['target_catalog']}`.`{w['config_schema']}` LIKE 'package_settings'"
    ).collect()
    if not tables:
        raise RuntimeError(
            f"{fqn} was not created. Re-run the save cell in 00_setup "
            f"(if 10_dry_run was true on an older repo version, sync latest code first)."
        )
    n = spark.sql(
        f"SELECT COUNT(*) AS n FROM {fqn} WHERE config_id = 'active'"
    ).collect()[0].n
    if n != 1:
        raise RuntimeError(f"Expected 1 active row in {fqn}, found {n}.")
    print(f"Verified: {fqn} (1 active config row)")
    return fqn


def load_package_settings(require_saved=False):
    """Load config from Delta; fall back to widgets/defaults (00_setup first run)."""
    bootstrap = _bootstrap_settings()
    cat = bootstrap["target_catalog"]
    schema = bootstrap["config_schema"]
    try:
        fqn, config_json = _load_active_package_settings(bootstrap)
        saved = json.loads(config_json)
        merged = {**bootstrap, **saved}
        if merged.get("target_catalog") != cat or merged.get("config_schema") != schema:
            print(
                f"Loaded config from {fqn} "
                f"(catalog={merged.get('target_catalog')}, schema={merged.get('config_schema')})"
            )
        return merged
    except Exception as exc:
        if require_saved:
            fqn = f"`{cat}`.`{schema}`.package_settings"
            had_rows = isinstance(exc, LookupError)
            raise Exception(
                _package_settings_not_found_msg(fqn, cat, had_rows=had_rows, cause=None if had_rows else exc)
            ) from exc
    return get_or_create_widgets(bootstrap)


def _package_settings_not_found_msg(fqn, cat, had_rows=False, cause=None):
    placeholder = cat == ALL_WIDGET_DEFAULTS["target_catalog"]
    lines = [
        f"Package settings not found at {fqn}.",
        "",
        "Checklist:",
        "  1. Run 00_setup after setting widget 01_target_catalog to your real Unity Catalog",
        "     (not the placeholder 'target_catalog').",
        "  2. Notebooks 01–06 auto-locate package_settings when only one exists in the",
        "     config schema (widget 01 is optional in that case). Otherwise set",
        "     01_target_catalog and 05_config_schema to match 00_setup.",
    ]
    if placeholder:
        lines.append(
            "  → This notebook is still using the default catalog name 'target_catalog'."
        )
    if had_rows:
        lines.append("  → Table exists but has no active row — re-run 00_setup.")
    lines.append(
        "  3. If 00_setup reported success but the table is missing, it may have run with"
    )
    lines.append(
        "     10_dry_run=true (older versions skipped the save). Re-run 00_setup with dry_run=false."
    )
    if cause:
        lines.append(f"\nUnderlying error: {cause}")
    return "\n".join(lines)


def parse_json_widget(w, key):
    raw = (w.get(key) or "").strip()
    if not raw:
        return []
    data = json.loads(raw)
    if not isinstance(data, list):
        raise ValueError(f"{key} must be a JSON array, got {type(data).__name__}")
    return data


def fq(catalog, schema, table):
    return f"`{catalog}`.`{schema}`.`{table}`"


class Ctx:
    """Carries widget values; resolves table names; executes SQL honoring dry_run."""

    def __init__(self, w):
        self.w = w
        self.dry = w["dry_run"].lower() == "true"

    def tgt(self, table):   return fq(self.w["target_catalog"], self.w["target_schema"], table)
    def src(self, table):   return fq(self.w["source_catalog"], self.w["source_schema"], table)
    def stg(self, table):   return fq(self.w["target_catalog"], self.w["staging_schema"], f"legacy_{table}")
    def km(self, table):    return fq(self.w["target_catalog"], self.w["keymap_schema"], f"{table}_keymap")
    def cfg(self, table):   return fq(self.w["target_catalog"], self.w["config_schema"], table)

    def exec_mut(self, sql, label=""):
        print(f"\n-- [{'DRY-RUN' if self.dry else 'EXEC'}] {label}\n{sql.strip()}\n")
        if not self.dry:
            return spark.sql(sql)
        return None

    def query(self, sql, label=""):
        if label:
            print(f"-- [QUERY] {label}")
        return spark.sql(sql)


# COMMAND ----------

# MAGIC %md ## The single NK normalization expression (§1.1, §1.2)

# COMMAND ----------

def _norm_component(col_sql, fmt):
    if fmt == "date":
        inner = f"date_format({col_sql}, 'yyyy-MM-dd')"
    elif fmt == "timestamp":
        inner = f"date_format({col_sql}, 'yyyy-MM-dd HH:mm:ss')"
    elif fmt in ("bigint", "int"):
        inner = f"cast(cast({col_sql} as bigint) as string)"
    elif fmt and fmt.lower().startswith("decimal"):
        inner = f"cast(cast({col_sql} as {fmt}) as string)"
    else:
        inner = f"cast({col_sql} as string)"
    return f"coalesce(upper(trim({inner})), '{NULL_SENTINEL}')"


def _q(alias, col):
    return (f"{alias}." if alias else "") + f"`{col}`"


def nk_string_expr(nk_cols, overrides=None, alias=""):
    overrides = overrides or {}
    parts = [_norm_component(_q(alias, c), overrides.get(c)) for c in nk_cols]
    return " || '||' || ".join(parts)


def nk_hash_expr(nk_cols, overrides=None, alias=""):
    return f"xxhash64({nk_string_expr(nk_cols, overrides, alias)})"


def ver_hash_expr(nk_cols, start_col, overrides=None, alias=""):
    return (f"xxhash64({nk_string_expr(nk_cols, overrides, alias)}"
            f" || '||' || date_format({_q(alias, start_col)}, 'yyyy-MM-dd HH:mm:ss'))")


DELETED_STATUS_VALUES = ("D", "DEL", "DELETED", "LOGICALLY_DELETED")

def status_class_expr(status_col, alias=""):
    vals = ", ".join(f"'{v}'" for v in DELETED_STATUS_VALUES)
    return (f"CASE WHEN upper(trim(cast({_q(alias, status_col)} as string))) IN ({vals}) "
            f"THEN 'DELETED' ELSE 'ACTIVE' END")


END_OF_TIME = "timestamp'9999-12-31'"

def window_end_expr(end_col, alias=""):
    return f"coalesce({_q(alias, end_col)}, {END_OF_TIME})"


# COMMAND ----------

# MAGIC %md ## Config registration (driven by 00_setup JSON widgets)

# COMMAND ----------

def upsert_provider(ctx, provider_table, archetype, sk_col, nk_cols,
                    nk_type_overrides=None, effective_start_col=None,
                    effective_end_col=None, record_status_col=None,
                    use_status_tiebreaker=False, version_match_path=None,
                    topo_level=0, enabled=True, notes=None):
    assert archetype in ("SCD1", "SCD2", "HUB_SCD2"), archetype
    if archetype in ("SCD2", "HUB_SCD2"):
        assert effective_start_col, f"{provider_table}: SCD2/HUB requires effective_start_col"
    schema = T.StructType([
        T.StructField("provider_table", T.StringType()),
        T.StructField("archetype", T.StringType()),
        T.StructField("sk_col", T.StringType()),
        T.StructField("nk_cols", T.ArrayType(T.StringType())),
        T.StructField("nk_type_overrides", T.MapType(T.StringType(), T.StringType())),
        T.StructField("effective_start_col", T.StringType()),
        T.StructField("effective_end_col", T.StringType()),
        T.StructField("record_status_col", T.StringType()),
        T.StructField("use_status_tiebreaker", T.BooleanType()),
        T.StructField("version_match_path", T.StringType()),
        T.StructField("topo_level", T.IntegerType()),
        T.StructField("enabled", T.BooleanType()),
        T.StructField("notes", T.StringType()),
        T.StructField("updated_at", T.TimestampType()),
    ])
    row = [(provider_table, archetype, sk_col, list(nk_cols),
            nk_type_overrides or {}, effective_start_col, effective_end_col,
            record_status_col, use_status_tiebreaker, version_match_path,
            topo_level, enabled, notes, datetime.datetime.utcnow())]
    spark.createDataFrame(row, schema).createOrReplaceTempView("_prov_upsert")
    spark.sql(f"""
      MERGE INTO {ctx.cfg('config_providers')} t
      USING _prov_upsert s ON lower(t.provider_table) = lower(s.provider_table)
      WHEN MATCHED THEN UPDATE SET *
      WHEN NOT MATCHED THEN INSERT *""")
    print(f"provider upserted: {provider_table} ({archetype}, sk={sk_col}, nk={nk_cols})")


def add_consumer(ctx, consumer_table, fk_col, provider_table, event_date_col=None,
                 measure_cols=None, notes="manual", repair_status=None):
    if repair_status is None:
        repair_status = default_repair_status_on_discover(ctx.w)
    schema = T.StructType([
        T.StructField("consumer_table", T.StringType()),
        T.StructField("fk_col", T.StringType()),
        T.StructField("provider_table", T.StringType()),
        T.StructField("event_date_col", T.StringType()),
        T.StructField("measure_cols", T.ArrayType(T.StringType())),
        T.StructField("notes", T.StringType()),
        T.StructField("repair_status", T.StringType()),
    ])
    row = [(consumer_table, fk_col, provider_table, event_date_col,
            measure_cols, notes, repair_status)]
    spark.createDataFrame(row, schema).createOrReplaceTempView("_cons_add")
    spark.sql(f"""
      MERGE INTO {ctx.cfg('config_consumers')} t
      USING _cons_add s
        ON lower(t.consumer_table)=lower(s.consumer_table)
       AND lower(t.fk_col)=lower(s.fk_col)
      WHEN NOT MATCHED THEN INSERT
        (consumer_table, fk_col, provider_table, event_date_col, measure_cols,
         classification, discovered_by, excluded, exclusion_reason, notes,
         repair_status, selected_at, fixed_at, last_validation_run_id, updated_at)
      VALUES (s.consumer_table, s.fk_col, s.provider_table, s.event_date_col,
              s.measure_cols, NULL, 'MANUAL', false, NULL, s.notes,
              s.repair_status,
              CASE WHEN s.repair_status = '{REPAIR_SELECTED}' THEN current_timestamp() ELSE NULL END,
              NULL, NULL, current_timestamp())""")
    print(f"consumer added: {consumer_table}.{fk_col} -> {provider_table} ({repair_status})")


def exclude_consumer(ctx, consumer_table, fk_col, reason):
    spark.sql(f"""
      UPDATE {ctx.cfg('config_consumers')}
      SET excluded = true, exclusion_reason = '{reason.replace("'", "''")}',
          repair_status = '{REPAIR_EXCLUDED}',
          updated_at = current_timestamp()
      WHERE lower(consumer_table)=lower('{consumer_table}')
        AND lower(fk_col)=lower('{fk_col}')""")
    print(f"consumer excluded: {consumer_table}.{fk_col} ({reason})")


def default_repair_status_on_discover(w):
    mode = (w.get("repair_mode") or "opt_in").lower()
    return REPAIR_SELECTED if mode == "opt_out" else REPAIR_DISCOVERED


def ensure_consumer_repair_columns(ctx):
    """Add repair lifecycle columns to config_consumers (idempotent)."""
    fq = ctx.cfg("config_consumers")
    ensure_columns(ctx, fq, {
        "repair_status": "STRING",
        "selected_at": "TIMESTAMP",
        "fixed_at": "TIMESTAMP",
        "last_validation_run_id": "STRING",
    })
    # Backfill legacy rows discovered before this feature
    spark.sql(f"""
      UPDATE {fq}
      SET repair_status = '{REPAIR_DISCOVERED}', updated_at = current_timestamp()
      WHERE repair_status IS NULL AND NOT excluded""")
    spark.sql(f"""
      UPDATE {fq}
      SET repair_status = '{REPAIR_EXCLUDED}', updated_at = current_timestamp()
      WHERE repair_status IS NULL AND excluded""")


def set_repair_status(ctx, consumer_table, fk_col, repair_status,
                      validation_run_id=None, set_selected_ts=False, set_fixed_ts=False):
    assert repair_status in REPAIR_STATUSES, repair_status
    sets = [f"repair_status = '{repair_status}'", "updated_at = current_timestamp()"]
    if set_selected_ts or repair_status == REPAIR_SELECTED:
        sets.append("selected_at = current_timestamp()")
    if set_fixed_ts or repair_status == REPAIR_FIXED:
        sets.append("fixed_at = current_timestamp()")
    if validation_run_id:
        sets.append(f"last_validation_run_id = '{validation_run_id}'")
    spark.sql(f"""
      UPDATE {ctx.cfg('config_consumers')}
      SET {', '.join(sets)}
      WHERE lower(consumer_table)=lower('{consumer_table}')
        AND lower(fk_col)=lower('{fk_col}')""")
    print(f"repair_status {consumer_table}.{fk_col} -> {repair_status}")


def set_classification(ctx, consumer_table, fk_col, classification, note="attested"):
    assert classification in ("LEGACY_KEYED", "RELOADED", "MIXED")
    note_sql = note.replace("'", "''")
    spark.sql(f"""
      UPDATE {ctx.cfg('config_consumers')}
      SET classification = '{classification}',
          notes = concat_ws(' | ', notes, '{note_sql}'),
          updated_at = current_timestamp()
      WHERE lower(consumer_table)=lower('{consumer_table}')
        AND lower(fk_col)=lower('{fk_col}')""")
    print(f"classified {consumer_table}.{fk_col} = {classification}")


def _apply_providers_from_json(ctx, w):
    for p in parse_json_widget(w, "providers_json"):
        upsert_provider(
            ctx,
            p["provider_table"],
            p["archetype"],
            p["sk_col"],
            p["nk_cols"],
            nk_type_overrides=p.get("nk_type_overrides"),
            effective_start_col=p.get("effective_start_col"),
            effective_end_col=p.get("effective_end_col"),
            record_status_col=p.get("record_status_col"),
            use_status_tiebreaker=bool(p.get("use_status_tiebreaker", False)),
            version_match_path=p.get("version_match_path"),
            topo_level=int(p.get("topo_level", 0)),
            enabled=bool(p.get("enabled", True)),
            notes=p.get("notes"),
        )


def _apply_manual_consumers_from_json(ctx, w):
    for c in parse_json_widget(w, "manual_consumers_json"):
        add_consumer(
            ctx,
            c["consumer_table"],
            c["fk_col"],
            c["provider_table"],
            event_date_col=c.get("event_date_col"),
            measure_cols=c.get("measure_cols"),
            notes=c.get("notes", "manual"),
        )


def _apply_exclusions_from_json(ctx, w):
    for x in parse_json_widget(w, "exclude_consumers_json"):
        exclude_consumer(ctx, x["consumer_table"], x["fk_col"], x["reason"])


def _apply_consumer_overrides_from_json(ctx, w):
    for o in parse_json_widget(w, "consumer_overrides_json"):
        tbl, fk = o["consumer_table"], o["fk_col"]
        sets = ["updated_at = current_timestamp()"]
        if o.get("event_date_col"):
            sets.append(f"event_date_col = '{o['event_date_col']}'")
        if o.get("measure_cols") is not None:
            arr = ", ".join(f"'{m}'" for m in o["measure_cols"])
            sets.append(f"measure_cols = array({arr})")
        if o.get("excluded") is True:
            sets.append("excluded = true")
            if o.get("exclusion_reason"):
                sets.append(f"exclusion_reason = '{o['exclusion_reason']}'")
        if len(sets) == 1:
            continue
        spark.sql(f"""
          UPDATE {ctx.cfg('config_consumers')}
          SET {', '.join(sets)}
          WHERE lower(consumer_table)=lower('{tbl}') AND lower(fk_col)=lower('{fk}')""")
        print(f"consumer override: {tbl}.{fk}")


def _apply_classifications_from_json(ctx, w):
    for c in parse_json_widget(w, "classifications_json"):
        set_classification(
            ctx,
            c["consumer_table"],
            c["fk_col"],
            c["classification"],
            c.get("note", "attested via 00_setup"),
        )


def _apply_repair_selection_from_json(ctx, w):
    for r in parse_json_widget(w, "repair_selection_json"):
        status = r["repair_status"].upper()
        assert status in REPAIR_STATUSES, status
        set_repair_status(
            ctx,
            r["consumer_table"],
            r["fk_col"],
            status,
            set_selected_ts=(status == REPAIR_SELECTED),
            set_fixed_ts=(status == REPAIR_FIXED),
        )


def promote_consumers_verified(ctx, consumers, results, run_id):
    """After a green 05 run, mark each in-scope consumer×role VERIFIED."""
    fails = {(r[1].lower(), r[2].lower()) for r in results if r[6] == "FAIL"}
    for c in consumers:
        key = (c["consumer_table"].lower(), c["fk_col"].lower())
        if key in fails:
            continue
        cls = (c.get("classification") or "").upper()
        if cls == "RELOADED":
            set_repair_status(ctx, c["consumer_table"], c["fk_col"], REPAIR_FIXED,
                              validation_run_id=run_id, set_fixed_ts=True)
            continue
        set_repair_status(ctx, c["consumer_table"], c["fk_col"], REPAIR_VERIFIED,
                          validation_run_id=run_id)


def promote_consumers_fixed(ctx, consumer_table, fk_col, run_id):
    set_repair_status(ctx, consumer_table, fk_col, REPAIR_FIXED,
                      validation_run_id=run_id, set_fixed_ts=True)


def apply_setup_config(ctx, w, sections=None):
    """Apply JSON widget config to Delta config tables. sections=None means all."""
    all_sections = (
        "providers", "manual_consumers", "exclusions",
        "consumer_overrides", "classifications", "repair_selection",
    )
    run = all_sections if sections is None else sections
    if "providers" in run:
        _apply_providers_from_json(ctx, w)
    if "manual_consumers" in run:
        _apply_manual_consumers_from_json(ctx, w)
    if "exclusions" in run:
        _apply_exclusions_from_json(ctx, w)
    if "consumer_overrides" in run:
        _apply_consumer_overrides_from_json(ctx, w)
    if "classifications" in run:
        _apply_classifications_from_json(ctx, w)
    if "repair_selection" in run:
        _apply_repair_selection_from_json(ctx, w)


# COMMAND ----------

# MAGIC %md ## Config access & misc utilities

# COMMAND ----------

def _in_scope(name, filt):
    if filt in ("*", ""):
        return True
    return name.lower() in {x.strip().lower() for x in filt.split(",")}


def load_providers(ctx, only_enabled=True):
    rows = spark.table(ctx.cfg("config_providers").replace("`", "")).collect()
    out = []
    for r in rows:
        d = r.asDict()
        if only_enabled and not d["enabled"]:
            continue
        if not _in_scope(d["provider_table"], ctx.w["provider_filter"]):
            continue
        d["nk_type_overrides"] = dict(d["nk_type_overrides"] or {})
        d["nk_cols"] = list(d["nk_cols"] or [])
        out.append(d)
    return sorted(out, key=lambda d: (d["topo_level"] or 0, d["provider_table"]))


def _consumer_repair_status(d):
    if d.get("excluded"):
        return REPAIR_EXCLUDED
    return (d.get("repair_status") or REPAIR_DISCOVERED).upper()


def _passes_repair_phase(status, w):
    """Whether a consumer row is eligible for populate / validate / sweep."""
    mode = (w.get("repair_mode") or "opt_in").lower()
    if mode == "opt_out":
        return status not in (REPAIR_SKIPPED, REPAIR_EXCLUDED, REPAIR_FIXED)
    return status in REPAIR_ACTIVE


def load_consumers(ctx, include_excluded=False, repair_phase="repair"):
    """Load consumer config rows.

    repair_phase:
      'repair'   — rows eligible for 04–06 (respects repair_mode / repair_status)
      'registry' — all non-excluded rows (triage / SQL review)
    """
    rows = spark.table(ctx.cfg("config_consumers").replace("`", "")).collect()
    out = []
    for r in rows:
        d = r.asDict()
        if d["excluded"] and not include_excluded:
            continue
        if not _in_scope(d["consumer_table"], ctx.w["consumer_filter"]):
            continue
        status = _consumer_repair_status(d)
        d["repair_status"] = status
        if repair_phase == "repair" and not _passes_repair_phase(status, ctx.w):
            continue
        out.append(d)
    return sorted(out, key=lambda d: (d["consumer_table"], d["fk_col"]))


def providers_by_name(ctx):
    return {p["provider_table"].lower(): p for p in load_providers(ctx, only_enabled=False)}


CHOICE_SEP = "::"

def consumer_choice_key(consumer_table, fk_col):
    """Stable key for multiselect widgets (table::fk_col)."""
    return f"{consumer_table}{CHOICE_SEP}{fk_col}"


def parse_consumer_choice_key(key):
    tbl, fk = key.split(CHOICE_SEP, 1)
    return tbl, fk


def consumer_choice_label(d):
    return (f"{d['consumer_table']}.{d['fk_col']} -> {d['provider_table']} "
            f"[{d['repair_status']}]")


def load_triage_candidates(ctx):
    """Consumers eligible for multiselect triage (not FIXED / EXCLUDED)."""
    skip = {REPAIR_FIXED, REPAIR_EXCLUDED}
    return [d for d in load_consumers(ctx, repair_phase="registry")
            if d["repair_status"] not in skip]


def apply_triage_selection(ctx, selected_keys, mark_others_skipped=False):
    """Mark picked rows SELECTED; optionally mark other DISCOVERED rows SKIPPED."""
    selected = {k.strip() for k in selected_keys if k.strip()}
    candidates = load_triage_candidates(ctx)
    cand_keys = {consumer_choice_key(d["consumer_table"], d["fk_col"]) for d in candidates}
    unknown = selected - cand_keys
    if unknown:
        raise ValueError(f"Unknown triage keys (re-run notebook to refresh list): {sorted(unknown)}")
    n_sel, n_skip = 0, 0
    for d in candidates:
        key = consumer_choice_key(d["consumer_table"], d["fk_col"])
        tbl, fk = d["consumer_table"], d["fk_col"]
        if key in selected:
            if d["repair_status"] != REPAIR_SELECTED:
                set_repair_status(ctx, tbl, fk, REPAIR_SELECTED, set_selected_ts=True)
            n_sel += 1
        elif mark_others_skipped and d["repair_status"] == REPAIR_DISCOVERED:
            set_repair_status(ctx, tbl, fk, REPAIR_SKIPPED)
            n_skip += 1
    print(f"triage applied: {n_sel} SELECTED, {n_skip} others -> SKIPPED")
    return n_sel, n_skip


def table_columns(fqname):
    return [f.name for f in spark.table(fqname.replace("`", "")).schema.fields]


def ensure_columns(ctx, fqname, cols):
    existing = {c.lower() for c in table_columns(fqname)}
    missing = {c: t for c, t in cols.items() if c.lower() not in existing}
    if missing:
        clause = ", ".join(f"`{c}` {t}" for c, t in missing.items())
        ctx.exec_mut(f"ALTER TABLE {fqname} ADD COLUMNS ({clause})", f"add columns to {fqname}")
    return list(missing)


def record_rows(ctx, result_table, rows, schema):
    if not rows:
        return
    df = spark.createDataFrame(rows, schema=schema)
    df.write.mode("append").saveAsTable(ctx.cfg(result_table).replace("`", ""))


def scalar(df):
    r = df.collect()
    return r[0][0] if r else None


def last_merge_metrics(fqname):
    h = spark.sql(f"DESCRIBE HISTORY {fqname} LIMIT 1").collect()
    return dict(h[0]["operationMetrics"]) if h else {}


def role_hash_cols(fk_col):
    return f"{fk_col}_nk_hash", f"{fk_col}_ver_hash"


print(f"_common loaded. RUN_ID = {RUN_ID}")
