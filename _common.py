# Databricks notebook source
# MAGIC %md
# MAGIC # _common — shared helpers for the hash-spine RI repair package
# MAGIC
# MAGIC Included from every notebook via `%run ./_common`. Defines:
# MAGIC - base widgets (catalogs/schemas, dry-run, scope filters)
# MAGIC - the **single** NK normalization / hash expression (§1.1–1.2 of the plan)
# MAGIC - config-table accessors and small SQL utilities
# MAGIC
# MAGIC **Never** hand-retype the NK expression anywhere else — every notebook builds it
# MAGIC from `nk_hash_expr()` so legacy, target, key-map and consumers stay byte-identical.

# COMMAND ----------

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


def create_widgets(extra=None):
    """Create base + notebook-specific widgets; return dict of current values."""
    d = dict(BASE_WIDGET_DEFAULTS)
    if extra:
        d.update(extra)
    for k, v in d.items():
        dbutils.widgets.text(k, v)
    return {k: dbutils.widgets.get(k).strip() for k in d}


def fq(catalog, schema, table):
    return f"`{catalog}`.`{schema}`.`{table}`"


class Ctx:
    """Carries widget values; resolves table names; executes SQL honoring dry_run."""

    def __init__(self, w):
        self.w = w
        self.dry = w["dry_run"].lower() == "true"

    # ---- name resolution (source/target names are mirrored by assumption) ----
    def tgt(self, table):   return fq(self.w["target_catalog"], self.w["target_schema"], table)
    def src(self, table):   return fq(self.w["source_catalog"], self.w["source_schema"], table)
    def stg(self, table):   return fq(self.w["target_catalog"], self.w["staging_schema"], f"legacy_{table}")
    def km(self, table):    return fq(self.w["target_catalog"], self.w["keymap_schema"], f"{table}_keymap")
    def cfg(self, table):   return fq(self.w["target_catalog"], self.w["config_schema"], table)

    # ---- execution ----
    def exec_mut(self, sql, label=""):
        """Mutating statement: respects dry_run."""
        print(f"\n-- [{'DRY-RUN' if self.dry else 'EXEC'}] {label}\n{sql.strip()}\n")
        if not self.dry:
            return spark.sql(sql)
        return None

    def query(self, sql, label=""):
        """Read-only statement: always executes."""
        if label:
            print(f"-- [QUERY] {label}")
        return spark.sql(sql)


# COMMAND ----------

# MAGIC %md ## The single NK normalization expression (§1.1, §1.2)

# COMMAND ----------

def _norm_component(col_sql, fmt):
    """Normalize one NK component to a canonical string before hashing.
    fmt overrides (from config_providers.nk_type_overrides):
      'date'      -> date_format(col,'yyyy-MM-dd')
      'timestamp' -> date_format(col,'yyyy-MM-dd HH:mm:ss')
      'bigint'    -> cast(cast(col as bigint) as string)   (decimals stored as ints)
      'decimal(p,s)' -> cast(cast(col as decimal(p,s)) as string)
      None        -> cast(col as string)
    """
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
    # upper(trim()) reconciles SQL Server CI collation / trailing-space semantics with Spark.
    return f"coalesce(upper(trim({inner})), '{NULL_SENTINEL}')"


def _q(alias, col):
    return (f"{alias}." if alias else "") + f"`{col}`"


def nk_string_expr(nk_cols, overrides=None, alias=""):
    """Normalized (possibly composite) natural-key string. Components joined with '||'."""
    overrides = overrides or {}
    parts = [_norm_component(_q(alias, c), overrides.get(c)) for c in nk_cols]
    return " || '||' || ".join(parts)


def nk_hash_expr(nk_cols, overrides=None, alias=""):
    """Member-level durable key: xxhash64 over the normalized NK string."""
    return f"xxhash64({nk_string_expr(nk_cols, overrides, alias)})"


def ver_hash_expr(nk_cols, start_col, overrides=None, alias=""):
    """Path A version-level key: member NK + effectiveStartDate (§2.3)."""
    return (f"xxhash64({nk_string_expr(nk_cols, overrides, alias)}"
            f" || '||' || date_format({_q(alias, start_col)}, 'yyyy-MM-dd HH:mm:ss'))")


DELETED_STATUS_VALUES = ("D", "DEL", "DELETED", "LOGICALLY_DELETED")

def status_class_expr(status_col, alias=""):
    """Categorical (deleted vs not) recordStatus class — the ONLY way recordStatus may
    participate in matching (§2.3 caveat). Never match on current/expired."""
    vals = ", ".join(f"'{v}'" for v in DELETED_STATUS_VALUES)
    return (f"CASE WHEN upper(trim(cast({_q(alias, status_col)} as string))) IN ({vals}) "
            f"THEN 'DELETED' ELSE 'ACTIVE' END")


END_OF_TIME = "timestamp'9999-12-31'"

def window_end_expr(end_col, alias=""):
    return f"coalesce({_q(alias, end_col)}, {END_OF_TIME})"


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


def load_consumers(ctx, include_excluded=False):
    rows = spark.table(ctx.cfg("config_consumers").replace("`", "")).collect()
    out = []
    for r in rows:
        d = r.asDict()
        if d["excluded"] and not include_excluded:
            continue
        if not _in_scope(d["consumer_table"], ctx.w["consumer_filter"]):
            continue
        out.append(d)
    return sorted(out, key=lambda d: (d["consumer_table"], d["fk_col"]))


def providers_by_name(ctx):
    return {p["provider_table"].lower(): p for p in load_providers(ctx, only_enabled=False)}


def table_columns(fqname):
    return [f.name for f in spark.table(fqname.replace("`", "")).schema.fields]


def ensure_columns(ctx, fqname, cols):
    """cols: {col_name: sql_type}. Adds only the missing ones (metadata-only op)."""
    existing = {c.lower() for c in table_columns(fqname)}
    missing = {c: t for c, t in cols.items() if c.lower() not in existing}
    if missing:
        clause = ", ".join(f"`{c}` {t}" for c, t in missing.items())
        ctx.exec_mut(f"ALTER TABLE {fqname} ADD COLUMNS ({clause})", f"add columns to {fqname}")
    return list(missing)


def record_rows(ctx, result_table, rows, schema):
    """Append audit/result rows. Always executes (results are evidence, even in dry runs)."""
    if not rows:
        return
    df = spark.createDataFrame(rows, schema=schema)
    df.write.mode("append").saveAsTable(ctx.cfg(result_table).replace("`", ""))


def scalar(df):
    r = df.collect()
    return r[0][0] if r else None


def last_merge_metrics(fqname):
    """Pull operationMetrics of the most recent operation on a Delta table."""
    h = spark.sql(f"DESCRIBE HISTORY {fqname} LIMIT 1").collect()
    return dict(h[0]["operationMetrics"]) if h else {}


def role_hash_cols(fk_col):
    """Consumer-side hash column names for one FK role."""
    return f"{fk_col}_nk_hash", f"{fk_col}_ver_hash"


print(f"_common loaded. RUN_ID = {RUN_ID}")
