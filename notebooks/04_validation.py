# Databricks notebook source
# MAGIC %md
# MAGIC # 04_validation — Four-layer evidence for every migrated object
# MAGIC
# MAGIC **Purpose:** For every object that migrated, prove via four independent
# MAGIC evidence layers that queries genuinely read from new storage.
# MAGIC
# MAGIC **Inputs:** `<OPS_SCHEMA>.migration_log`, `<OPS_SCHEMA>.inventory`.
# MAGIC
# MAGIC **Outputs:** `<OPS_SCHEMA>.validation_results` — one row per object with
# MAGIC all evidence flags and raw evidence JSON.
# MAGIC
# MAGIC **Side effects:** Read-only against UC objects; writes only to
# MAGIC `<OPS_SCHEMA>.validation_results`.

# COMMAND ----------
# MAGIC %md
# MAGIC ## Path setup

# COMMAND ----------
import os
import sys


def _notebook_path() -> str | None:
    """Return the absolute workspace path of this notebook, or None.

    On Databricks serverless, sys.path[0] is the worker's tmp dir, not the
    notebook's workspace path — so we have to ask Databricks directly.
    """
    try:
        ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()  # noqa: F821
        p = ctx.notebookPath().get()
        return f"/Workspace{p}" if p and not p.startswith("/Workspace") else p
    except Exception:
        return None


def _add_utils_to_path() -> bool:
    """Walk up looking for sibling utils/. Returns True if found.

    Tries (in order):
      1. The notebook's own workspace path via dbutils.notebook context.
      2. sys.path[0] (works when running standalone / via %run).
    Walks up either candidate looking for a sibling utils/ directory.
    Silent on success.
    """
    starts: list[str] = []
    nb = _notebook_path()
    if nb:
        starts.append(nb)
    if sys.path:
        starts.append(sys.path[0])
    starts.append(os.getcwd())

    for start in starts:
        candidate = start
        for _ in range(6):
            parent = os.path.dirname(candidate)
            if parent == candidate:
                break
            if os.path.isdir(os.path.join(parent, "utils")):
                if parent not in sys.path:
                    sys.path.insert(0, parent)
                return True
            candidate = parent
    return False


_found = _add_utils_to_path()

# Once utils is importable, an explicit REPO_ROOT_HINT in utils/config.py
# overrides — for the rare layout where utils/ is not a sibling of
# notebooks/. Customers set it ONCE in utils/config.py; every notebook
# picks it up here.
if _found:
    try:
        from utils.config import REPO_ROOT_HINT as _hint
        if _hint and _hint not in sys.path:
            sys.path.insert(0, _hint)
    except ImportError:
        pass
else:
    raise RuntimeError(
        "Could not auto-locate utils/ relative to this notebook. "
        "If utils/ isn't a sibling of notebooks/ in your workspace, edit "
        "this cell to add `sys.path.insert(0, '/Workspace/path/to/repo')` "
        "above this block (then set REPO_ROOT_HINT in utils/config.py so "
        "subsequent cells don't need the manual hack)."
    )

# COMMAND ----------
# MAGIC %md
# MAGIC ## Config

# COMMAND ----------
# All values come from utils/config.py — edit there, not here.
import importlib
from utils import config as _cfg
importlib.reload(_cfg)  # pick up edits to utils/config.py without restarting Python
_cfg.resolve_config(spark=spark)
NEW_STORAGE_ACCOUNT = _cfg.NEW_STORAGE_ACCOUNT
OPS_SCHEMA = _cfg.OPS_SCHEMA
SAMPLE_LIMIT = _cfg.SAMPLE_LIMIT
VALIDATE_CONTENT_CHECKSUM = _cfg.VALIDATE_CONTENT_CHECKSUM
VALIDATE_VOLUME_CONTENT_CHECKSUM = _cfg.VALIDATE_VOLUME_CONTENT_CHECKSUM
VOLUME_CHECKSUM_SAMPLE_FRACTION = _cfg.VOLUME_CHECKSUM_SAMPLE_FRACTION
VOLUME_CHECKSUM_MAX_INMEMORY_BYTES = _cfg.VOLUME_CHECKSUM_MAX_INMEMORY_BYTES

# COMMAND ----------
# MAGIC %md
# MAGIC ## Run validation

# COMMAND ----------
from datetime import datetime, timezone

from utils.validation import validate_object_on_new, evidence_to_json
from utils.state import VALIDATION_RESULTS_SCHEMA, ValidationResultsWriter
from utils.migration import derive_pre_migration_fqn, select_checksum_sample
from utils.sql import quote_fqn


def _walk_volume(base):
    """Recursive (relpath, size) listing under a /Volumes/ path (see 03b)."""
    base_norm = base.rstrip("/")
    out, stack = [], [base_norm]
    while stack:
        p = stack.pop()
        for e in dbutils.fs.ls(p):  # noqa: F821
            ep = e.path
            if ep.rstrip("/").endswith(base_norm):
                continue
            if e.size == 0 and ep.endswith("/"):
                stack.append(ep)
            else:
                idx = ep.find(base_norm)
                rel = ep[idx + len(base_norm):].lstrip("/") if idx >= 0 else ep
                out.append((rel, e.size))
    return out


def _hash_big_file(path, chunk=8 * 1024 * 1024):
    """Stream-hash a single file via the FUSE /Volumes path (avoids the
    binaryFile ~2 GB content-column cap). Returns a hex md5 string."""
    import hashlib
    h = hashlib.md5()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(chunk), b""):
            h.update(block)
    return h.hexdigest()


def _hash_volume_files(vol_path, sample_fraction, max_inmem):
    """Produce [(relpath, md5_hex)] for (a sample of) a volume's files.

    Sampling is deterministic by relpath (select_checksum_sample), so the
    source shadow and the migrated volume independently pick the SAME files.
    Small files are hashed in bulk via Spark binaryFile (distributed); files
    larger than `max_inmem` are streamed-hashed on the driver."""
    from pyspark.sql.functions import md5 as _md5, col
    base = vol_path.rstrip("/")
    sampled = select_checksum_sample(_walk_volume(vol_path), sample_fraction)
    small = [(rel, sz) for rel, sz in sampled if sz <= max_inmem]
    big = [(rel, sz) for rel, sz in sampled if sz > max_inmem]
    out = []
    if small:
        paths = [f"{base}/{rel}" for rel, _ in small]
        df = spark.read.format("binaryFile").load(paths)
        for row in df.select("path", _md5(col("content")).alias("h")).collect():
            ep = row["path"]
            idx = ep.find(base)
            rel = ep[idx + len(base):].lstrip("/") if idx >= 0 else ep
            out.append((rel, row["h"]))
    for rel, _ in big:
        out.append((rel, _hash_big_file(f"{base}/{rel}")))
    return out


validated_rows = spark.sql(
    f"SELECT m.catalog, m.schema, m.name, m.object_type, i.data_source_format, "
    f"       i.parent_managed_location, i.table_type "
    f"FROM {OPS_SCHEMA}.migration_log m "
    f"JOIN {OPS_SCHEMA}.inventory i "
    f"  ON m.catalog = i.catalog AND m.schema = i.schema AND m.name = i.name "
    f"WHERE m.status = 'validated'"
).collect()

print(f"Validating {len(validated_rows)} migrated objects...")

writer = ValidationResultsWriter(spark=spark, table_name=f"{OPS_SCHEMA}.validation_results")
writer.ensure_exists()

fs = dbutils.fs  # noqa: F821
results_rows = []
for r in validated_rows:
    is_external = (r["table_type"] == "EXTERNAL")
    is_vol = (r["object_type"] == "VOLUME")
    # Content checksum compares the migrated table to its retained
    # `__pre_migration` shadow — only managed objects have one.
    compare_fqn = None
    if VALIDATE_CONTENT_CHECKSUM and not is_external:
        pc, ps, pn = derive_pre_migration_fqn(r["catalog"], r["schema"], r["name"])
        compare_fqn = quote_fqn(pc, ps, pn)
    # Layer 6: per-file content hash for managed volumes. Read 100% of bytes of
    # both copies (the live volume + its __pre_migration shadow), so it's gated.
    vol_source_hashes = vol_target_hashes = None
    if VALIDATE_VOLUME_CONTENT_CHECKSUM and is_vol and not is_external:
        pc, ps, pn = derive_pre_migration_fqn(r["catalog"], r["schema"], r["name"])
        live_path = f"/Volumes/{r['catalog']}/{r['schema']}/{r['name']}"
        shadow_path = f"/Volumes/{pc}/{ps}/{pn}"
        try:
            vol_source_hashes = _hash_volume_files(
                shadow_path, VOLUME_CHECKSUM_SAMPLE_FRACTION, VOLUME_CHECKSUM_MAX_INMEMORY_BYTES)
            vol_target_hashes = _hash_volume_files(
                live_path, VOLUME_CHECKSUM_SAMPLE_FRACTION, VOLUME_CHECKSUM_MAX_INMEMORY_BYTES)
        except Exception as e:
            # Leave hashes None → Layer 6 records "no listings"; surface the cause.
            print(f"  (volume hash failed for {r['catalog']}.{r['schema']}.{r['name']}: {e})")
    result = validate_object_on_new(
        spark=spark, fs=fs,
        catalog=r["catalog"], schema=r["schema"], name=r["name"],
        expected_new_account=NEW_STORAGE_ACCOUNT,
        parent_managed_location=r["parent_managed_location"],
        is_delta=(r["data_source_format"] == "DELTA"),
        sample_limit=SAMPLE_LIMIT,
        is_external=is_external,
        object_type=r["object_type"],
        verify_content_checksum=VALIDATE_CONTENT_CHECKSUM,
        compare_fqn=compare_fqn,
        verify_volume_content_checksum=VALIDATE_VOLUME_CONTENT_CHECKSUM,
        volume_source_hashes=vol_source_hashes,
        volume_target_hashes=vol_target_hashes,
    )
    # Spark Connect's Arrow path errors on a boolean column that is mixed
    # null/non-null across rows (volumes have N/A=None layers where tables have
    # True/False). Persist the per-layer flags as definite booleans (None→False);
    # the authoritative verdict is `overall_pass`, and `evidence_json` retains
    # the per-layer N/A / skip reasons.
    def _b(x):
        return False if x is None else bool(x)
    results_rows.append((
        result.catalog, result.schema, result.name,
        _b(result.metadata_location_ok), _b(result.delta_log_at_new_ok),
        _b(result.input_file_name_ok), _b(result.parent_managed_location_match),
        False, False, False, False, False, False,   # governance flags — Plan 2.1 expansion
        _b(result.content_checksum_ok),
        _b(result.volume_content_checksum_ok),
        result.overall_pass,
        evidence_to_json(result),
        result.validated_at,
    ))
    print(f"  {result.catalog}.{result.schema}.{result.name}: "
          f"overall_pass={result.overall_pass} "
          f"(meta={result.metadata_location_ok} "
          f"delta_log={result.delta_log_at_new_ok} "
          f"input_file={result.input_file_name_ok} "
          f"parent={result.parent_managed_location_match} "
          f"checksum={result.content_checksum_ok} "
          f"vol_checksum={result.volume_content_checksum_ok})")

if results_rows:
    writer.overwrite(results_rows)
    _OVERALL_PASS_IDX = VALIDATION_RESULTS_SCHEMA.fieldNames().index("overall_pass")
    pass_count = sum(1 for r in results_rows if r[_OVERALL_PASS_IDX])
    _extra = []
    if VALIDATE_CONTENT_CHECKSUM:
        _extra.append("table checksum")
    if VALIDATE_VOLUME_CONTENT_CHECKSUM:
        _extra.append(f"volume checksum @ {VOLUME_CHECKSUM_SAMPLE_FRACTION:g}x")
    print(f"\n{pass_count} / {len(results_rows)} passed all evidence layers"
          f"{(' (incl. ' + ', '.join(_extra) + ')') if _extra else ''}.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Failure detail

# COMMAND ----------
spark.sql(
    f"SELECT catalog, schema, name, metadata_location_ok, delta_log_at_new_ok, "
    f"       input_file_name_ok, parent_managed_location_match, content_checksum_ok, "
    f"       volume_content_checksum_ok, evidence_json "
    f"FROM {OPS_SCHEMA}.validation_results "
    f"WHERE NOT overall_pass"
).show(truncate=False)
