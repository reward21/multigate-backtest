"""Databento local ingester/converter (offline-first).

Purpose
- Convert locally downloaded Databento payloads (DBN/CSV/JSON, optionally .zst) into Parquet in the
  project's ingested data layout.
- Generate a small `*.source.json` next to the output Parquet to preserve provenance.

Important
- This module is OFFLINE-FIRST. The core conversion functions operate on *local* Databento downloads
  and do not call Databento APIs.
- Optional helper functions near the bottom can download a small raw payload via the Databento API
  (using your credits) into the raw folder *in a chosen encoding* (dbn/csv/json), then convert it to
  Parquet in the ingested layout.
- Orchestration (YAML loading + dispatch) should live in scripts/ (e.g., scripts/ingest.py).

Typical usage (from a runner script):
    from pathlib import Path
    from src.ingest_databento import convert_databento_folder_auto_ingested

    convert_databento_folder_auto_ingested(
        raw_dir=Path("data/raw/databento/spy/arcx-ohlcv-1m/ARCX-20260131-XXXXX"),
        ingested_root=Path("data/ingested/databento"),
        overwrite=False,
    )
"""

from __future__ import annotations

import json
import os
import platform
import tempfile

try:
    import zstandard as zstd
except Exception:  # pragma: no cover
    zstd = None  # type: ignore
from dataclasses import dataclass
from hashlib import sha256
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

# Optional dependency for CSV/JSON->Parquet conversion
try:
    import pyarrow as pa
    import pyarrow.csv as pa_csv
    import pyarrow.parquet as pa_parquet
    import pyarrow.json as pa_json
except Exception:  # pragma: no cover
    pa = None  # type: ignore
    pa_csv = None  # type: ignore
    pa_parquet = None  # type: ignore
    pa_json = None  # type: ignore

try:
    from importlib.metadata import version as pkg_version
except Exception:  # pragma: no cover
    pkg_version = None  # type: ignore


# Databento Python client (also provides DBNStore for local files)
import databento as db


@dataclass(frozen=True)
class ConvertOptions:
    """Options passed through to Databento's Parquet transcoder."""

    map_symbols: bool = True
    pretty_ts: bool = True
    price_type: str = "float"  # Databento accepts: "float" | "int" (keep as str for YAML friendliness)


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


# --- Helper: SHA-256 hash of a file (for provenance) ---
def _sha256_file(p: Path) -> str:
    h = sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _safe_pkg_version(name: str) -> Optional[str]:
    if pkg_version is None:
        return None
    try:
        return pkg_version(name)
    except Exception:
        return None


def _list_sidecars(raw_dir: Path) -> List[str]:
    """Return list of known sidecar files present in raw_dir."""

    names = [
        "manifest.json",
        "metadata.json",
        "symbology.json",
        "condition.json",
    ]
    present: List[str] = []
    for n in names:
        if (raw_dir / n).exists():
            present.append(n)
    return present


# --- Helpers for input fingerprinting / provenance ---
def _file_stat_dict(p: Path) -> Dict[str, Any]:
    st = p.stat()
    return {
        "path": str(p),
        "size_bytes": int(st.st_size),
        "mtime_utc": datetime.fromtimestamp(st.st_mtime, tz=timezone.utc).replace(microsecond=0).isoformat(),
    }


def _collect_input_fingerprints(
    *,
    raw_dir: Path,
    payload_filename: str,
    compute_payload_sha256: bool,
    compute_sidecar_sha256: bool,
) -> Dict[str, Any]:
    """Collect fast fingerprints (size/mtime) and optional sha256 for payload + sidecars."""

    raw_dir = raw_dir.expanduser().resolve()
    payload_path = (raw_dir / payload_filename).expanduser().resolve()

    inputs: Dict[str, Any] = {"payload": None, "sidecars": {}}

    if payload_path.exists():
        payload_info = _file_stat_dict(payload_path)
        if compute_payload_sha256:
            payload_info["sha256"] = _sha256_file(payload_path)
        inputs["payload"] = payload_info

    for name in _list_sidecars(raw_dir):
        p = (raw_dir / name).expanduser().resolve()
        if not p.exists():
            continue
        info = _file_stat_dict(p)
        if compute_sidecar_sha256:
            info["sha256"] = _sha256_file(p)
        inputs["sidecars"][name] = info

    return inputs



def _read_metadata_json(raw_dir: Path) -> Optional[Dict[str, Any]]:
    """Read Databento metadata.json from a raw folder, if present.

    IMPORTANT: This is read-only. Raw data must not be modified.
    """

    p = raw_dir / "metadata.json"
    if not p.exists():
        return None
    try:
        return json.loads(p.read_text())
    except Exception:
        return None


# --- Helpers for compressed payloads (.zst) ---
def _is_zst(path: Path) -> bool:
    return any(s.lower() == ".zst" for s in path.suffixes)


def _inner_suffix_without_zst(path: Path) -> str:
    """Return the 'inner' suffix for files like *.json.zst or *.csv.zst."""
    # e.g. foo.json.zst -> ".json"; foo.ndjson.zst -> ".ndjson"; foo.zst -> ""
    suffixes = [s.lower() for s in path.suffixes]
    if suffixes and suffixes[-1] == ".zst":
        suffixes = suffixes[:-1]
    return suffixes[-1] if suffixes else ""


def _decompress_zst_to_temp(src_path: Path) -> Path:
    """Decompress a .zst file to a temporary file and return the temp path.

    NOTE: The caller is responsible for deleting the temp file.
    """
    if zstd is None:
        raise RuntimeError(
            "zstandard is required to read *.zst payloads. Install zstandard (it is typically installed via databento)."
        )

    src_path = src_path.expanduser().resolve()
    if not src_path.exists():
        raise FileNotFoundError(f"Compressed payload not found: {src_path}")

    # Keep a recognizable suffix for tools that sniff by extension.
    inner = _inner_suffix_without_zst(src_path)
    tmp = tempfile.NamedTemporaryFile(prefix="dbt_payload_", suffix=inner or ".tmp", delete=False)
    tmp_path = Path(tmp.name)
    tmp.close()

    dctx = zstd.ZstdDecompressor()
    with open(src_path, "rb") as f_in, open(tmp_path, "wb") as f_out:
        with dctx.stream_reader(f_in) as reader:
            while True:
                chunk = reader.read(1024 * 1024)
                if not chunk:
                    break
                f_out.write(chunk)

    return tmp_path


# Helper to build fully disambiguated output base filename
def _build_output_basename_from_metadata(*, raw_dir: Path, symbol: str) -> str:
    """Build an unambiguous output base filename.

    Format:
        <symbol>-<dataset>-<schema>

    Where:
      - dataset comes from metadata.query.dataset (dots replaced with dashes)
      - schema comes from metadata.query.schema

    Examples:
        symbol=spy, dataset=ARCX.PILLAR, schema=ohlcv-1d
        -> spy-arcx-pillar-ohlcv-1d

    IMPORTANT: Reads metadata.json (if present) but never modifies raw data.
    """

    md = _read_metadata_json(raw_dir)
    dataset = None
    schema = None

    if isinstance(md, dict):
        q = md.get("query", {})
        if isinstance(q, dict):
            dataset = q.get("dataset")
            schema = q.get("schema")

    dataset_s = str(dataset).strip().lower().replace(".", "-") if dataset else "unknown-dataset"
    schema_s = str(schema).strip().lower() if schema else "unknown-schema"
    sym_s = symbol.strip().lower()

    return f"{sym_s}-{dataset_s}-{schema_s}"


def plan_ingested_paths_from_metadata(
    *,
    raw_dir: Path,
    ingested_root: Path,
    symbol_override: Optional[str] = None,
    dataset_folder_override: Optional[str] = None,
) -> Tuple[Path, str, str]:
    """Plan ingested output paths based on raw Databento `metadata.json` (read-only).

    Returns:
        (out_dir, symbol, dataset_folder)

    Canonical layout:
        <ingested_root>/<symbol>/<venue>-<schema>/ (typically under data/ingested/databento/)

    Where:
      - symbol comes from metadata.query.symbols[0] (lowercased) unless overridden
      - venue comes from metadata.query.dataset before the dot (e.g., "XNAS" in "XNAS.ITCH")
      - schema comes from metadata.query.schema (e.g., "ohlcv-1m", "trades")
      - dataset_folder = "<venue>-<schema>" lowercased (e.g., "xnas-trades")

    Works for BOTH DBN and CSV payload downloads because both include metadata.json.
    """

    raw_dir = raw_dir.expanduser().resolve()
    ingested_root = ingested_root.expanduser().resolve()

    md = _read_metadata_json(raw_dir)

    # Symbol
    sym: Optional[str] = None
    if symbol_override:
        sym = symbol_override.strip().lower()
    elif md:
        q = md.get("query", {}) if isinstance(md, dict) else {}
        syms = q.get("symbols")
        if isinstance(syms, list) and syms:
            s0 = syms[0]
            if isinstance(s0, str) and s0:
                sym = s0.strip().lower()

    # Venue + schema -> dataset folder
    dataset_folder: Optional[str] = None
    if dataset_folder_override:
        dataset_folder = dataset_folder_override.strip().lower()
    elif md:
        q = md.get("query", {}) if isinstance(md, dict) else {}
        dataset = q.get("dataset")
        schema = q.get("schema")

        venue: Optional[str] = None
        if isinstance(dataset, str) and dataset:
            venue = dataset.split(".", 1)[0].strip().lower()

        if isinstance(schema, str) and schema:
            schema_l = schema.strip().lower()
            dataset_folder = f"{venue}-{schema_l}" if venue else schema_l

    if not sym:
        raise RuntimeError(
            f"Could not determine symbol for raw_dir={raw_dir}. "
            "Provide symbol_override or ensure metadata.json has query.symbols."
        )

    if not dataset_folder:
        raise RuntimeError(
            f"Could not determine dataset folder for raw_dir={raw_dir}. "
            "Provide dataset_folder_override or ensure metadata.json has query.dataset and query.schema."
        )

    out_dir = ingested_root / sym / dataset_folder
    return out_dir, sym, dataset_folder


def find_single_dbn_zst(
    raw_dir: Path,
    filename_contains: Optional[str] = None,
) -> Path:
    """Find exactly one .dbn.zst file in a raw dataset folder.

    If filename_contains is provided, only files containing that substring are considered.

    Raises:
        FileNotFoundError: if no matching file exists
        RuntimeError: if multiple matches exist
    """

    raw_dir = raw_dir.expanduser().resolve()
    if not raw_dir.exists():
        raise FileNotFoundError(f"Raw directory not found: {raw_dir}")

    candidates = sorted(raw_dir.glob("*.dbn.zst"))
    if filename_contains:
        candidates = [p for p in candidates if filename_contains in p.name]

    if not candidates:
        suffix = f" (contains: {filename_contains})" if filename_contains else ""
        raise FileNotFoundError(f"No .dbn.zst files found in {raw_dir}{suffix}")

    if len(candidates) > 1:
        names = ", ".join(p.name for p in candidates)
        raise RuntimeError(
            "Multiple .dbn.zst files matched; be more specific (or split folders). "
            f"raw_dir={raw_dir} matches=[{names}]"
        )

    return candidates[0]


def find_single_payload_file(
    raw_dir: Path,
    filename_contains: Optional[str] = None,
) -> Tuple[Path, str]:
    """Find exactly one Databento payload file in a raw dataset folder.

    Preference order:
      1) *.dbn.zst (DBN)
      2) *.csv or *.csv.zst (CSV)
      3) *.json / *.jsonl / *.ndjson (JSON)
         plus their compressed variants: *.json.zst / *.jsonl.zst / *.ndjson.zst

    For CSV, this ignores common sidecar CSVs like symbology.csv.
    For JSON, this ignores Databento sidecars: metadata.json, manifest.json, symbology.json, condition.json,
    and also ignores our own provenance outputs: *.source.json.

    Returns:
        (path, kind) where kind is "dbn" | "csv" | "json".
    """

    raw_dir = raw_dir.expanduser().resolve()

    # Prefer DBN if present
    try:
        dbn = find_single_dbn_zst(raw_dir, filename_contains=filename_contains)
        return dbn, "dbn"
    except FileNotFoundError:
        pass

    # Fall back to a single CSV payload
    candidates: List[Path] = []
    candidates.extend(sorted(raw_dir.glob("*.csv")))
    candidates.extend(sorted(raw_dir.glob("*.csv.zst")))

    # Exclude known sidecar CSVs
    candidates = [
        p
        for p in candidates
        if p.name.lower() not in ("symbology.csv", "symbology.csv.zst")
    ]

    if filename_contains:
        candidates = [p for p in candidates if filename_contains in p.name]

    # If no CSV payload matched, we will try JSON next.
    if len(candidates) > 1:
        names = ", ".join(p.name for p in candidates)
        raise RuntimeError(
            "Multiple CSV payloads matched; be more specific (or split folders). "
            f"raw_dir={raw_dir} matches=[{names}]"
        )

    # Return CSV if exactly one matched; otherwise fall through to JSON.
    if len(candidates) == 1:
        return candidates[0], "csv"

    # Fall back to a single JSON payload (json/jsonl/ndjson and their .zst variants)
    json_candidates: List[Path] = []
    json_candidates.extend(sorted(raw_dir.glob("*.json")))
    json_candidates.extend(sorted(raw_dir.glob("*.jsonl")))
    json_candidates.extend(sorted(raw_dir.glob("*.ndjson")))
    json_candidates.extend(sorted(raw_dir.glob("*.json.zst")))
    json_candidates.extend(sorted(raw_dir.glob("*.jsonl.zst")))
    json_candidates.extend(sorted(raw_dir.glob("*.ndjson.zst")))

    # Exclude known Databento sidecar JSONs and our own provenance outputs
    sidecar_json = {
        "manifest.json",
        "metadata.json",
        "symbology.json",
        "condition.json",
        "source.json",
    }
    json_candidates = [p for p in json_candidates if p.name.lower() not in sidecar_json]
    json_candidates = [p for p in json_candidates if not p.name.lower().endswith(".source.json")]

    if filename_contains:
        json_candidates = [p for p in json_candidates if filename_contains in p.name]

    if not json_candidates:
        suffix = f" (contains: {filename_contains})" if filename_contains else ""
        raise FileNotFoundError(f"No DBN/CSV/JSON payload files found in {raw_dir}{suffix}")

    if len(json_candidates) > 1:
        names = ", ".join(p.name for p in json_candidates)
        raise RuntimeError(
            "Multiple JSON payloads matched; be more specific (or split folders). "
            f"raw_dir={raw_dir} matches=[{names}]"
        )

    return json_candidates[0], "json"


def convert_dbn_file_to_parquet(
    dbn_path: Path,
    out_parquet_path: Path,
    opts: ConvertOptions = ConvertOptions(),
    overwrite: bool = False,
) -> Path:
    """Convert a single DBN(.dbn.zst) file to Parquet using Databento's DBNStore.

    This does NOT call the Databento API.
    """

    dbn_path = dbn_path.expanduser().resolve()
    out_parquet_path = out_parquet_path.expanduser().resolve()

    if not dbn_path.exists():
        raise FileNotFoundError(f"DBN file not found: {dbn_path}")

    out_parquet_path.parent.mkdir(parents=True, exist_ok=True)

    if out_parquet_path.exists() and not overwrite:
        raise FileExistsError(
            f"Refusing to overwrite existing file: {out_parquet_path}. Use overwrite=True."
        )

    store = db.DBNStore.from_file(str(dbn_path))
    # Databento writes Parquet via PyArrow internally.
    store.to_parquet(
        str(out_parquet_path),
        map_symbols=opts.map_symbols,
        pretty_ts=opts.pretty_ts,
        price_type=opts.price_type,
    )

    return out_parquet_path


def convert_csv_file_to_parquet(
    csv_path: Path,
    out_parquet_path: Path,
    overwrite: bool = False,
) -> Path:
    """Convert a single CSV payload file to Parquet.

    Requires pyarrow.
    """

    if pa_csv is None or pa_parquet is None:
        raise RuntimeError(
            "pyarrow is required for CSV->Parquet conversion. Install pyarrow to use CSV payloads."
        )

    csv_path = csv_path.expanduser().resolve()
    out_parquet_path = out_parquet_path.expanduser().resolve()

    if not csv_path.exists():
        raise FileNotFoundError(f"CSV file not found: {csv_path}")

    out_parquet_path.parent.mkdir(parents=True, exist_ok=True)

    if out_parquet_path.exists() and not overwrite:
        raise FileExistsError(
            f"Refusing to overwrite existing file: {out_parquet_path}. Use overwrite=True."
        )

    tmp_path: Optional[Path] = None
    try:
        read_path = csv_path
        if csv_path.name.lower().endswith(".csv.zst"):
            tmp_path = _decompress_zst_to_temp(csv_path)
            read_path = tmp_path

        table = pa_csv.read_csv(str(read_path))
        pa_parquet.write_table(table, str(out_parquet_path))
    finally:
        if tmp_path is not None:
            try:
                tmp_path.unlink()
            except Exception:
                pass

    return out_parquet_path


def convert_json_file_to_parquet(
    json_path: Path,
    out_parquet_path: Path,
    overwrite: bool = False,
) -> Path:
    """Convert a single JSON payload file to Parquet.

    Supports newline-delimited JSON (jsonl/ndjson) via pyarrow.json.read_json,
    with a small fallback for standard JSON arrays/dicts.

    Requires pyarrow.
    """

    if pa_parquet is None or pa is None:
        raise RuntimeError(
            "pyarrow is required for JSON->Parquet conversion. Install pyarrow to use JSON payloads."
        )

    json_path = json_path.expanduser().resolve()
    out_parquet_path = out_parquet_path.expanduser().resolve()

    if not json_path.exists():
        raise FileNotFoundError(f"JSON file not found: {json_path}")

    out_parquet_path.parent.mkdir(parents=True, exist_ok=True)

    if out_parquet_path.exists() and not overwrite:
        raise FileExistsError(
            f"Refusing to overwrite existing file: {out_parquet_path}. Use overwrite=True."
        )

    tmp_path: Optional[Path] = None
    try:
        read_path = json_path
        if _is_zst(json_path):
            tmp_path = _decompress_zst_to_temp(json_path)
            read_path = tmp_path

        table = None

        # Prefer pyarrow.json when available (great for JSONL/NDJSON)
        if "pa_json" in globals() and pa_json is not None:  # type: ignore[name-defined]
            try:
                table = pa_json.read_json(str(read_path))  # type: ignore[name-defined]
            except Exception:
                table = None

        # Fallback: try loading as standard JSON
        if table is None:
            obj = json.loads(Path(read_path).read_text())
            if isinstance(obj, list):
                table = pa.Table.from_pylist(obj)
            elif isinstance(obj, dict):
                table = pa.Table.from_pylist([obj])

        if table is None:
            raise RuntimeError(f"Failed to convert JSON payload to table: {json_path}")

        pa_parquet.write_table(table, str(out_parquet_path))
        return out_parquet_path
    finally:
        if tmp_path is not None:
            try:
                tmp_path.unlink()
            except Exception:
                pass


def write_source_json(
    *,
    source_json_path: Path,
    raw_dir: Path,
    payload_filename: str,
    dataset_id: Optional[str] = None,
    opts: ConvertOptions = ConvertOptions(),
    extra: Optional[Dict[str, Any]] = None,
    overwrite: bool = True,
    compute_payload_sha256: bool = False,
    compute_sidecar_sha256: bool = True,
) -> Path:
    """Write a small provenance file next to the output Parquet."""

    source_json_path = source_json_path.expanduser().resolve()
    source_json_path.parent.mkdir(parents=True, exist_ok=True)

    if source_json_path.exists() and not overwrite:
        raise FileExistsError(f"Refusing to overwrite existing file: {source_json_path}")

    payload: Dict[str, Any] = {
        "created_at_utc": _utc_now_iso(),
        "vendor": "databento",
        "dataset_id": dataset_id,
        "payload_kind": None,
        "raw_dir": str(raw_dir.expanduser().resolve()),
        "payload_file": payload_filename,
        "inputs": _collect_input_fingerprints(
            raw_dir=raw_dir,
            payload_filename=payload_filename,
            compute_payload_sha256=compute_payload_sha256,
            compute_sidecar_sha256=compute_sidecar_sha256,
        ),
        "sidecars_present": _list_sidecars(raw_dir),
        "convert_options": {
            "map_symbols": opts.map_symbols,
            "pretty_ts": opts.pretty_ts,
            "price_type": opts.price_type,
        },
        "environment": {
            "python": platform.python_version(),
            "platform": platform.platform(),
            "databento": _safe_pkg_version("databento"),
            "databento-dbn": _safe_pkg_version("databento-dbn"),
            "pyarrow": _safe_pkg_version("pyarrow"),
        },
    }

    if extra:
        payload["extra"] = extra

    # If caller provided kind in extra, mirror it at top-level for easier grepping.
    if extra and "payload_kind" in extra:
        payload["payload_kind"] = extra.get("payload_kind")

    source_json_path.write_text(json.dumps(payload, indent=2, sort_keys=False) + "\n")
    return source_json_path


def convert_dbn_folder(
    *,
    raw_dir: Path,
    out_dir: Path,
    dataset_id: Optional[str] = None,
    filename_contains: Optional[str] = None,
    opts: ConvertOptions = ConvertOptions(),
    parquet_name: Optional[str] = None,
    source_name: Optional[str] = None,
    overwrite: bool = False,
    compute_payload_sha256: bool = False,
    compute_sidecar_sha256: bool = True,
) -> Tuple[Path, Path]:
    """ Convert a raw Databento folder (containing one .dbn.zst + sidecars) into Parquet.

    Returns:
        (parquet_path, source_json_path)
    """

    raw_dir = raw_dir.expanduser().resolve()
    out_dir = out_dir.expanduser().resolve()

    # Default output file name:
    # Prefer a stable, dataset-scoped name (e.g., "arcx-ohlcv-1m.parquet") so multiple
    # datasets don't all end up named "data.parquet".
    if parquet_name is None:
        parquet_name = f"{dataset_id}.parquet" if dataset_id else "data.parquet"

    # Default provenance file name:
    # Keep it dataset-scoped (e.g., "arcx-ohlcv-1m.source.json") so multiple datasets
    # don't all end up named "source.json".
    if source_name is None:
        if dataset_id:
            source_name = f"{dataset_id}.source.json"
        else:
            source_name = "source.json"

    dbn_path = find_single_dbn_zst(raw_dir, filename_contains=filename_contains)

    parquet_path = out_dir / parquet_name
    source_json_path = out_dir / source_name

    convert_dbn_file_to_parquet(
        dbn_path=dbn_path,
        out_parquet_path=parquet_path,
        opts=opts,
        overwrite=overwrite,
    )

    write_source_json(
        source_json_path=source_json_path,
        raw_dir=raw_dir,
        payload_filename=dbn_path.name,
        dataset_id=dataset_id,
        opts=opts,
        extra={
            "out_dir": str(out_dir),
            "parquet": parquet_name,
            "source": source_name,
        },
        overwrite=True,
        compute_payload_sha256=compute_payload_sha256,
        compute_sidecar_sha256=compute_sidecar_sha256,
    )

    return parquet_path, source_json_path


def convert_databento_folder(
    *,
    raw_dir: Path,
    out_dir: Path,
    dataset_id: Optional[str] = None,
    filename_contains: Optional[str] = None,
    opts: ConvertOptions = ConvertOptions(),
    parquet_name: Optional[str] = None,
    source_name: Optional[str] = None,
    overwrite: bool = False,
    compute_payload_sha256: bool = False,
    compute_sidecar_sha256: bool = True,
) -> Tuple[Path, Path]:
    """Convert a raw Databento folder into Parquet.

    Auto-detects payload type:
      - DBN (.dbn.zst) -> uses Databento DBNStore
      - CSV (.csv)     -> uses PyArrow CSV reader
      - JSON (.json/.jsonl/.ndjson) -> uses PyArrow JSON reader (or fallback)

    Returns:
        (parquet_path, source_json_path)
    """

    raw_dir = raw_dir.expanduser().resolve()
    out_dir = out_dir.expanduser().resolve()

    if parquet_name is None:
        parquet_name = f"{dataset_id}.parquet" if dataset_id else "data.parquet"

    if source_name is None:
        source_name = f"{dataset_id}.source.json" if dataset_id else "source.json"

    payload_path, kind = find_single_payload_file(raw_dir, filename_contains=filename_contains)

    parquet_path = out_dir / parquet_name
    source_json_path = out_dir / source_name

    if kind == "dbn":
        convert_dbn_file_to_parquet(
            dbn_path=payload_path,
            out_parquet_path=parquet_path,
            opts=opts,
            overwrite=overwrite,
        )
    elif kind == "csv":
        convert_csv_file_to_parquet(
            csv_path=payload_path,
            out_parquet_path=parquet_path,
            overwrite=overwrite,
        )
    else:
        convert_json_file_to_parquet(
            json_path=payload_path,
            out_parquet_path=parquet_path,
            overwrite=overwrite,
        )

    write_source_json(
        source_json_path=source_json_path,
        raw_dir=raw_dir,
        payload_filename=payload_path.name,
        dataset_id=dataset_id,
        opts=opts,
        extra={
            "payload_kind": kind,
            "payload_compressed": _is_zst(payload_path),
            "payload_inner_kind": (
                _inner_suffix_without_zst(payload_path).lstrip(".") if _is_zst(payload_path) else None
            ),
            "out_dir": str(out_dir),
            "parquet": parquet_name,
            "source": source_name,
        },
        overwrite=True,
        compute_payload_sha256=compute_payload_sha256,
        compute_sidecar_sha256=compute_sidecar_sha256,
    )

    return parquet_path, source_json_path


def convert_databento_folder_auto_ingested(
    *,
    raw_dir: Path,
    ingested_root: Path = Path("data/ingested/databento"),
    filename_contains: Optional[str] = None,
    symbol_override: Optional[str] = None,
    dataset_folder_override: Optional[str] = None,
    opts: ConvertOptions = ConvertOptions(),
    overwrite: bool = False,
    compute_payload_sha256: bool = False,
    compute_sidecar_sha256: bool = True,
) -> Tuple[Path, Path]:
    """Convert a raw Databento folder into the canonical ingested layout.

    Reads (only) `metadata.json` from `raw_dir` and writes outputs to:
        <ingested_root>/<symbol>/<venue>-<schema>/ (defaults to data/ingested/databento/...)

    Output filenames are dataset-scoped:
        <dataset_id>.parquet
        <dataset_id>.source.json

    Where dataset_id defaults to the computed dataset folder (e.g., "xnas-trades").

    Works for BOTH DBN (.dbn.zst) and CSV payloads.
    """

    out_dir, _sym, dataset_folder = plan_ingested_paths_from_metadata(
        raw_dir=raw_dir,
        ingested_root=ingested_root,
        symbol_override=symbol_override,
        dataset_folder_override=dataset_folder_override,
    )

    dataset_id = dataset_folder

    # Use fully disambiguated filenames so files remain identifiable even if moved.
    base_name = _build_output_basename_from_metadata(raw_dir=raw_dir, symbol=_sym)
    parquet_name = f"{base_name}.parquet"
    source_name = f"{base_name}.source.json"

    # Ensure the folder exists (idempotent)
    out_dir.mkdir(parents=True, exist_ok=True)

    return convert_databento_folder(
        raw_dir=raw_dir,
        out_dir=out_dir,
        dataset_id=dataset_id,
        filename_contains=filename_contains,
        opts=opts,
        parquet_name=parquet_name,
        source_name=source_name,
        overwrite=overwrite,
        # hashing/fingerprints for provenance
        compute_payload_sha256=compute_payload_sha256,
        compute_sidecar_sha256=compute_sidecar_sha256,
    )


def guess_ingested_out_dir(
    *,
    ingested_root: Path,
    symbol: str,
    dataset_folder: str,
) -> Path:
    """Small helper to match your folder layout.

    Example:
        ingested_root=data/ingested/databento
        symbol=spy
        dataset_folder=arcx-ohlcv-1m

        -> data/ingested/databento/spy/arcx-ohlcv-1m
    """

    return ingested_root / symbol / dataset_folder


# -----------------------------------------------------------------------------
# Optional API helpers (raw download -> offline conversion)
# -----------------------------------------------------------------------------

def _ensure_str_list(symbols: Iterable[str] | str) -> List[str]:
    if isinstance(symbols, str):
        return [symbols]
    return [str(s) for s in symbols]


def _build_raw_payload_filename(
    *,
    symbol: str,
    dataset: str,
    schema: str,
    encoding: str,
    compression: str,
) -> str:
    """Create a deterministic raw payload filename.

    Examples:
      spy-arcx-pillar-ohlcv-1m.dbn.zst
      msft-xnas-itch-trades.csv.zst
      es.fut-glbx-mdp3-ohlcv-1m.json.zst
    """

    sym = symbol.strip().lower()
    ds = dataset.strip().lower().replace(".", "-")
    sc = schema.strip().lower()
    enc = encoding.strip().lower()
    comp = compression.strip().lower() if compression else "none"

    # Normalize encoding
    if enc in ("dbn", "dbn.zst", "dbn_zst"):
        enc = "dbn"
    elif enc in ("csv", "csv.zst", "csv_zst"):
        enc = "csv"
    elif enc in ("json", "json.zst", "json_zst", "jsonl", "ndjson"):
        enc = "json"

    # Normalize compression
    if comp in ("zstd", "zst"):
        comp = "zstd"
    elif comp in ("none", "", "null"):
        comp = "none"

    ext = enc
    if enc == "dbn":
        ext = "dbn"

    name = f"{sym}-{ds}-{sc}.{ext}"
    if comp == "zstd":
        name += ".zst"
    return name


def write_metadata_json_for_api_download(
    *,
    raw_dir: Path,
    dataset: str,
    schema: str,
    symbols: Iterable[str] | str,
    start: Any,
    end: Any | None,
    encoding: str,
    compression: str,
    extra: Optional[Dict[str, Any]] = None,
    overwrite: bool = False,
) -> Path:
    """Create a Databento-like metadata.json for API-downloaded raw folders.

    This is ONLY for folders we download via API, so the offline converter can
    plan output paths uniformly.

    NOTE: This function CREATES metadata.json if missing. It does not mutate
    existing vendor-provided metadata.json unless overwrite=True is explicitly used.
    """

    raw_dir = raw_dir.expanduser().resolve()
    raw_dir.mkdir(parents=True, exist_ok=True)

    p = raw_dir / "metadata.json"
    if p.exists() and not overwrite:
        raise FileExistsError(f"Refusing to overwrite existing metadata.json: {p}")

    payload: Dict[str, Any] = {
        "created_at_utc": _utc_now_iso(),
        "vendor": "databento",
        "query": {
            "dataset": dataset,
            "schema": schema,
            "symbols": _ensure_str_list(symbols),
            "start": str(start),
            "end": str(end) if end is not None else None,
            "encoding": encoding,
            "compression": compression,
        },
    }

    if extra:
        payload["extra"] = extra

    p.write_text(json.dumps(payload, indent=2, sort_keys=False) + "\n")
    return p


def download_timeseries_range_to_raw_folder(
    *,
    raw_dir: Path,
    dataset: str,
    schema: str,
    symbols: Iterable[str] | str,
    start: Any,
    end: Any | None = None,
    encoding: str = "dbn",
    compression: str = "zstd",
    limit: Optional[int] = None,
    pretty_ts: bool = True,
    pretty_px: bool = True,
    map_symbols: bool = True,
    api_key_env: str = "DATABENTO_API_KEY",
    overwrite: bool = False,
) -> Tuple[Path, Path]:
    """Download a Databento timeseries range into a raw folder.

    Writes:
      - <payload> in the chosen encoding (dbn/csv/json) and compression
      - metadata.json (created if missing)

    Returns:
      (payload_path, metadata_json_path)
    """

    raw_dir = raw_dir.expanduser().resolve()
    raw_dir.mkdir(parents=True, exist_ok=True)

    api_key = os.getenv(api_key_env)
    if not api_key:
        raise RuntimeError(
            f"Missing Databento API key. Set env var {api_key_env}=... (or put it in .env and load it)."
        )

    sym_list = _ensure_str_list(symbols)
    symbol_for_name = sym_list[0] if sym_list else "unknown"

    payload_name = _build_raw_payload_filename(
        symbol=symbol_for_name,
        dataset=dataset,
        schema=schema,
        encoding=encoding,
        compression=compression,
    )
    payload_path = raw_dir / payload_name

    if payload_path.exists() and not overwrite:
        raise FileExistsError(f"Refusing to overwrite existing payload: {payload_path}")

    md_path = raw_dir / "metadata.json"
    if not md_path.exists() or overwrite:
        write_metadata_json_for_api_download(
            raw_dir=raw_dir,
            dataset=dataset,
            schema=schema,
            symbols=sym_list,
            start=start,
            end=end,
            encoding=encoding,
            compression=compression,
            extra={"downloaded_via": "api"},
            overwrite=overwrite,
        )

    client = db.Historical(api_key)
    store = client.timeseries.get_range(
        dataset=dataset,
        schema=schema,
        symbols=sym_list,
        start=start,
        end=end,
        limit=limit,
    )

    enc = encoding.strip().lower()
    comp = compression.strip().lower() if compression else "none"

    if enc == "dbn":
        store.to_file(str(payload_path), compression=("zstd" if comp in ("zstd", "zst") else "none"))
    elif enc == "csv":
        store.to_csv(
            str(payload_path),
            pretty_ts=pretty_ts,
            pretty_px=pretty_px,
            map_symbols=map_symbols,
            compression=("zstd" if comp in ("zstd", "zst") else "none"),
        )
    elif enc == "json":
        store.to_json(
            str(payload_path),
            pretty_ts=pretty_ts,
            pretty_px=pretty_px,
            map_symbols=map_symbols,
            compression=("zstd" if comp in ("zstd", "zst") else "none"),
        )
    else:
        raise ValueError(f"Unsupported encoding: {encoding}. Expected 'dbn' | 'csv' | 'json'.")

    return payload_path, md_path


def download_and_convert_timeseries_to_ingested(
    *,
    raw_dir: Path,
    ingested_root: Path = Path("data/ingested/databento"),
    dataset: str,
    schema: str,
    symbols: Iterable[str] | str,
    start: Any,
    end: Any | None = None,
    encoding: str = "dbn",
    compression: str = "zstd",
    limit: Optional[int] = None,
    filename_contains: Optional[str] = None,
    symbol_override: Optional[str] = None,
    dataset_folder_override: Optional[str] = None,
    opts: ConvertOptions = ConvertOptions(),
    overwrite: bool = False,
    compute_payload_sha256: bool = False,
    compute_sidecar_sha256: bool = True,
) -> Tuple[Path, Path, Path]:
    """API download into raw_dir (encoded) -> convert to Parquet in ingested layout.

    Returns:
      (raw_payload_path, ingested_parquet_path, ingested_source_json_path)
    """

    raw_payload_path, _md = download_timeseries_range_to_raw_folder(
        raw_dir=raw_dir,
        dataset=dataset,
        schema=schema,
        symbols=symbols,
        start=start,
        end=end,
        encoding=encoding,
        compression=compression,
        limit=limit,
        overwrite=overwrite,
    )

    parquet_path, source_path = convert_databento_folder_auto_ingested(
        raw_dir=raw_dir,
        ingested_root=ingested_root,
        filename_contains=filename_contains,
        symbol_override=symbol_override,
        dataset_folder_override=dataset_folder_override,
        opts=opts,
        overwrite=overwrite,
        compute_payload_sha256=compute_payload_sha256,
        compute_sidecar_sha256=compute_sidecar_sha256,
    )

    return raw_payload_path, parquet_path, source_path


# NOTE: No CLI in this module on purpose.
# Build `scripts/ingest.py` to load ingest.yaml and call convert_databento_folder_auto_ingested().

# Backward-compatible aliases
plan_processed_paths_from_metadata = plan_ingested_paths_from_metadata
convert_databento_folder_auto_processed = convert_databento_folder_auto_ingested
guess_processed_out_dir = guess_ingested_out_dir