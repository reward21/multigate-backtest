#!/usr/bin/env python3
"""GulfChain ingest dispatcher.

This script reads repo-root `ingest.yaml` and dispatches ingestion jobs.

Design goals
- One config file (ingest.yaml) controls all ingestion.
- Raw artifacts live under data/raw (vendor-native, untouched).
- Normalized outputs live under data/ingested (Parquet + *.source.json).
- Easy to extend: add a new key under `sources:` in ingest.yaml, then add a handler
  function below in this file.

Extending for a new source (template)
1) Add config under ingest.yaml:

   sources:
     newvendor:
       enabled: true
       mode: offline
       jobs:
         - name: example
           ...

2) Add a handler function here:

   def run_newvendor(cfg, defaults, paths) -> int:
       # import src.ingest_newvendor and execute jobs
       return 0

3) Register it in HANDLERS at the bottom.
"""

from __future__ import annotations

import sys
import os
import inspect
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


import yaml
from decimal import Decimal



REPO_ROOT = Path(__file__).resolve().parents[1]
# Ensure `src.*` imports resolve when executed as `python scripts/ingest.py`.
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

# Load repo-root .env so API-mode ingests can see credentials when run via `python -m scripts.ingest`.
# NOTE: Python does NOT automatically load .env files.
try:
    from dotenv import load_dotenv  # type: ignore

    load_dotenv(REPO_ROOT / ".env")
except Exception:
    # If python-dotenv isn't installed (or .env is missing), continue.
    pass


def ensure_running_in_venv() -> None:
    """Fail fast if this script is executed outside the repo virtualenv.

    This prevents accidental use of system/Homebrew Python when running via shebang.
    """

    exe = str(Path(sys.executable).resolve())
    venv_env = os.environ.get("VIRTUAL_ENV")

    # Preferred: VIRTUAL_ENV is set and matches this repo's .venv
    if venv_env:
        try:
            venv_env_p = Path(venv_env).resolve()
            expected = (REPO_ROOT / ".venv").resolve()
            if venv_env_p == expected:
                return
        except Exception:
            pass

    # Fallback: executable path contains /.venv/
    if f"{REPO_ROOT}/.venv/" in exe:
        return

    raise RuntimeError(
        "This script must be run from the repo virtualenv. "
        "Activate it with: source .venv/bin/activate"
    )


def eprint(*args: Any) -> None:
    print(*args, file=sys.stderr)


def load_yaml(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Missing config file: {path}")
    data = yaml.safe_load(path.read_text())
    if not isinstance(data, dict):
        raise ValueError("ingest.yaml must parse to a mapping/object")
    return data


def require(d: Dict[str, Any], key: str, ctx: str) -> Any:
    if key not in d:
        raise KeyError(f"Missing required key '{key}' in {ctx}")
    return d[key]



def as_bool(v: Any, default: bool = False) -> bool:
    if v is None:
        return default
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return bool(v)
    if isinstance(v, str):
        return v.strip().lower() in ("1", "true", "yes", "y", "on")
    return default


def as_decimal(v: Any, default: Decimal = Decimal("0")) -> Decimal:
    if v is None:
        return default
    if isinstance(v, Decimal):
        return v
    if isinstance(v, (int, float)):
        return Decimal(str(v))
    if isinstance(v, str):
        s = v.strip()
        if not s:
            return default
        return Decimal(s)
    return default


def confirm_or_abort(prompt: str) -> None:
    """Ask for confirmation if running in an interactive TTY.

    Raises RuntimeError if the user declines or if stdin is non-interactive.
    """

    if not sys.stdin.isatty():
        raise RuntimeError(
            "Refusing to run a cost-confirmed API job in non-interactive mode. "
            "Run in a terminal (TTY) or disable sources.<vendor>.api.require_confirm."
        )

    ans = input(prompt).strip().lower()
    if ans not in ("y", "yes"):
        raise RuntimeError("User aborted (did not confirm).")


def merge_job_defaults(defaults: Dict[str, Any], job: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(defaults)
    out.update(job)
    return out


def normalize_paths(paths_cfg: Dict[str, Any]) -> Tuple[Path, Path]:
    raw_root = Path(require(paths_cfg, "raw_root", "paths")).expanduser()
    ingested_root = Path(require(paths_cfg, "ingested_root", "paths")).expanduser()

    # Interpret relative to repo root
    if not raw_root.is_absolute():
        raw_root = (REPO_ROOT / raw_root).resolve()
    if not ingested_root.is_absolute():
        ingested_root = (REPO_ROOT / ingested_root).resolve()

    return raw_root, ingested_root


# -----------------------------------------------------------------------------
# Shared job filtering by source.mode
# -----------------------------------------------------------------------------

def filter_jobs_by_source_mode(source_name: str, source_cfg: Dict[str, Any]) -> Dict[str, Any]:
    """Return a copy of source_cfg with jobs filtered to match source_cfg.mode.

    Convention:
      - source_cfg.mode selects which job blocks run (api vs offline).
      - each job should declare `mode: api|offline`.
      - if mode is set on the source and a job omits job.mode, we SKIP it to avoid surprises.

    If the source has no jobs list, or mode is missing/empty, returns source_cfg unchanged.
    """

    if not isinstance(source_cfg, dict):
        return source_cfg

    selected_mode = str(source_cfg.get("mode") or "").strip().lower()
    jobs = source_cfg.get("jobs")

    if not selected_mode or not isinstance(jobs, list) or not jobs:
        return source_cfg

    if selected_mode not in ("offline", "api"):
        # Let the handler raise a clearer error later.
        return source_cfg

    filtered: List[Dict[str, Any]] = []
    for j in jobs:
        if not isinstance(j, dict):
            # Keep invalid items; handler will report.
            filtered.append(j)
            continue
        job_mode = str(j.get("mode") or "").strip().lower()
        if not job_mode:
            print(f"[{source_name}]   SKIP job={j.get('name','(unnamed)')}: job.mode is missing (source.mode={selected_mode})")
            continue
        if job_mode != selected_mode:
            print(f"[{source_name}]   SKIP job={j.get('name','(unnamed)')}: job.mode={job_mode} does not match source.mode={selected_mode}")
            continue
        filtered.append(j)

    out = dict(source_cfg)
    out["jobs"] = filtered
    return out


# -----------------------------------------------------------------------------
# Databento handler
# -----------------------------------------------------------------------------


def run_databento(source_cfg: Dict[str, Any], defaults: Dict[str, Any], raw_root: Path, ingested_root: Path) -> int:
    """Run Databento jobs.

    Supports:
      - mode: offline -> convert existing raw_dir(s)
      - mode: api     -> download into raw_dir then convert

    Returns non-zero on any job failure.
    """

    try:
        from src.ingest_databento import (
            convert_databento_folder_auto_ingested,
            download_and_convert_timeseries_to_ingested,
            ConvertOptions,
        )
    except Exception as ex:
        eprint("[databento] ❗ERROR: failed to import src.ingest_databento:", ex)
        return 2

    enabled = as_bool(source_cfg.get("enabled"), True)
    if not enabled:
        print("[databento] disabled; skipping")
        return 0

    mode = str(source_cfg.get("mode", "offline")).strip().lower()
    if mode not in ("offline", "api"):
        eprint(f"[databento] ❗ERROR: unsupported mode '{mode}' (expected offline|api)")
        return 2

    api_cfg = source_cfg.get("api") or {}
    if not isinstance(api_cfg, dict):
        eprint("[databento] ❗ERROR: sources.databento.api must be a mapping")
        return 2

    jobs = source_cfg.get("jobs") or []
    if not isinstance(jobs, list):
        eprint("[databento] ❗ERROR: sources.databento.jobs must be a list")
        return 2

    if not jobs:
        print("[databento] no jobs; skipping")
        return 0

    # Global defaults for API mode
    api_encoding = str(api_cfg.get("encoding", "dbn")).strip().lower()
    api_compression = str(api_cfg.get("compression", "zstd")).strip().lower()
    api_limit = api_cfg.get("limit", None)

    # Optional cost preview / guardrails for API mode
    api_print_cost = as_bool(api_cfg.get("print_cost"), True)

    # Safety-first default: if you're running Databento in API mode, require confirmation
    # unless the config explicitly disables it.
    api_require_confirm = as_bool(api_cfg.get("require_confirm"), True)

    # Source-level max cost cap (USD). Can be overridden per job via job.api.max_cost_usd.
    api_max_cost_usd = api_cfg.get("max_cost_usd", None)
    api_max_cost_usd_d = as_decimal(api_max_cost_usd, default=Decimal("0"))

    # ConvertOptions is only used for some conversions; keep default instance for now
    opts = ConvertOptions()

    failures = 0

    for job0 in jobs:
        if not isinstance(job0, dict):
            eprint("[databento] ❗ERROR: each job must be a mapping")
            failures += 1
            continue

        job = merge_job_defaults(defaults, job0)
        name = str(job.get("name") or "(unnamed)")
        overwrite = as_bool(job.get("overwrite"), False)
        compute_payload_sha256 = as_bool(job.get("compute_payload_sha256"), False)
        compute_sidecar_sha256 = as_bool(job.get("compute_sidecar_sha256"), True)

        filename_contains = job.get("filename_contains")
        if filename_contains is not None:
            filename_contains = str(filename_contains)

        print(f"[databento] job={name} mode={mode} overwrite={overwrite}")

        try:
            if mode == "offline":
                raw_dir = job.get("raw_dir")
                if not raw_dir:
                    raise KeyError("offline mode requires job.raw_dir")

                raw_dir_p = Path(raw_dir).expanduser()
                if not raw_dir_p.is_absolute():
                    raw_dir_p = (REPO_ROOT / raw_dir_p).resolve()

                parquet_path, source_path = convert_databento_folder_auto_ingested(
                    raw_dir=raw_dir_p,
                    ingested_root=ingested_root / "databento",
                    filename_contains=filename_contains,
                    opts=opts,
                    overwrite=overwrite,
                    compute_payload_sha256=compute_payload_sha256,
                    compute_sidecar_sha256=compute_sidecar_sha256,
                )

                print(f"[databento]   OK parquet={parquet_path}")
                print(f"[databento]   OK source ={source_path}")

            else:
                # api mode
                job_api = job.get("api")
                if not isinstance(job_api, dict) or not job_api:
                    raise KeyError("api mode requires job.api mapping with dataset/schema/symbols/start/end")

                dataset = str(require(job_api, "dataset", f"job.api ({name})"))
                schema = str(require(job_api, "schema", f"job.api ({name})"))
                symbols = job_api.get("symbols")
                if not symbols:
                    raise KeyError("job.api.symbols is required")

                start = require(job_api, "start", f"job.api ({name})")
                end = job_api.get("end", None)

                # Allow per-job overrides for encoding/compression/limit
                encoding = str(job_api.get("encoding", api_encoding)).strip().lower()
                compression = str(job_api.get("compression", api_compression)).strip().lower()
                limit = job_api.get("limit", api_limit)

                # Choose a deterministic raw_dir for this job
                # Convention: data/raw/databento/<first_symbol>/<dataset_folder>/<job_name>/
                first_sym = symbols[0] if isinstance(symbols, list) and symbols else str(symbols)
                dataset_folder = f"{dataset.strip().lower().replace('.', '-')}-{schema.strip().lower()}"
                raw_dir_p = (raw_root / "databento" / str(first_sym).lower() / dataset_folder / name)
                raw_dir_p.mkdir(parents=True, exist_ok=True)

                # --- Cost preview / guardrails (API mode) ---
                # Source-level defaults can be overridden per job via job.api.*
                job_print_cost = as_bool(job_api.get("print_cost"), api_print_cost)
                job_require_confirm = as_bool(job_api.get("require_confirm"), api_require_confirm)
                job_max_cost_usd_d = as_decimal(job_api.get("max_cost_usd", api_max_cost_usd), default=Decimal("0"))

                est_cost = None
                if job_print_cost or job_require_confirm or job_max_cost_usd_d > Decimal("0"):
                    try:
                        import databento as db  # type: ignore

                        # Ensure the key is available (dotenv is loaded at script start).
                        client = db.Historical()
                        est_cost = client.metadata.get_cost(
                            dataset=dataset,
                            schema=schema,
                            symbols=symbols,
                            start=start,
                            end=end,
                        )

                        # Databento returns a numeric type; normalize to Decimal for comparisons.
                        est_cost_d = as_decimal(est_cost, default=Decimal("0"))

                        if job_print_cost:
                            print(f"[databento]   ESTIMATED_COST_USD={est_cost_d}")

                        if job_max_cost_usd_d > Decimal("0") and est_cost_d > job_max_cost_usd_d:
                            raise RuntimeError(
                                f"Estimated cost ${est_cost_d} exceeds max_cost_usd ${job_max_cost_usd_d}. "
                                "Lower the date range/limit or increase max_cost_usd."
                            )

                        if job_require_confirm:
                            confirm_or_abort(
                                f"[databento]   Proceed with API download (est. ${est_cost_d})? Type 'y' to continue: "
                            )

                    except Exception as ce:
                        # If we're enforcing confirmation or caps, treat cost-check failures as fatal.
                        if job_require_confirm or job_max_cost_usd_d > Decimal("0"):
                            raise
                        # Otherwise, just warn and continue.
                        print(f"[databento]   (warn) could not estimate cost: {ce}")

                raw_payload_path, parquet_path, source_path = download_and_convert_timeseries_to_ingested(
                    raw_dir=raw_dir_p,
                    ingested_root=ingested_root / "databento",
                    dataset=dataset,
                    schema=schema,
                    symbols=symbols,
                    start=start,
                    end=end,
                    encoding=encoding,
                    compression=compression,
                    limit=limit,
                    filename_contains=filename_contains,
                    opts=opts,
                    overwrite=overwrite,
                    compute_payload_sha256=compute_payload_sha256,
                    compute_sidecar_sha256=compute_sidecar_sha256,
                )

                print(f"[databento]   OK raw   ={raw_payload_path}")
                print(f"[databento]   OK parquet={parquet_path}")
                print(f"[databento]   OK source ={source_path}")

        except Exception as ex:
            msg = str(ex)
            # If outputs already exist and overwrite=False, skip instead of failing.
            if (not overwrite) and ("Refusing to overwrite existing file" in msg):
                print(f"[databento]   SKIP job={name}: already ingested (set overwrite: true to rebuild)")
                continue
            failures += 1
            eprint(f"[databento] ❗ERROR job={name}: {ex}")

    return 1 if failures else 0


# -----------------------------------------------------------------------------
# VIX handler
# -----------------------------------------------------------------------------


def run_vix(source_cfg: Dict[str, Any], defaults: Dict[str, Any], raw_root: Path, ingested_root: Path) -> int:
    """Run VIX ingestion.

    Currently supports offline only (FRED + CBOE local/raw fetch as implemented in src.ingest_vix).
    """

    try:
        from src.ingest_vix import ingest_vix_auto_ingested
    except Exception as ex:
        eprint("[vix] ❗ERROR: failed to import src.ingest_vix:", ex)
        return 2

    enabled = as_bool(source_cfg.get("enabled"), True)
    if not enabled:
        print("[vix] disabled; skipping")
        return 0

    mode = str(source_cfg.get("mode", "offline")).strip().lower()
    if mode != "offline":
        eprint(f"[vix] ❗ERROR: unsupported mode '{mode}' (expected offline)")
        return 2

    # Allow per-source overrides, fallback to defaults
    merged = dict(defaults)
    merged.update(source_cfg)

    overwrite = as_bool(merged.get("overwrite"), False)

    # Hashing toggle (some versions of src.ingest_vix expose this; others don't)
    compute_raw_sha256 = as_bool(merged.get("compute_raw_sha256"), True)

    print(f"[vix] mode={mode} overwrite={overwrite}")

    try:
        # Call with only the kwargs supported by the current function signature.
        sig = inspect.signature(ingest_vix_auto_ingested)
        kwargs: Dict[str, Any] = {}
        if "overwrite" in sig.parameters:
            kwargs["overwrite"] = overwrite
        if "compute_raw_sha256" in sig.parameters:
            kwargs["compute_raw_sha256"] = compute_raw_sha256
        res = ingest_vix_auto_ingested(**kwargs)
        # res is a mapping of providers -> paths
        for provider, paths in res.items():
            parquet = paths.get("parquet")
            source_json = paths.get("source_json")
            print(f"[vix]   OK provider={provider} parquet={parquet}")
            print(f"[vix]   OK provider={provider} source ={source_json}")
        return 0

    except Exception as ex:
        eprint("[vix] ❗ERROR:", ex)
        return 1


# -----------------------------------------------------------------------------
# Alpaca handler
# -----------------------------------------------------------------------------


def run_alpaca(source_cfg: Dict[str, Any], defaults: Dict[str, Any], raw_root: Path, ingested_root: Path) -> int:
    """Run Alpaca ingestion.

    Supports:
      - mode: offline -> ingest from existing local parquet (data/raw/alpaca/...)
      - mode: api     -> fetch via Alpaca Market Data API

    Jobs are required (multiple date ranges, symbols, etc.).
    """

    try:
        # New, symbol-generic entrypoint (preferred)
        from src.ingest_alpaca import ingest_stock_bars_auto_ingested as ingest_alpaca_auto_ingested
    except Exception:
        try:
            # Back-compat fallback (older name)
            from src.ingest_alpaca import ingest_spy_5m_auto_ingested as ingest_alpaca_auto_ingested
        except Exception as ex:
            eprint("[alpaca] ❗ERROR: failed to import src.ingest_alpaca:", ex)
            return 2

    enabled = as_bool(source_cfg.get("enabled"), True)
    if not enabled:
        print("[alpaca] disabled; skipping")
        return 0

    mode = str(source_cfg.get("mode", "offline")).strip().lower()
    if mode not in ("offline", "api"):
        eprint(f"[alpaca] ❗ERROR: unsupported mode '{mode}' (expected offline|api)")
        return 2

    jobs = source_cfg.get("jobs") or []
    if not isinstance(jobs, list):
        eprint("[alpaca] ❗ERROR: sources.alpaca.jobs must be a list")
        return 2

    if not jobs:
        print("[alpaca] no jobs; skipping")
        return 0

    failures = 0

    for job0 in jobs:
        if not isinstance(job0, dict):
            eprint("[alpaca] ❗ERROR: each job must be a mapping")
            failures += 1
            continue

        job = merge_job_defaults(defaults, job0)
        name = str(job.get("name") or "(unnamed)")
        overwrite = as_bool(job.get("overwrite"), False)
        save_raw_payload = as_bool(job.get("save_raw_payload"), True)
        compute_raw_sha256 = as_bool(job.get("compute_raw_sha256"), True)

        # Dates are required for Alpaca jobs
        start = require(job, "start", f"job ({name})")
        end = require(job, "end", f"job ({name})")

        # Symbol + timeframe are configurable per job. We default for backward compatibility,
        # but your ingest_alpaca.py should treat these as the authoritative values.
        symbol = str(job.get("symbol") or "SPY").strip().upper()
        timeframe = str(job.get("timeframe") or job.get("tf") or "5m").strip().lower()

        # Optional API feed selection (iex or sip). If omitted, src.ingest_alpaca.py can fall back to env/defaults.
        data_feed = job.get("data_feed")
        if data_feed is not None:
            data_feed = str(data_feed).strip().lower()

        existing_parquet = job.get("existing_parquet")
        existing_parquet_p: Optional[Path] = None
        if existing_parquet:
            existing_parquet_p = Path(str(existing_parquet)).expanduser()
            if not existing_parquet_p.is_absolute():
                existing_parquet_p = (REPO_ROOT / existing_parquet_p).resolve()

        print(f"[alpaca] job={name} mode={mode} symbol={symbol} timeframe={timeframe} overwrite={overwrite}")

        try:
            sig = inspect.signature(ingest_alpaca_auto_ingested)
            kwargs: Dict[str, Any] = {}

            # Common
            if "start" in sig.parameters:
                kwargs["start"] = start
            if "end" in sig.parameters:
                kwargs["end"] = end
            if "overwrite" in sig.parameters:
                kwargs["overwrite"] = overwrite
            if "compute_raw_sha256" in sig.parameters:
                kwargs["compute_raw_sha256"] = compute_raw_sha256

            # Symbol/timeframe (new generic ingester)
            if "symbol" in sig.parameters:
                kwargs["symbol"] = symbol
            if "timeframe" in sig.parameters:
                kwargs["timeframe"] = timeframe
            if "data_feed" in sig.parameters and data_feed is not None:
                kwargs["data_feed"] = data_feed

            if mode == "offline":
                if existing_parquet_p is None:
                    raise KeyError("offline mode requires job.existing_parquet")
                if "existing_parquet" in sig.parameters:
                    kwargs["existing_parquet"] = existing_parquet_p
                # Ensure we don't try to save API payload in offline mode
                if "save_raw_payload" in sig.parameters:
                    kwargs["save_raw_payload"] = False
            else:
                # api mode
                if "existing_parquet" in sig.parameters:
                    kwargs["existing_parquet"] = None
                if "save_raw_payload" in sig.parameters:
                    kwargs["save_raw_payload"] = save_raw_payload

            res = ingest_alpaca_auto_ingested(**kwargs)

            parquet = res.get("parquet")
            source_json = res.get("source_json")
            raw_payload = res.get("raw_payload")

            if raw_payload:
                print(f"[alpaca]   OK raw   ={raw_payload}")
            print(f"[alpaca]   OK parquet={parquet}")
            print(f"[alpaca]   OK source ={source_json}")

        except Exception as ex:
            failures += 1
            eprint(f"[alpaca] ❗ERROR job={name}: {ex}")

    return 1 if failures else 0


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------


HANDLERS = {
    "databento": run_databento,
    "vix": run_vix,
    "alpaca": run_alpaca,
}


def main(argv: List[str]) -> int:
    cfg_path = REPO_ROOT / "ingest.yaml"

    try:
        ensure_running_in_venv()
    except Exception as ex:
        eprint("[ingest] ❗ERROR:", ex)
        return 2

    try:
        cfg = load_yaml(cfg_path)
        version = int(cfg.get("version", 0))
        if version != 1:
            raise ValueError(f"Unsupported ingest.yaml version: {version} (expected 1)")

        defaults = cfg.get("defaults") or {}
        if not isinstance(defaults, dict):
            raise ValueError("defaults must be a mapping")

        paths_cfg = cfg.get("paths") or {}
        if not isinstance(paths_cfg, dict):
            raise ValueError("paths must be a mapping")

        raw_root, ingested_root = normalize_paths(paths_cfg)
        raw_root.mkdir(parents=True, exist_ok=True)
        ingested_root.mkdir(parents=True, exist_ok=True)

        sources = cfg.get("sources")
        if not isinstance(sources, dict) or not sources:
            raise ValueError("sources must be a non-empty mapping")

    except Exception as ex:
        eprint("[ingest] ❗ERROR:", ex)
        return 2

    print(f"[ingest] repo_root={REPO_ROOT}")
    print(f"[ingest] raw_root={raw_root}")
    print(f"[ingest] ingested_root={ingested_root}")

    failures = 0

    for source_name, source_cfg in sources.items():
        if not isinstance(source_cfg, dict):
            eprint(f"[ingest] ❗ERROR: sources.{source_name} must be a mapping")
            failures += 1
            continue

        # `enabled` is the most authoritative switch: if false, skip this source entirely.
        if not as_bool(source_cfg.get("enabled"), True):
            print(f"[ingest] skipping source={source_name} (enabled: false)")
            continue

        # Global rule: source.mode selects which jobs run for this vendor.
        source_cfg = filter_jobs_by_source_mode(source_name, source_cfg)

        handler = HANDLERS.get(source_name)
        if handler is None:
            print(f"[ingest] skipping unknown source '{source_name}' (no handler yet)")
            continue

        print(f"[ingest] running source={source_name}")
        rc = handler(source_cfg, defaults, raw_root, ingested_root)
        if rc != 0:
            failures += 1

    if failures:
        eprint(f"[ingest] 👋completed with failures={failures}")
        return 1

    print("[ingest] completed successfully✅")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
