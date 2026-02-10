from __future__ import annotations

from datetime import date, datetime, timezone, timedelta, time as dtime
from pathlib import Path
import os
import requests
import pandas as pd
import json
from hashlib import sha256
from typing import Any, Dict, Optional, Tuple

from dotenv import load_dotenv
def _alpaca_env_from_dotenv() -> Dict[str, str]:
    """Load Alpaca credentials from repo-root .env (if present) and environment variables.

    Expected env vars (either in .env or exported in the shell):
      - ALPACA_BASE_URL (optional; defaults to https://data.alpaca.markets)
      - ALPACA_API_KEY_ID
      - ALPACA_API_SECRET_KEY

    Note: load_dotenv will NOT override already-exported environment variables by default.
    """
    repo_root = Path(__file__).resolve().parents[1]
    load_dotenv(repo_root / ".env")

    base_url = os.getenv("ALPACA_BASE_URL", os.getenv("APCA_API_BASE_URL", "https://data.alpaca.markets"))
    # Support both Alpaca docs-style and legacy APCA_* naming
    key_id = os.getenv("ALPACA_API_KEY_ID", os.getenv("APCA_API_KEY_ID", os.getenv("APCA_API_KEY_ID", "")))
    secret_key = os.getenv(
        "ALPACA_API_SECRET_KEY",
        os.getenv("APCA_API_SECRET_KEY", os.getenv("APCA_API_SECRET_KEY", "")),
    )

    return {
        "base_url": (base_url or "").strip(),
        "key_id": (key_id or "").strip(),
        "secret_key": (secret_key or "").strip(),
    }


def _sha256_file(p: Path) -> str:
    h = sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _write_json(p: Path, obj: Any) -> Path:
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(obj, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return p


def _to_ymd(d: date) -> str:
    return d.strftime("%Y%m%d")


def _to_utc_iso(dt: datetime) -> str:
    """Format a timezone-aware datetime as an RFC3339/ISO-8601 UTC string.

    Alpaca accepts timestamps like `2024-01-01T00:00:00Z`.
    """
    if dt.tzinfo is None:
        # Treat naive datetimes as UTC to avoid accidental local-time bugs.
        dt = dt.replace(tzinfo=timezone.utc)
    dt_utc = dt.astimezone(timezone.utc)
    # Use `Z` suffix for UTC.
    return dt_utc.isoformat().replace("+00:00", "Z")


def _normalize_symbol(sym: str) -> str:
    return (sym or "").strip().upper()


def _normalize_timeframe(timeframe: str) -> str:
    """Normalize timeframe into an Alpaca-compatible canonical string.

    We preserve Alpaca-style granularity strings (e.g., '5min', '1min', '1hour', '1day')
    for printing and for folder/file naming.

    Accepts common aliases like '5m' and '5Min' and normalizes to lowercase.
    """
    tf = (timeframe or "").strip()
    if not tf:
        return ""

    lower = tf.lower()

    # Minute aliases
    if lower.endswith("min"):
        n = lower[:-3].strip()
        return f"{n}min" if n else "min"
    if lower.endswith("m") and lower[:-1].strip().isdigit():
        return f"{lower[:-1].strip()}min"

    # Hour aliases
    if lower.endswith("hour"):
        n = lower[:-4].strip()
        return f"{n}hour" if n else "hour"
    if lower.endswith("h") and lower[:-1].strip().isdigit():
        return f"{lower[:-1].strip()}hour"

    # Day aliases
    if lower.endswith("day"):
        n = lower[:-3].strip()
        return f"{n}day" if n else "day"
    if lower.endswith("d") and lower[:-1].strip().isdigit():
        return f"{lower[:-1].strip()}day"

    # Fallback: just lowercase (lets advanced Alpaca values pass through)
    return lower


def _schema_from_timeframe(timeframe: str) -> str:
    """Map Alpaca timeframe strings to our canonical schema naming.

    IMPORTANT: For Alpaca we keep the timeframe token as-is (normalized) so that
    folder/file names match the API timeframe (e.g., '5min' -> 'ohlcv-5min').
    """
    tf = _normalize_timeframe(timeframe)
    if not tf:
        return "ohlcv"
    return f"ohlcv-{tf}"


def plan_ingested_alpaca_paths(
    *,
    ingested_root: Path = Path("data/ingested"),
    symbol: str,
    feed: str,
    schema: str = "ohlcv-5min",
) -> Tuple[Path, Path]:
    """Plan canonical ingested output paths for Alpaca.

    Canonical layout:
        <ingested_root>/alpaca/<symbol>/<feed>-<schema>/

    Files:
        <symbol>-alpaca-<feed>-<schema>.parquet
        <symbol>-alpaca-<feed>-<schema>.source.json
    """
    sym = symbol.lower()
    folder = Path(ingested_root).expanduser().resolve() / "alpaca" / sym / f"{feed.lower()}-{schema}"
    base = f"{sym}-alpaca-{feed.lower()}-{schema}"
    return folder / f"{base}.parquet", folder / f"{base}.source.json"


def plan_raw_alpaca_capture_path(
    *,
    raw_root: Path = Path("data/raw/alpaca"),
    symbol: str,
    feed: str,
    schema: str,
    start: date,
    end: date,
) -> Path:
    """Where to store the raw Alpaca API JSON response for reproducibility."""
    sym = symbol.lower()
    folder = Path(raw_root).expanduser().resolve() / sym / f"{feed.lower()}-{schema}"
    name = f"{sym}-{feed.lower()}-{schema}-{_to_ymd(start)}-{_to_ymd(end)}.json"
    return folder / name


def fetch_stock_bars_alpaca_with_payload(
    start: date,
    end: date,
    *,
    symbol: str,
    timeframe: str = "5Min",
    feed: Optional[str] = None,
    limit: int = 10000,
) -> Tuple[pd.DataFrame, Dict[str, Any], Dict[str, Any]]:
    """Fetch stock bars and also return the raw JSON payload and the final request params used."""
    env = _alpaca_env_from_dotenv()
    base_url = env["base_url"].rstrip("/")
    key_id = env["key_id"].strip()
    secret = env["secret_key"].strip()

    if not key_id or not secret:
        raise RuntimeError(
            "Missing Alpaca API keys. Set:\n"
            "  export ALPACA_API_KEY_ID=...\n"
            "  export ALPACA_API_SECRET_KEY=...\n"
        )

    feed_final = (feed or os.getenv("ALPACA_DATA_FEED", "iex")).lower().strip()
    sym = _normalize_symbol(symbol)

    start_dt = datetime.combine(start, dtime(0, 0, 0), tzinfo=timezone.utc)
    end_dt = datetime.combine(end, dtime(23, 59, 59), tzinfo=timezone.utc)

    lag_min = int(os.getenv("ALPACA_END_LAG_MIN", "20"))
    cap_dt = datetime.now(timezone.utc) - timedelta(minutes=lag_min)
    if end_dt > cap_dt:
        end_dt = cap_dt

    url = f"{base_url}/v2/stocks/bars"
    headers = {"APCA-API-KEY-ID": key_id, "APCA-API-SECRET-KEY": secret}
    params: Dict[str, Any] = {
        "symbols": sym,
        "timeframe": _normalize_timeframe(timeframe) or timeframe,
        "start": _to_utc_iso(start_dt),
        "end": _to_utc_iso(end_dt),
        "limit": int(limit),
        "adjustment": "raw",
        "feed": feed_final,
    }

    resp = requests.get(url, headers=headers, params=params, timeout=60)
    resp.raise_for_status()

    payload: Dict[str, Any] = resp.json()

    # Normal payload shape: {"bars": {"SYM": [ ... ]}}
    bars = (payload.get("bars") or {}).get(sym)
    if bars is None:
        # Tolerate alternate shapes
        bars = payload.get("bars", [])
    bars = bars or []

    df = pd.DataFrame(bars)
    if df.empty:
        return df, payload, {"url": url, "params": params, "feed": feed_final, "lag_min": lag_min}

    if "t" in df.columns:
        df["t"] = pd.to_datetime(df["t"], utc=True)
        df = df.rename(columns={"t": "ts"})

    return df, payload, {"url": url, "params": params, "feed": feed_final, "lag_min": lag_min}


def fetch_5m_alpaca_with_payload(
    start: date,
    end: date,
    *,
    symbol: str,
    feed: Optional[str] = None,
) -> Tuple[pd.DataFrame, Dict[str, Any], Dict[str, Any]]:
    """Convenience wrapper for 5-minute stock bars."""
    return fetch_stock_bars_alpaca_with_payload(start, end, symbol=symbol, timeframe="5min", feed=feed)


def fetch_5m_alpaca(start: date, end: date, *, symbol: str, feed: Optional[str] = None) -> pd.DataFrame:
    df, _payload, _req = fetch_5m_alpaca_with_payload(start, end, symbol=symbol, feed=feed)
    return df


def ingest_ohlcv_from_existing_parquet(
    *,
    existing_parquet: Path,
    symbol: str,
    feed: str = "offline",
    schema: str = "ohlcv",
    ingested_root: Path = Path("data/ingested"),
    overwrite: bool = False,
    compute_input_sha256: bool = True,
) -> Dict[str, str]:
    """Offline plumbing test: use a local parquet as the 'raw input' and publish it into data/ingested.

    This does NOT call the Alpaca API.
    """
    existing_parquet = Path(existing_parquet).expanduser().resolve()
    if not existing_parquet.exists():
        raise FileNotFoundError(f"existing_parquet not found: {existing_parquet}")

    parquet_path, source_path = plan_ingested_alpaca_paths(
        ingested_root=ingested_root,
        symbol=symbol,
        feed=feed,
        schema=schema,
    )

    parquet_path.parent.mkdir(parents=True, exist_ok=True)
    if parquet_path.exists() and not overwrite:
        return {"parquet": str(parquet_path), "source_json": str(source_path)}

    # Copy bytes (fast) rather than read/write via pandas.
    parquet_path.write_bytes(existing_parquet.read_bytes())

    in_sha = _sha256_file(existing_parquet) if compute_input_sha256 else None

    out_meta = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "provider": "alpaca",
        "mode": "offline_existing_parquet",
        "symbol": symbol.lower(),
        "feed": feed,
        "schema": schema,
        "inputs": {
            "existing_parquet": str(existing_parquet),
            "existing_parquet_sha256": in_sha,
        },
        "output": {
            "parquet": str(parquet_path),
        },
    }

    source_path.parent.mkdir(parents=True, exist_ok=True)
    source_path.write_text(json.dumps(out_meta, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    return {"parquet": str(parquet_path), "source_json": str(source_path)}


def ingest_stock_bars_auto_ingested(
    *,
    start: date,
    end: date,
    symbol: str,
    timeframe: str = "5min",
    feed: Optional[str] = None,
    raw_root: Path = Path("data/raw/alpaca"),
    ingested_root: Path = Path("data/ingested"),
    overwrite: bool = False,
    save_raw_payload: bool = True,
    compute_raw_sha256: bool = True,
    existing_parquet: Optional[Path] = None,
) -> Dict[str, str]:
    """Fetch stock bars (API) or ingest existing parquet (offline) and write canonical ingested parquet + source.json.

    Writes:
      - data/ingested/alpaca/<symbol>/<feed>-<schema>/<symbol>-alpaca-<feed>-<schema>.parquet
      - matching .source.json

    Optionally stores the raw API JSON response under:
      - data/raw/alpaca/<symbol>/<feed>-<schema>/<symbol>-<feed>-<schema>-YYYYMMDD-YYYYMMDD.json
    """
    if existing_parquet is not None:
        schema = _schema_from_timeframe(timeframe)
        return ingest_ohlcv_from_existing_parquet(
            existing_parquet=existing_parquet,
            symbol=symbol,
            feed="offline",
            schema=schema,
            ingested_root=ingested_root,
            overwrite=overwrite,
            compute_input_sha256=True,
        )

    df, payload, req = fetch_stock_bars_alpaca_with_payload(
        start,
        end,
        symbol=symbol,
        timeframe=timeframe,
        feed=feed,
    )
    if df.empty:
        raise RuntimeError(f"Alpaca returned 0 bars for { _normalize_symbol(symbol) } in requested range.")

    feed_final = str(req.get("feed", feed or os.getenv("ALPACA_DATA_FEED", "iex"))).lower().strip()
    schema = _schema_from_timeframe(timeframe)

    parquet_path, source_path = plan_ingested_alpaca_paths(
        ingested_root=ingested_root,
        symbol=symbol,
        feed=feed_final,
        schema=schema,
    )

    parquet_path.parent.mkdir(parents=True, exist_ok=True)

    if parquet_path.exists() and not overwrite:
        return {"parquet": str(parquet_path), "source_json": str(source_path)}

    raw_capture_path: Optional[Path] = None
    raw_sha: Optional[str] = None
    if save_raw_payload:
        raw_capture_path = plan_raw_alpaca_capture_path(
            raw_root=raw_root,
            symbol=symbol,
            feed=feed_final,
            schema=schema,
            start=start,
            end=end,
        )
        _write_json(raw_capture_path, payload)
        if compute_raw_sha256:
            raw_sha = _sha256_file(raw_capture_path)

    df.to_parquet(parquet_path, index=False)

    out_meta = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "provider": "alpaca",
        "symbol": symbol.lower(),
        "feed": feed_final,
        "schema": schema,
        "request": req,
        "inputs": {
            "raw_payload_json": str(raw_capture_path) if raw_capture_path else None,
            "raw_payload_sha256": raw_sha,
        },
        "output": {
            "parquet": str(parquet_path),
            "rows": int(len(df)),
            "min_ts": df["ts"].min().isoformat() if "ts" in df.columns and len(df) else None,
            "max_ts": df["ts"].max().isoformat() if "ts" in df.columns and len(df) else None,
            "columns": list(df.columns),
        },
    }

    source_path.parent.mkdir(parents=True, exist_ok=True)
    source_path.write_text(json.dumps(out_meta, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    return {"parquet": str(parquet_path), "source_json": str(source_path)}