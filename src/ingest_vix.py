#!/usr/bin/env python3
"""
ingest_vix

VIX ingestion utilities.

Goal:
- Read VIX daily data from local CSV sources (FRED primary; CBOE optional)
- Normalize to a consistent schema:
    date (datetime64[ns]), vix_close (float)
- Write parquet caches (pyarrow/fastparquet via pandas.to_parquet)

This module is intentionally defensive:
- Handles common FRED column names: observation_date / DATE
- Handles common CBOE column names: Date / DATE and Close / CLOSE / "VIX Close"
- Handles weird encodings and occasional malformed rows
- Tries to recover if a CSV accidentally contains extra header lines or HTML
"""

from __future__ import annotations

from io import StringIO
from pathlib import Path
from typing import Optional, Tuple, Union, Iterable

import pandas as pd


PathLike = Union[str, Path]


# ---------------------------
# Low-level CSV read helpers
# ---------------------------

def _decode_bytes_best_effort(raw: bytes) -> str:
    """
    Decode bytes into text without throwing. Prefer UTF-8 variants, fall back to cp1252/latin1.
    """
    for enc in ("utf-8", "utf-8-sig", "cp1252", "latin1"):
        try:
            return raw.decode(enc)
        except UnicodeDecodeError:
            continue
    # final never-throw path
    return raw.decode("latin1", errors="replace")


def _trim_to_probable_header(text: str, header_candidates: Iterable[str]) -> str:
    """
    If the file has junk before the real CSV header (extra lines, HTML, etc),
    attempt to trim to the first line that looks like a real header.
    """
    lower = text.lower()
    best_idx = None
    for h in header_candidates:
        i = lower.find(h.lower())
        if i != -1:
            best_idx = i if best_idx is None else min(best_idx, i)
    if best_idx is None:
        return text
    return text[best_idx:]


def _read_csv_best_effort(csv_path: Path) -> pd.DataFrame:
    """
    Read a CSV using multiple fallbacks:
    - pandas fast path
    - alternate encodings
    - python engine with on_bad_lines
    - bytes->decode->StringIO parse (never throws UnicodeDecodeError)
    """
    # First: try normal read (fast)
    try:
        return pd.read_csv(csv_path)
    except Exception:
        pass

    # Second: try common encodings
    for enc in ("utf-8", "utf-8-sig", "cp1252", "latin1"):
        try:
            return pd.read_csv(csv_path, encoding=enc)
        except Exception:
            continue

    # Third: python engine with bad-line skipping (still may raise UnicodeDecodeError)
    for enc in ("utf-8", "utf-8-sig", "cp1252", "latin1"):
        try:
            return pd.read_csv(csv_path, engine="python", encoding=enc, on_bad_lines="skip")
        except Exception:
            continue

    # Final: read bytes, decode safely, trim, then parse from memory
    raw = csv_path.read_bytes()
    text = _decode_bytes_best_effort(raw)

    # If it's HTML (common when a download failed), surface that clearly
    if "<html" in text.lower() or "<!doctype html" in text.lower():
        raise ValueError(
            f"{csv_path} looks like HTML, not CSV (download likely failed). "
            f"Open the file and confirm it starts with a CSV header."
        )

    # Trim to likely header for FRED/CBOE
    text = _trim_to_probable_header(
        text,
        header_candidates=("observation_date", "date", "Date"),
    )

    try:
        return pd.read_csv(StringIO(text), engine="python", on_bad_lines="skip")
    except TypeError:
        # Older pandas
        return pd.read_csv(StringIO(text), engine="python", error_bad_lines=False, warn_bad_lines=True)


# ---------------------------
# Normalization helpers
# ---------------------------

def _pick_col(df: pd.DataFrame, candidates: Iterable[str]) -> Optional[str]:
    cols = {c.lower(): c for c in df.columns}
    for cand in candidates:
        if cand.lower() in cols:
            return cols[cand.lower()]
    return None


def _normalize_vix_df(
    df: pd.DataFrame,
    *,
    source: str,
    date_candidates: Iterable[str],
    value_candidates: Iterable[str],
) -> pd.DataFrame:
    if df is None or df.empty:
        raise ValueError(f"{source}: CSV loaded but is empty")

    date_col = _pick_col(df, date_candidates)
    if not date_col:
        raise ValueError(f"{source}: missing date column. Have columns: {list(df.columns)}")

    value_col = _pick_col(df, value_candidates)
    if not value_col:
        # Fallback: pick first non-date column
        other_cols = [c for c in df.columns if c != date_col]
        if not other_cols:
            raise ValueError(f"{source}: no value columns found. Have columns: {list(df.columns)}")
        value_col = other_cols[0]

    out = df[[date_col, value_col]].copy()
    out.columns = ["date", "vix_close"]

    out["date"] = pd.to_datetime(out["date"], errors="coerce", utc=False)
    out["vix_close"] = pd.to_numeric(out["vix_close"], errors="coerce")

    out = out.dropna(subset=["date", "vix_close"])
    out = out.sort_values("date").drop_duplicates(subset=["date"], keep="last").reset_index(drop=True)

    # Ensure naive dates (daily series)
    out["date"] = out["date"].dt.tz_localize(None)

    return out


def ensure_parquet_path(p: PathLike) -> Path:
    p = Path(p)
    if p.suffix.lower() != ".parquet":
        return p.with_suffix(".parquet")
    return p


def write_parquet(df: pd.DataFrame, out_path: PathLike) -> Path:
    out = ensure_parquet_path(out_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out, index=False)
    return out


# ---------------------------
# Public loaders
# ---------------------------

def load_fred_vix_csv(csv_path: PathLike) -> pd.DataFrame:
    """
    FRED VIX series (VIXCLS) typically comes as:
      observation_date,VIXCLS
      1990-01-02,17.24

    Some exports may use DATE instead of observation_date.
    """
    csv_path = Path(csv_path)
    df = _read_csv_best_effort(csv_path)

    return _normalize_vix_df(
        df,
        source=f"FRED({csv_path.name})",
        date_candidates=("observation_date", "DATE", "date"),
        value_candidates=("VIXCLS", "vixcls", "Close", "CLOSE", "value"),
    )


def load_cboe_vix_file(csv_path: PathLike) -> pd.DataFrame:
    """
    CBOE VIX historical downloads vary. Common patterns include:
      Date, VIX Close
      DATE, CLOSE
      Date, Close
    """
    csv_path = Path(csv_path)
    df = _read_csv_best_effort(csv_path)

    return _normalize_vix_df(
        df,
        source=f"CBOE({csv_path.name})",
        date_candidates=("Date", "DATE", "date"),
        value_candidates=("VIX Close", "VIX_Close", "Close", "CLOSE", "Last", "LAST"),
    )


# ---------------------------
# Main ingestion entrypoints
# ---------------------------

def ingest_vix_from_local(
    fred_csv: PathLike,
    cboe_csv: Optional[PathLike] = None,
) -> Tuple[pd.DataFrame, Optional[pd.DataFrame]]:
    fred_df = load_fred_vix_csv(fred_csv)
    cboe_df = load_cboe_vix_file(cboe_csv) if cboe_csv else None
    return fred_df, cboe_df


def ingest_vix_to_parquet(
    *,
    fred_csv: PathLike,
    cboe_csv: Optional[PathLike] = None,
    out_fred_parquet: PathLike,
    out_cboe_parquet: Optional[PathLike] = None,
) -> Tuple[Path, Optional[Path]]:
    """
    Load VIX daily CSVs and write parquet caches.

    Returns:
        (fred_parquet_path, cboe_parquet_path or None)
    """
    fred_df = load_fred_vix_csv(fred_csv)
    fred_out = write_parquet(fred_df, out_fred_parquet)

    cboe_out: Optional[Path] = None
    if cboe_csv and out_cboe_parquet:
        cboe_df = load_cboe_vix_file(cboe_csv)
        cboe_out = write_parquet(cboe_df, out_cboe_parquet)

    return fred_out, cboe_out