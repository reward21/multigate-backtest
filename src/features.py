import numpy as np
import pandas as pd


# ---------------------------
# Column normalization helpers
# ---------------------------

def _normalize_ohlcv_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize common OHLCV column names to:
      open, high, low, close, volume

    Handles Alpaca-style short keys:
      o,h,l,c,v  (and sometimes 't' for timestamp)
    Also lowercases all column names.
    """
    out = df.copy()
    out.columns = [str(c).strip() for c in out.columns]

    # lowercase for matching
    lower_map = {c: c.lower() for c in out.columns}
    out = out.rename(columns=lower_map)

    rename = {}
    cols = set(out.columns)

    # Alpaca bar keys
    if "c" in cols and "close" not in cols:
        rename["c"] = "close"
    if "o" in cols and "open" not in cols:
        rename["o"] = "open"
    if "h" in cols and "high" not in cols:
        rename["h"] = "high"
    if "l" in cols and "low" not in cols:
        rename["l"] = "low"
    if "v" in cols and "volume" not in cols:
        rename["v"] = "volume"

    # Other common variations
    if "adj close" in cols and "close" not in cols:
        rename["adj close"] = "close"
    if "vol" in cols and "volume" not in cols:
        rename["vol"] = "volume"

    out = out.rename(columns=rename)
    return out


def ensure_datetime_index(df: pd.DataFrame) -> pd.DataFrame:
    """Return df with a tz-aware UTC DatetimeIndex named 'ts'."""
    out = df.copy()

    # Case 1: already a DatetimeIndex
    if isinstance(out.index, pd.DatetimeIndex):
        idx = out.index
        if idx.tz is None:
            idx = idx.tz_localize("UTC")
        else:
            idx = idx.tz_convert("UTC")
        out.index = idx
        out.index.name = "ts"
        out = out[~out.index.isna()].sort_index()
        return out

    # Case 2: find a datetime-like column
    candidates = (
        "ts", "timestamp", "t", "time", "datetime", "date", "Date", "DATE", "observation_date"
    )
    dt_col = next((c for c in candidates if c in out.columns), None)
    if dt_col is None:
        raise ValueError(
            f"ensure_datetime_index: no datetime index/column found. "
            f"Index={type(out.index).__name__}, columns={list(out.columns)}"
        )

    idx = pd.to_datetime(out[dt_col], errors="coerce", utc=True)
    out = out.loc[~idx.isna()].copy()
    out.index = idx.loc[~idx.isna()]
    out.index = out.index.tz_convert("UTC")
    out.index.name = "ts"
    return out.sort_index()


# ---------------------------
# Indicators
# ---------------------------

def compute_vwap_intraday(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # must have close + volume at this point
    required = ("close", "volume")
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise KeyError(f"VWAP missing columns {missing}. Have columns: {list(df.columns)}")

    # ensure index is tz-aware before calling this
    df["date_et"] = df.index.tz_convert("America/New_York").normalize().tz_localize(None)

    df["pv"] = df["close"] * df["volume"]
    df["cum_pv"] = df.groupby("date_et")["pv"].cumsum()
    df["cum_v"] = df.groupby("date_et")["volume"].cumsum()
    df["vwap"] = df["cum_pv"] / df["cum_v"]
    return df


def add_indicators(spy_df: pd.DataFrame) -> pd.DataFrame:
    df = ensure_datetime_index(spy_df)
    df = _normalize_ohlcv_columns(df)

    # VWAP (intraday)
    df = compute_vwap_intraday(df)

    return df


# ---------------------------
# VIX join + regime
# ---------------------------

def join_vix_regime(spy_df: pd.DataFrame, vix_df: pd.DataFrame) -> pd.DataFrame:
    """Join a daily VIX series onto intraday SPY bars and compute a simple regime bucket."""
    spy = ensure_datetime_index(spy_df).copy()
    spy = _normalize_ohlcv_columns(spy)

    # Normalize SPY join key: midnight ET, timezone-naive
    spy["date_et"] = spy.index.tz_convert("America/New_York").normalize().tz_localize(None)

    v = vix_df.copy()

    # Ensure VIX has a daily date column or datetime index
    if isinstance(v.index, pd.DatetimeIndex):
        vidx = v.index
        if vidx.tz is None:
            vidx = vidx.tz_localize("UTC")
        else:
            vidx = vidx.tz_convert("UTC")
        v["_ts"] = vidx
    else:
        date_candidates = ("date", "Date", "DATE", "observation_date", "ts", "timestamp", "t", "time")
        dcol = next((c for c in date_candidates if c in v.columns), None)
        if dcol is None:
            raise ValueError(f"VIX df missing date column/index. Columns: {list(v.columns)}")
        v["_ts"] = pd.to_datetime(v[dcol], errors="coerce", utc=True)

    v = v.loc[~v["_ts"].isna()].copy()
    v["date_et"] = v["_ts"].dt.tz_convert("America/New_York").dt.normalize().dt.tz_localize(None)

    # Pick VIX value column
    value_candidates = ("vix_close", "VIXCLS", "vixcls", "Close", "CLOSE", "Last", "LAST", "value")
    vcol = next((c for c in value_candidates if c in v.columns), None)
    if vcol is None:
        raise ValueError(f"VIX df missing value column. Columns: {list(v.columns)}")

    v2 = (
        v[["date_et", vcol]]
        .rename(columns={vcol: "vix_close"})
        .groupby("date_et", as_index=False)
        .last()
        .sort_values("date_et")
    )

    out = spy.join(v2.set_index("date_et"), on="date_et", how="left")
    out["vix_close"] = pd.to_numeric(out["vix_close"], errors="coerce")

    # Simple regime buckets
    if out["vix_close"].notna().any():
        q1 = out["vix_close"].quantile(0.33)
        q2 = out["vix_close"].quantile(0.66)
        out["vix_regime"] = pd.cut(
            out["vix_close"],
            bins=[-1e9, q1, q2, 1e9],
            labels=["LOW", "NORMAL", "HIGH"],
            include_lowest=True,
        )
    else:
        out["vix_regime"] = "NORMAL"

    return out