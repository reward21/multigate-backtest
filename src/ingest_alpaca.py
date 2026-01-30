from __future__ import annotations

from datetime import date, datetime, timezone, timedelta, time as dtime
from pathlib import Path
import os
import requests
import pandas as pd

from .config import alpaca_env


def _to_utc_iso(dt: datetime) -> str:
    # Alpaca expects RFC3339-ish; Z is fine
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def fetch_spy_5m_alpaca(start: date, end: date, symbol: str = "SPY") -> pd.DataFrame:
    """
    Fetch SPY 5-minute bars from Alpaca Market Data API.

    Key fix for 403:
      - Explicitly set feed=iex (free) unless user overrides via ALPACA_DATA_FEED
      - Optionally cap end to avoid requesting bars too close to "now" for some plans
    """
    env = alpaca_env()
    base_url = env["base_url"].rstrip("/")
    key_id = env["key_id"]
    secret = env["secret_key"]

    if not key_id or not secret:
        raise RuntimeError(
            "Missing Alpaca API keys. Set:\n"
            "  export ALPACA_API_KEY_ID=...\n"
            "  export ALPACA_API_SECRET_KEY=...\n"
        )

    # Use IEX by default (free). SIP generally requires a paid market-data subscription.
    feed = os.getenv("ALPACA_DATA_FEED", "iex").lower().strip()  # "iex" or "sip"

    # Convert date -> datetime range
    start_dt = datetime.combine(start, dtime(0, 0, 0), tzinfo=timezone.utc)
    end_dt = datetime.combine(end, dtime(23, 59, 59), tzinfo=timezone.utc)

    # Optional: avoid "too recent" bars on some plans by lagging end behind now.
    lag_min = int(os.getenv("ALPACA_END_LAG_MIN", "20"))
    cap_dt = datetime.now(timezone.utc) - timedelta(minutes=lag_min)
    if end_dt > cap_dt:
        end_dt = cap_dt

    url = f"{base_url}/v2/stocks/bars"
    headers = {
        "APCA-API-KEY-ID": key_id,
        "APCA-API-SECRET-KEY": secret,
    }
    params = {
        "symbols": symbol,
        "timeframe": "5Min",
        "start": _to_utc_iso(start_dt),
        "end": _to_utc_iso(end_dt),
        "limit": 10000,
        "adjustment": "raw",
        "feed": feed,  # <-- critical
    }

    resp = requests.get(url, headers=headers, params=params, timeout=60)
    resp.raise_for_status()

    payload = resp.json()
    bars = payload.get("bars", {}).get(symbol, []) or payload.get("bars", [])  # tolerate both shapes
    df = pd.DataFrame(bars)
    if df.empty:
        return df

    # Normalize column names
    # Alpaca commonly returns: t (timestamp), o/h/l/c, v, n, vw
    # We'll standardize minimally.
    if "t" in df.columns:
        df["t"] = pd.to_datetime(df["t"], utc=True)
        df = df.rename(columns={"t": "ts"})
    return df


def ingest_spy_5m_to_parquet(start: date, end: date, out_path: Path) -> Path:
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df = fetch_spy_5m_alpaca(start, end, symbol="SPY")
    if df.empty:
        raise RuntimeError("Alpaca returned 0 bars for SPY in requested range.")
    df.to_parquet(out_path, index=False)
    return out_path