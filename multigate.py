# multigate.py
from __future__ import annotations
from src.config import get_config, paths_from_config
from src.viz import write_equity_curve_png
from src.reporting import write_run_report_md


import json
import sqlite3
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd


# -------------------------
# Config (config.yaml in root folder)
# -------------------------

HERE = Path(__file__).resolve().parent

cfg = get_config()  # reads config.yaml using src/config.py defaults
paths = paths_from_config(cfg)

DATA_PATH = paths.data_path
DB_PATH = paths.db_path
GATES_DIR = paths.gates_dir

# optional: move these into config.yaml later; keep defaults for now
GATE_IDS = cfg.get("gates", {}).get("gate_ids", ["G0", "G1", "G2", "G3", "G4", "G5"])

start_eq = cfg.get("runtime", {}).get("start_equity", None)
if start_eq is None:
    raise ValueError("Missing runtime.start_equity in config.yaml")

s = str(start_eq).strip()
s = s.replace("$", "").replace(",", "")  # allow $ and commas
START_EQUITY = float(s)                  # decimals still OK


SPEC_VERSION = cfg.get("gates", {}).get("spec_version", "RISK_GATE_MULTI_GATES_G0-G5_v1_2026-01-29")
STRATEGY_VERSION = cfg.get("runtime", {}).get("strategy_version", "MULTIGATE_BASELINE_v1")

# -------------------------
# Utilities
# -------------------------

def utc_run_id() -> str:
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    return f"{ts}_{uuid.uuid4().hex[:8]}"


def read_table_auto(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Data file not found: {path}")
    suf = path.suffix.lower()
    if suf == ".parquet":
        return pd.read_parquet(path)
    if suf == ".csv":
        return pd.read_csv(path)
    raise ValueError(f"Unsupported data format: {path} (use .parquet or .csv)")


def normalize_spy_5m(df: pd.DataFrame) -> pd.DataFrame:
    # normalize column names
    df = df.copy()
    df.columns = [str(c).strip().lower() for c in df.columns]

    # common timestamp column names
    ts_candidates = ["ts", "timestamp", "time", "datetime", "date"]
    ts_col = next((c for c in ts_candidates if c in df.columns), None)

    if ts_col is None and isinstance(df.index, pd.DatetimeIndex):
        out = df
    elif ts_col is not None:
        out = df
        out[ts_col] = pd.to_datetime(out[ts_col], errors="coerce", utc=True)
        out = out.dropna(subset=[ts_col])
        out = out.set_index(ts_col)
    else:
        raise ValueError(f"Could not find timestamp column. Have: {list(df.columns)}")

    # enforce datetime index
    if not isinstance(out.index, pd.DatetimeIndex):
        raise ValueError("Expected DatetimeIndex after normalization.")

    # standard price/volume names
    rename_map = {}
    if "close" not in out.columns:
        for alt in ("c", "close_price", "last"):
            if alt in out.columns:
                rename_map[alt] = "close"
                break
    if "open" not in out.columns:
        for alt in ("o",):
            if alt in out.columns:
                rename_map[alt] = "open"
                break
    if "high" not in out.columns:
        for alt in ("h",):
            if alt in out.columns:
                rename_map[alt] = "high"
                break
    if "low" not in out.columns:
        for alt in ("l",):
            if alt in out.columns:
                rename_map[alt] = "low"
                break
    if "volume" not in out.columns:
        for alt in ("v", "vol"):
            if alt in out.columns:
                rename_map[alt] = "volume"
                break

    out = out.rename(columns=rename_map)

    needed = {"open", "high", "low", "close", "volume"}
    missing = [c for c in needed if c not in out.columns]
    if missing:
        raise ValueError(f"Missing required OHLCV columns: {missing}. Have: {list(out.columns)}")

    # sort & drop duplicates
    out = out[~out.index.duplicated(keep="last")].sort_index()
    return out


def make_trades_from_signals(spy: pd.DataFrame, run_id: str) -> pd.DataFrame:
    """
    Minimal placeholder strategy:
    - 1 trade per day: LONG at first bar, exit at last bar.
    This is just to get the gate pipeline writing rows again.
    Replace later with real strategy eval.
    """
    df = spy.copy()

    # group by UTC date (simple + stable)
    df["date_utc"] = df.index.tz_convert("UTC").normalize()

    rows = []
    for date, g in df.groupby("date_utc", sort=True):
        if g.empty:
            continue
        entry_ts = g.index[0]
        exit_ts = g.index[-1]
        entry_px = float(g["close"].iloc[0])
        exit_px = float(g["close"].iloc[-1])

        # simple stop/target defaults (so gate_eval never sees None)
        stop_px = entry_px * (1 - 0.005)
        target_px = entry_px * (1 + 0.010)

        pnl_points = exit_px - entry_px
        minutes_held = int((exit_ts - entry_ts).total_seconds() // 60)
        bars_held = int(len(g))

        signal_id = f"{run_id}_{date.strftime('%Y%m%d')}"

        rows.append(
            {
                "run_id": run_id,
                "signal_id": signal_id,
                "entry_ts": entry_ts.isoformat(),
                "exit_ts": exit_ts.isoformat(),
                "side": "LONG",
                "entry_px": entry_px,
                "exit_px": exit_px,
                "stop_px": float(stop_px),
                "target_px": float(target_px),
                "pnl_points": float(pnl_points),
                "bars_held": int(bars_held),
                "minutes_held": int(minutes_held),
                "exit_reason": "EOD",
            }
        )

    return pd.DataFrame(rows)


def insert_run_row(conn: sqlite3.Connection, run_id: str, spy: pd.DataFrame, report_path: str) -> None:
    # matches your current runs schema (PRAGMA table_info(runs) you pasted earlier)
    created_at_utc = datetime.now(timezone.utc).isoformat()
    date_start_et = spy.index.min().tz_convert("America/New_York").date().isoformat()
    date_end_et = spy.index.max().tz_convert("America/New_York").date().isoformat()

    params = {
        "data_path": str(DATA_PATH),
        "strategy_version": STRATEGY_VERSION,
        "spec_version": SPEC_VERSION,
        "gate_ids": GATE_IDS,
        "start_equity": START_EQUITY,
    }

    conn.execute(
        """
        INSERT OR REPLACE INTO runs(
            run_id, created_at_utc, synthetic_data, date_start_et, date_end_et,
            params_json, metrics_json, report_path, equity_curve_path, pnl_hist_path
        ) VALUES (?,?,?,?,?,?,?,?,?,?)
        """,
        (
            run_id,
            created_at_utc,
            0,
            date_start_et,
            date_end_et,
            json.dumps(params),
            json.dumps({}),
            report_path,
            "",
            "",
        ),
    )


# -------------------------
# Gates (real hook)
# -------------------------

def load_gate_pack(gates_dir: Path, gate_ids: List[str]) -> Dict:
    # Canonical import (no PYTHONPATH dependency)
    import src.gate_eval as ge

    if hasattr(ge, "load_gate_pack"):
        return ge.load_gate_pack(gates_dir, gate_ids)
    if hasattr(ge, "load_gate_config"):
        # some versions call it load_gate_config
        return ge.load_gate_config(gates_dir, gate_ids)
    raise RuntimeError("gate_eval loader not found (expected load_gate_pack or load_gate_config).")


def evaluate_gates(conn: sqlite3.Connection, run_id: str, gates: Dict, start_equity: float) -> None:
    # Canonical import (no PYTHONPATH dependency)
    import src.gate_eval as ge

    # optional helper
    if hasattr(ge, "ensure_tables"):
        ge.ensure_tables(conn)

    # real evaluator
    return ge.evaluate_gates(conn, run_id, gates, start_equity=start_equity)


# -------------------------
# Main
# -------------------------

def main() -> int:
    run_id = utc_run_id()

    spy_raw = read_table_auto(DATA_PATH)
        # --- normalize timestamps + enforce RTH >= 09:45 ET (fix SESSION_PRE_945) ---
    import datetime as _dt
    import pandas as pd

    # pick timestamp column
    ts_col = "ts" if "ts" in spy_raw.columns else ("timestamp" if "timestamp" in spy_raw.columns else None)
    if ts_col is None:
        raise RuntimeError("No timestamp column found in data (expected 'ts' or 'timestamp').")

    # parse as UTC then convert to US/Eastern
    spy_raw[ts_col] = pd.to_datetime(spy_raw[ts_col], utc=True, errors="coerce")
    spy_raw = spy_raw.dropna(subset=[ts_col]).copy()
    ts_et = spy_raw[ts_col].dt.tz_convert("America/New_York")

    # enforce session window (>= 09:45 ET, <= 16:00 ET)
    t = ts_et.dt.time
    spy_raw = spy_raw[(t >= _dt.time(9, 45)) & (t <= _dt.time(16, 0))].copy()

    spy_raw["ts"] = ts_et.loc[spy_raw.index].dt.strftime("%Y-%m-%dT%H:%M:%S%z").str.replace(
    r"(\+|\-)(\d{2})(\d{2})$", r"\1\2:\3", regex=True
    )
    # --- end normalize ---
    spy = normalize_spy_5m(spy_raw)

    # create minimal trades so gates have something to evaluate
    trades = make_trades_from_signals(spy, run_id)

    # connect DB + write
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH))

    # write trades into existing table schema
    # (must match your PRAGMA table_info(trades))
    required_cols = [
        "run_id","signal_id","entry_ts","exit_ts","side",
        "entry_px","exit_px","stop_px","target_px",
        "pnl_points","bars_held","minutes_held","exit_reason"
    ]
    trades = trades[required_cols].copy()

    # If no trades, stop early (but don’t crash gates)
    if trades.empty:
        raise RuntimeError("No trades generated (even the placeholder strategy made none). Check DATA_PATH and parsing.")

    trades.to_sql("trades", conn, if_exists="append", index=False)

    # gate eval writes gate_decisions + gate_metrics
    gates = load_gate_pack(GATES_DIR, GATE_IDS)
    # -------------------------
    # Persist signals + trades BEFORE gate eval
    # gate_eval requires BOTH tables to already contain this run_id.
    # -------------------------

    # --- minimal signals table for gate_eval ---
    import pandas as pd

    signals_df = pd.DataFrame({
        "run_id": trades["run_id"].values,
        "signal_id": trades["signal_id"].values,
        "ts": trades["entry_ts"].values,     # use entry timestamp as the signal timestamp
    })

    signals_df.to_sql("signals", conn, if_exists="append", index=False)


    # --- end signals ---
    evaluate_gates(conn, run_id, gates, start_equity=START_EQUITY)


    # --- equity curve chart ---
    equity_png = HERE / "runs" / "artifacts" / "charts" / f"{run_id}_equity.png"
    write_equity_curve_png(conn, run_id, equity_png, START_EQUITY)

    # --- report md ---
    report_md = HERE / "runs" / "artifacts" / "reports" / f"{run_id}_multigate_report.md"
    write_run_report_md(conn, run_id, report_md, start_equity=START_EQUITY)


    # write run row last
    report_path = str(HERE / "runs" / "artifacts" / "reports" / f"{run_id}_multigate_report.md")
    insert_run_row(conn, run_id, spy, report_path)

    conn.commit()
    conn.close()

    print(f"✅ Completed run_id={run_id}")
    print(f"DB: {DB_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())