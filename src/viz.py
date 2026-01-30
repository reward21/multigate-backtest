# src/viz.py

from __future__ import annotations

from pathlib import Path
import sqlite3


def write_equity_curve_png(
    conn: sqlite3.Connection,
    run_id: str,
    out_path: Path,
    start_equity: float,
    table: str = "trades_pass",
    pnl_col: str = "pnl_points",
    ts_col: str = "exit_ts",
) -> None:
    """
    Writes an equity-curve PNG for a given run_id from a SQLite connection.

    Default source: trades_pass (per-run trades that passed gates).
    Assumes pnl_points is additive (MVP plumbing).
    """

    import pandas as pd
    import matplotlib.pyplot as plt

    q = f"SELECT {ts_col} AS ts, {pnl_col} AS pnl FROM {table} WHERE run_id=? ORDER BY {ts_col}"
    df = pd.read_sql_query(q, conn, params=(run_id,))

    if df.empty:
        print(f"ℹ️ No rows in {table} for run_id={run_id}; skipping equity curve.")
        return

    df["ts"] = pd.to_datetime(df["ts"], utc=True, errors="coerce")
    df = df.dropna(subset=["ts"])

    if df.empty:
        print(f"ℹ️ {table} had no usable timestamps for run_id={run_id}; skipping equity curve.")
        return

    df["equity"] = float(start_equity) + df["pnl"].cumsum()

    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    plt.figure()
    plt.plot(df["ts"], df["equity"])
    plt.title(f"Equity curve — {run_id}")
    plt.xlabel("time")
    plt.ylabel("equity")
    plt.tight_layout()
    plt.savefig(out_path, dpi=150)
    plt.close()

    print(f"📈 Equity curve saved: {out_path}")