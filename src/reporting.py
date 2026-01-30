# src/reporting.py
import math
import sqlite3
from pathlib import Path
from typing import Optional, Tuple

import pandas as pd

# --- timestamp formatting (report display) ---
def _fmt_ts_col(s, tz="America/New_York", fmt="%Y-%m-%d %H:%M"):
    """
    Accepts either a pandas Series OR a scalar (string/Timestamp).
    Returns formatted timestamps with cleaner display (no microseconds).
    """
    if s is None:
        return s

    ts = pd.to_datetime(s, errors="coerce", utc=True)

    # Series case (DataFrame column)
    if isinstance(ts, pd.Series):
        ts = ts.dt.tz_convert(tz)
        return ts.dt.strftime(fmt)

    # Scalar case
    if ts is pd.NaT:
        return s

    try:
        ts = ts.tz_convert(tz)
    except Exception:
        pass

    return ts.strftime(fmt)

def _read_sql(conn: sqlite3.Connection, sql: str, params: Tuple = ()) -> pd.DataFrame:
    try:
        return pd.read_sql_query(sql, conn, params=params)
    except Exception:
        return pd.DataFrame()


def _money(x: Optional[float]) -> str:
    if x is None or (isinstance(x, float) and (math.isnan(x))):
        return "n/a"
    return f"${x:,.2f}"


def _pct(x: Optional[float]) -> str:
    if x is None or (isinstance(x, float) and (math.isnan(x))):
        return "n/a"
    return f"{x*100:,.2f}%"


def _max_drawdown(equity: pd.Series) -> float:
    if equity.empty:
        return float("nan")
    peak = equity.cummax()
    dd = (equity - peak) / peak.replace(0, float("nan"))
    return float(dd.min())


def _profit_factor(pnls: pd.Series) -> float:
    if pnls.empty:
        return float("nan")
    gains = pnls[pnls > 0].sum()
    losses = -pnls[pnls < 0].sum()
    if losses == 0:
        return float("inf") if gains > 0 else float("nan")
    return float(gains / losses)


def write_run_report_md(
    conn: sqlite3.Connection,
    run_id: str,
    report_md_path: str | Path,
    start_equity: float = 100_000.0,
) -> Path:
    """
    Generates a detailed markdown report for a run_id using the sqlite DB.
    Safe if some tables/columns are missing (it will just omit sections).
    """
    report_path = Path(report_md_path)
    report_path.parent.mkdir(parents=True, exist_ok=True)

    # --- Core run row (optional) ---
    runs = _read_sql(
        conn,
        "SELECT run_id, created_at_utc, date_start_et, date_end_et, params_json, report_path "
        "FROM runs WHERE run_id = ? LIMIT 1",
        (run_id,),
    )
    run_row = runs.iloc[0].to_dict() if not runs.empty else {}

    # --- Trades (prefer PASS trades if present) ---
    trades_pass = _read_sql(
        conn,
        "SELECT * FROM trades_pass WHERE run_id = ? ORDER BY exit_ts",
        (run_id,),
    )
    trades_all = _read_sql(
        conn,
        "SELECT * FROM trades WHERE run_id = ? ORDER BY exit_ts",
        (run_id,),
    )

    trades = trades_pass if not trades_pass.empty else trades_all

    # Normalize timestamps if present
    for c in ("entry_ts", "exit_ts"):
        if c in trades.columns:
            trades[c] = pd.to_datetime(trades[c], errors="coerce", utc=True)

    # PnL column
    pnl_col = "pnl_points" if "pnl_points" in trades.columns else None
    pnls = trades[pnl_col].astype(float) if pnl_col else pd.Series(dtype=float)

    # Equity curve
    if pnl_col and not trades.empty:
        equity = start_equity + pnls.cumsum()
        end_equity = float(equity.iloc[-1])
        total_pnl = float(pnls.sum())
        ret = (end_equity / start_equity) - 1.0 if start_equity else float("nan")
        win_rate = float((pnls > 0).mean()) if len(pnls) else float("nan")
        pf = _profit_factor(pnls)
        avg = float(pnls.mean())
        med = float(pnls.median())
        worst = float(pnls.min())
        best = float(pnls.max())
        mdd = _max_drawdown(equity)
    else:
        end_equity = float("nan")
        total_pnl = float("nan")
        ret = float("nan")
        win_rate = float("nan")
        pf = float("nan")
        avg = float("nan")
        med = float("nan")
        worst = float("nan")
        best = float("nan")
        mdd = float("nan")

    # --- Gate PASS/FAIL breakdown ---
    gate_pf = _read_sql(
        conn,
        "SELECT gate_id, "
        "SUM(CASE WHEN decision='PASS' THEN 1 ELSE 0 END) AS pass, "
        "SUM(CASE WHEN decision='FAIL' THEN 1 ELSE 0 END) AS fail "
        "FROM gate_decisions WHERE run_id = ? "
        "GROUP BY gate_id ORDER BY gate_id",
        (run_id,),
    )

    # --- Denial reasons ---
    denial = _read_sql(
        conn,
        "SELECT gate_id, denial_code, COUNT(*) AS n "
        "FROM gate_decisions "
        "WHERE run_id = ? AND decision='FAIL' "
        "GROUP BY gate_id, denial_code "
        "ORDER BY gate_id, n DESC",
        (run_id,),
    )

    # --- Gate metrics (rollups) ---
    metrics = _read_sql(
        conn,
        "SELECT * FROM gate_metrics WHERE run_id = ? ORDER BY gate_id",
        (run_id,),
    )

    # --- Equity chart path (expected) ---
    # report is in runs/artifacts/reports; charts is sibling folder
    expected_png_rel = f"../charts/{run_id}_equity.png"
    expected_png_abs = report_path.parent.parent / "charts" / f"{run_id}_equity.png"

    # --- Build markdown ---
    md = []
    md.append(f"# Multigate report — `{run_id}`\n")

    md.append("## Summary\n")
    md.append(f"- Trades used: **{len(trades)}** ({'trades_pass' if trades is trades_pass else 'trades'})")
    md.append(f"- Start equity: **{_money(start_equity)}**")
    md.append(f"- End equity: **{_money(end_equity)}**")
    md.append(f"- Total PnL: **{_money(total_pnl)}**")
    md.append(f"- Return: **{_pct(ret)}**")
    md.append(f"- Win rate: **{_pct(win_rate)}**")
    md.append(f"- Profit factor: **{pf:,.3f}**" if math.isfinite(pf) else f"- Profit factor: **{pf}**")
    md.append(f"- Max drawdown: **{_pct(mdd)}**" if math.isfinite(mdd) else f"- Max drawdown: **{mdd}**")
    md.append("")

    if run_row:
        md.append("## Run metadata\n")
            for k in meta_keys:
        v = run_row.get(k)

        # clean up timestamps (remove microseconds / long ISO)
        if k == "created_at_utc" and v:
            v = f"{_fmt_ts_col(v, tz='UTC', fmt='%Y-%m-%d %H:%M')} UTC"

        elif k in ("date_start_et", "date_end_et") and v:
            v_fmt = _fmt_ts_col(v, tz="America/New_York", fmt="%Y-%m-%d %H:%M")
            # if it's midnight, show date only
            if v_fmt.endswith("00:00"):
                v = v_fmt.split()[0]
            else:
                v = f"{v_fmt} ET"

        md.append(f"- **{k}**: `{v}`")

    # Denial reasons
    if not denial.empty:
        md.append("## Denial reasons\n")
        md.append(denial.to_markdown(index=False))
        md.append("")

    # Gate metrics
    if not metrics.empty:
        md.append("## Gate metrics (rollups)\n")
        md.append(metrics.to_markdown(index=False))
        md.append("")

    # Best/worst trades
    if pnl_col and not trades.empty:
        cols = [c for c in ("signal_id", "entry_ts", "exit_ts", "side", "entry_px", "exit_px", pnl_col, "exit_reason") if c in trades.columns]

        md.append("## Best trades (top 10)\n")
        md.append(trades.sort_values(pnl_col, ascending=False).head(10)[cols].to_markdown(index=False))
        md.append("")

        md.append("## Worst trades (bottom 10)\n")
        md.append(trades.sort_values(pnl_col, ascending=True).head(10)[cols].to_markdown(index=False))
        md.append("")

    # Sanity check snippets
    md.append("## Helpful sanity checks\n")
    md.append("```bash")
    md.append('RUN_ID="$(sqlite3 runs/backtests.sqlite \\"SELECT MAX(run_id) FROM runs;\\")"')
    md.append('open "runs/artifacts/reports/${RUN_ID}_multigate_report.md"')
    md.append('open "runs/artifacts/charts/${RUN_ID}_equity.png"')
    md.append("```")
    md.append("")

    report_path.write_text("\n".join(md), encoding="utf-8")
    return report_path