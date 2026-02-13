import json
import math
import sqlite3
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

ET_TZ = "America/New_York"


def _fmt_ts_col(s: Any, tz: str = ET_TZ, fmt: str = "%Y-%m-%d %H:%M:%S %Z"):
    if s is None:
        return s

    ts = pd.to_datetime(s, errors="coerce", utc=True)
    if isinstance(ts, pd.Series):
        ts = ts.dt.tz_convert(tz)
        out = ts.dt.strftime(fmt)
        return out.where(~ts.isna(), other="")

    if ts is pd.NaT:
        return ""

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


def _safe_json_obj(text: Any) -> Dict[str, Any]:
    if text is None:
        return {}
    if isinstance(text, dict):
        return text
    try:
        obj = json.loads(str(text))
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}


def _money(x: Optional[float]) -> str:
    if x is None or (isinstance(x, float) and math.isnan(x)):
        return "n/a"
    return f"${x:,.2f}"


def _pct(x: Optional[float]) -> str:
    if x is None or (isinstance(x, float) and math.isnan(x)):
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


def _fmt_float(v: Any, digits: int = 4) -> Any:
    try:
        x = float(v)
    except Exception:
        return v
    if math.isnan(x) or math.isinf(x):
        return v
    return round(x, digits)


def _direction_label(direction: Any, fallback_side: Any = None) -> str:
    s = str(direction).strip().upper()
    if s in {"1", "LONG", "BUY"}:
        return "LONG"
    if s in {"-1", "SHORT", "SELL"}:
        return "SHORT"
    fb = str(fallback_side).strip().upper()
    if fb in {"LONG", "SHORT"}:
        return fb
    return s if s else "UNK"


def _table_md(df: pd.DataFrame, max_rows: Optional[int] = None) -> str:
    if df.empty:
        return "_none_"
    out = df if max_rows is None else df.head(int(max_rows)).copy()
    try:
        return out.to_markdown(index=False)
    except Exception:
        return "```\n" + out.to_string(index=False) + "\n```"


def _parse_run_context(run_row: Dict[str, Any]) -> Dict[str, Any]:
    params = _safe_json_obj(run_row.get("params_json"))
    return {
        "requested_data_path": params.get("requested_data_path") or params.get("data_path") or "",
        "resolved_data_path": params.get("resolved_data_path") or params.get("data_path") or "",
        "source_sidecar_path": params.get("source_sidecar_path") or "",
        "source_vendor": params.get("source_vendor") or "unknown",
        "source_dataset": params.get("source_dataset") or "unknown",
        "source_schema": params.get("source_schema") or "unknown",
        "source_feed": params.get("source_feed") or "",
        "source_symbol": params.get("source_symbol") or "UNK",
        "bar_timeframe": params.get("bar_timeframe") or "unknown",
        "db_path": params.get("db_path") or "runs/backtests.sqlite",
        "raw_row_count": params.get("raw_row_count"),
        "normalized_row_count": params.get("normalized_row_count"),
        "post_rth_row_count": params.get("post_rth_row_count"),
        "raw_columns": params.get("raw_columns") or [],
        "normalized_columns": params.get("normalized_columns") or [],
        "timezone": params.get("timezone") or ET_TZ,
        "rth_enabled": params.get("rth_enabled"),
        "rth_start": params.get("rth_start"),
        "rth_end": params.get("rth_end"),
    }


def write_run_report_md(
    conn: sqlite3.Connection,
    run_id: str,
    report_md_path: str | Path,
    start_equity: float = 100_000.0,
) -> Path:
    report_path = Path(report_md_path)
    report_path.parent.mkdir(parents=True, exist_ok=True)

    runs = _read_sql(
        conn,
        "SELECT run_id, created_at_utc, date_start_et, date_end_et, params_json, report_path, equity_curve_path FROM runs WHERE run_id = ? LIMIT 1",
        (run_id,),
    )
    run_row = runs.iloc[0].to_dict() if not runs.empty else {}
    ctx = _parse_run_context(run_row)

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

    trades_source = "trades"
    if not trades_pass.empty:
        trades = trades_pass.copy()
        if "signal_id" in trades.columns:
            trades = trades.drop_duplicates(subset=["signal_id"], keep="last")
        trades_source = "trades_pass (deduped by signal_id)"
    else:
        trades = trades_all.copy()

    for c in ("entry_ts", "exit_ts"):
        if c in trades.columns:
            trades[c] = pd.to_datetime(trades[c], errors="coerce", utc=True)

    pnl_col = "pnl_points" if "pnl_points" in trades.columns else None
    pnls = trades[pnl_col].astype(float) if pnl_col else pd.Series(dtype=float)

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

    gate_pf = _read_sql(
        conn,
        "SELECT gate_id, SUM(CASE WHEN decision='PASS' THEN 1 ELSE 0 END) AS pass, SUM(CASE WHEN decision='FAIL' THEN 1 ELSE 0 END) AS fail FROM gate_decisions WHERE run_id = ? GROUP BY gate_id ORDER BY gate_id",
        (run_id,),
    )

    denial = _read_sql(
        conn,
        "SELECT gate_id, denial_code, COUNT(*) AS n FROM gate_decisions WHERE run_id = ? AND decision='FAIL' GROUP BY gate_id, denial_code ORDER BY gate_id, n DESC",
        (run_id,),
    )

    metrics = _read_sql(
        conn,
        "SELECT * FROM gate_metrics WHERE run_id = ? ORDER BY gate_id",
        (run_id,),
    )

    blocked = _read_sql(
        conn,
        """
        SELECT gd.gate_id, gd.signal_id, gd.denial_code, gd.denial_detail, gd.ts,
               t.side, t.entry_ts, t.exit_ts, t.entry_px, t.exit_px, t.pnl_points
        FROM gate_decisions gd
        LEFT JOIN trades t
          ON t.run_id = gd.run_id
         AND t.signal_id = gd.signal_id
        WHERE gd.run_id = ? AND gd.decision = 'FAIL'
        ORDER BY gd.gate_id, gd.ts DESC
        """,
        (run_id,),
    )

    blocked_summary_rows: List[Dict[str, Any]] = []
    blocked_top = pd.DataFrame()
    if not blocked.empty:
        blocked["pnl_points"] = pd.to_numeric(blocked["pnl_points"], errors="coerce")
        grouped = blocked.groupby("gate_id", dropna=False)
        for gate_id, g in grouped:
            poss = g["pnl_points"] > 0
            blocked_summary_rows.append(
                {
                    "gate_id": gate_id,
                    "blocked_signals": int(g["signal_id"].nunique()),
                    "missed_winners": int(poss.sum()),
                    "missed_winner_points": _fmt_float(g.loc[poss, "pnl_points"].sum(), 4),
                    "avg_blocked_points": _fmt_float(g["pnl_points"].mean(), 4),
                    "worst_blocked_points": _fmt_float(g["pnl_points"].min(), 4),
                    "best_blocked_points": _fmt_float(g["pnl_points"].max(), 4),
                }
            )

        blocked_top = blocked[blocked["pnl_points"] > 0].copy()
        if not blocked_top.empty:
            blocked_top["decision_ts_et"] = _fmt_ts_col(blocked_top["ts"])
            blocked_top["entry_et"] = _fmt_ts_col(blocked_top["entry_ts"])
            blocked_top["exit_et"] = _fmt_ts_col(blocked_top["exit_ts"])
            blocked_top = blocked_top.sort_values("pnl_points", ascending=False)
            blocked_top = blocked_top[
                [
                    "gate_id",
                    "signal_id",
                    "side",
                    "decision_ts_et",
                    "entry_et",
                    "exit_et",
                    "pnl_points",
                    "denial_code",
                    "denial_detail",
                ]
            ]

    blocked_summary = pd.DataFrame(blocked_summary_rows).sort_values("gate_id") if blocked_summary_rows else pd.DataFrame()

    signals = _read_sql(
        conn,
        """
        SELECT s.signal_id, s.ts AS signal_ts_utc, s.direction, s.features_json,
               t.side, t.entry_ts, t.exit_ts, t.entry_px, t.exit_px, t.pnl_points
        FROM signals s
        LEFT JOIN trades t
          ON t.run_id = s.run_id
         AND t.signal_id = s.signal_id
        WHERE s.run_id = ?
        ORDER BY s.ts DESC
        """,
        (run_id,),
    )

    regime_summary = pd.DataFrame()
    signal_details = pd.DataFrame()
    if not signals.empty:
        feature_objs = signals["features_json"].apply(_safe_json_obj)
        signals["signal_description"] = feature_objs.apply(lambda d: d.get("signal_description", ""))
        signals["strategy_rationale"] = feature_objs.apply(lambda d: d.get("strategy_rationale", ""))
        signals["vix_regime"] = feature_objs.apply(lambda d: d.get("vix_regime", "UNKNOWN"))
        signals["session_date"] = feature_objs.apply(lambda d: d.get("session_date", ""))

        signals["direction_label"] = [
            _direction_label(d, s) for d, s in zip(signals.get("direction", []), signals.get("side", []))
        ]
        signals["signal_ts_et"] = _fmt_ts_col(signals["signal_ts_utc"])
        signals["entry_et"] = _fmt_ts_col(signals["entry_ts"])
        signals["exit_et"] = _fmt_ts_col(signals["exit_ts"])

        signals["pnl_points"] = pd.to_numeric(signals["pnl_points"], errors="coerce")
        regime_summary = (
            signals.groupby("vix_regime", dropna=False)
            .agg(
                signals=("signal_id", "count"),
                win_rate=("pnl_points", lambda s: float((s > 0).mean()) if len(s) else float("nan")),
                avg_pnl_points=("pnl_points", "mean"),
                total_pnl_points=("pnl_points", "sum"),
            )
            .reset_index()
            .sort_values("signals", ascending=False)
        )
        if not regime_summary.empty:
            regime_summary["win_rate"] = regime_summary["win_rate"].apply(lambda x: _pct(x if pd.notna(x) else float("nan")))
            regime_summary["avg_pnl_points"] = regime_summary["avg_pnl_points"].apply(lambda x: _fmt_float(x, 4))
            regime_summary["total_pnl_points"] = regime_summary["total_pnl_points"].apply(lambda x: _fmt_float(x, 4))

        signal_details = signals[
            [
                "signal_id",
                "signal_ts_et",
                "session_date",
                "direction_label",
                "vix_regime",
                "signal_description",
                "strategy_rationale",
                "entry_et",
                "exit_et",
                "entry_px",
                "exit_px",
                "pnl_points",
            ]
        ].copy()
        signal_details["entry_px"] = signal_details["entry_px"].apply(lambda x: _fmt_float(x, 4))
        signal_details["exit_px"] = signal_details["exit_px"].apply(lambda x: _fmt_float(x, 4))
        signal_details["pnl_points"] = signal_details["pnl_points"].apply(lambda x: _fmt_float(x, 4))

    schema_rows: List[pd.DataFrame] = []
    for table in ("runs", "signals", "trades", "gate_decisions", "trades_pass"):
        s = _read_sql(conn, f"PRAGMA table_info({table})")
        if not s.empty:
            s = s[["name", "type", "notnull", "pk"]].copy()
            s.insert(0, "table", table)
            schema_rows.append(s)
    schema_df = pd.concat(schema_rows, ignore_index=True) if schema_rows else pd.DataFrame()

    expected_png_rel = f"../charts/{run_id}_equity.png"
    expected_png_abs = report_path.parent.parent / "charts" / f"{run_id}_equity.png"

    narrative_lines: List[str] = []
    narrative_lines.append(
        f"Run `{run_id}` tested `{ctx['source_symbol']}` using vendor `{ctx['source_vendor']}` "
        f"dataset `{ctx['source_dataset']}` schema `{ctx['source_schema']}` on timeframe `{ctx['bar_timeframe']}`."
    )
    narrative_lines.append(
        f"The run spans ET dates `{run_row.get('date_start_et', 'n/a')}` to `{run_row.get('date_end_et', 'n/a')}` "
        f"with {len(trades)} executed trades from `{trades_source}`."
    )
    if math.isfinite(total_pnl) and math.isfinite(win_rate):
        narrative_lines.append(
            f"Performance finished at {_money(end_equity)} ({_pct(ret)}), with win rate {_pct(win_rate)}, "
            f"profit factor {pf:,.3f} and max drawdown {_pct(mdd)}."
        )
    if not blocked_summary.empty:
        top_gate = blocked_summary.sort_values("missed_winner_points", ascending=False).iloc[0]
        narrative_lines.append(
            f"Largest blocked winner pool came from gate `{top_gate['gate_id']}` with "
            f"{top_gate['missed_winners']} blocked winners totaling {top_gate['missed_winner_points']} points."
        )
    if not regime_summary.empty:
        top_reg = regime_summary.iloc[0]
        narrative_lines.append(
            f"Most frequent regime was `{top_reg['vix_regime']}` with {int(top_reg['signals'])} signals."
        )

    md: List[str] = []
    md.append(f"# Multigate report — `{run_id}`\n")

    md.append("## Summary\n")
    md.append(f"- Trades used: **{len(trades)}** ({trades_source})")
    md.append(f"- Start equity: **{_money(start_equity)}**")
    md.append(f"- End equity: **{_money(end_equity)}**")
    md.append(f"- Total PnL: **{_money(total_pnl)}**")
    md.append(f"- Return: **{_pct(ret)}**")
    md.append(f"- Win rate: **{_pct(win_rate)}**")
    md.append(f"- Profit factor: **{pf:,.3f}**" if math.isfinite(pf) else f"- Profit factor: **{pf}**")
    md.append(f"- Avg trade PnL: **{_fmt_float(avg, 4)} points**")
    md.append(f"- Median trade PnL: **{_fmt_float(med, 4)} points**")
    md.append(f"- Best trade: **{_fmt_float(best, 4)} points**")
    md.append(f"- Worst trade: **{_fmt_float(worst, 4)} points**")
    md.append(f"- Max drawdown: **{_pct(mdd)}**" if math.isfinite(mdd) else f"- Max drawdown: **{mdd}**")
    md.append("")

    md.append("## Narrative\n")
    for line in narrative_lines:
        md.append(f"- {line}")
    md.append("")

    if run_row:
        md.append("## Run Metadata\n")
        created = _fmt_ts_col(run_row.get("created_at_utc"), tz="UTC", fmt="%Y-%m-%d %H:%M:%S %Z")
        md.append(f"- **created_at_utc**: `{created}`")
        md.append(f"- **date_start_et**: `{run_row.get('date_start_et')}`")
        md.append(f"- **date_end_et**: `{run_row.get('date_end_et')}`")
        md.append(f"- **report_path**: `{run_row.get('report_path')}`")
        md.append(f"- **equity_curve_path**: `{run_row.get('equity_curve_path')}`")
        md.append("")

    md.append("## Data Source & Schema\n")
    md.append(f"- **symbol**: `{ctx['source_symbol']}`")
    md.append(f"- **vendor**: `{ctx['source_vendor']}`")
    md.append(f"- **dataset**: `{ctx['source_dataset']}`")
    md.append(f"- **schema**: `{ctx['source_schema']}`")
    md.append(f"- **feed**: `{ctx['source_feed']}`")
    md.append(f"- **bar_timeframe**: `{ctx['bar_timeframe']}`")
    md.append(f"- **requested_data_path**: `{ctx['requested_data_path']}`")
    md.append(f"- **resolved_data_path**: `{ctx['resolved_data_path']}`")
    md.append(f"- **source_sidecar_path**: `{ctx['source_sidecar_path']}`")
    md.append(f"- **db_path**: `{ctx['db_path']}`")
    md.append(f"- **row_counts(raw/normalized/post_rth)**: `{ctx['raw_row_count']} / {ctx['normalized_row_count']} / {ctx['post_rth_row_count']}`")
    md.append(f"- **runtime_timezone**: `{ctx['timezone']}`")
    md.append(f"- **rth_window**: `{ctx['rth_start']} -> {ctx['rth_end']}` (enabled={ctx['rth_enabled']})")
    md.append("")

    if ctx["normalized_columns"]:
        cols_df = pd.DataFrame({"normalized_columns": [", ".join(ctx["normalized_columns"])]})
        md.append("### Input Data Columns\n")
        md.append(_table_md(cols_df))
        md.append("")

    if not schema_df.empty:
        md.append("### SQLite Table Schema Snapshot\n")
        md.append(_table_md(schema_df))
        md.append("")

    if expected_png_abs.exists():
        md.append("## Equity Curve\n")
        md.append(f"![Equity Curve]({expected_png_rel})")
        md.append("")

    if not gate_pf.empty:
        md.append("## Gate PASS/FAIL\n")
        md.append(_table_md(gate_pf))
        md.append("")

    if not denial.empty:
        md.append("## Denial Reasons\n")
        md.append(_table_md(denial))
        md.append("")

    if not metrics.empty:
        md.append("## Gate Metrics (Rollups)\n")
        md.append(_table_md(metrics))
        md.append("")

    if not blocked_summary.empty:
        md.append("## Blocked Opportunities (If Gates Had Not Blocked)\n")
        md.append(_table_md(blocked_summary))
        md.append("")

    if not blocked_top.empty:
        md.append("## Top Missed Winners Due To Gate Fails\n")
        md.append(_table_md(blocked_top, max_rows=20))
        md.append("")

    if not regime_summary.empty:
        md.append("## Regime Breakdown\n")
        md.append(_table_md(regime_summary))
        md.append("")

    if pnl_col and not trades.empty:
        trades_disp = trades.copy()
        if "entry_ts" in trades_disp.columns:
            trades_disp["entry_et"] = _fmt_ts_col(trades_disp["entry_ts"])
        if "exit_ts" in trades_disp.columns:
            trades_disp["exit_et"] = _fmt_ts_col(trades_disp["exit_ts"])

        cols = [
            c
            for c in (
                "signal_id",
                "entry_et",
                "exit_et",
                "side",
                "entry_px",
                "exit_px",
                pnl_col,
                "exit_reason",
            )
            if c in trades_disp.columns
        ]

        md.append("## Best Trades (Top 10)\n")
        md.append(_table_md(trades_disp.sort_values(pnl_col, ascending=False).head(10)[cols]))
        md.append("")

        md.append("## Worst Trades (Bottom 10)\n")
        md.append(_table_md(trades_disp.sort_values(pnl_col, ascending=True).head(10)[cols]))
        md.append("")

    if not signal_details.empty:
        md.append("## Signal Detail (What Signaled)\n")
        md.append("Signals now include direction, rationale, regime, entry/exit context, and realized outcome.")
        md.append(_table_md(signal_details, max_rows=120))
        if len(signal_details) > 120:
            md.append(f"\n_Only first 120 of {len(signal_details)} signals shown in report. Full detail is in `signals.features_json`._")
        md.append("")

    md.append("## Helpful Sanity Checks\n")
    md.append("```bash")
    md.append('RUN_ID="$(sqlite3 runs/backtests.sqlite \\\"SELECT MAX(run_id) FROM runs;\\\")"')
    md.append('open "runs/artifacts/reports/${RUN_ID}_multigate_report.md"')
    md.append('open "runs/artifacts/charts/${RUN_ID}_equity.png"')
    md.append("```")
    md.append("")

    report_path.write_text("\n".join(md), encoding="utf-8")
    return report_path
