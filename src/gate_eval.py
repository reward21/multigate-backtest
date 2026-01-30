from __future__ import annotations

import json
import sqlite3
from datetime import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

ET_TZ = "America/New_York"

CANONICAL_DENIAL_CODES = [
    "RISK_DAILY_KILL","RISK_PER_TRADE_CAP","RISK_OVERSIZE","RISK_MAX_TRADES","RISK_LOSS_STREAK","RISK_COOLDOWN",
    "SESSION_PRE_945","SESSION_POST_300","SESSION_FORCE_FLAT","VOL_VIX_BLOCK","LIQ_SPREAD","LIQ_VOLUME","LIQ_OPEN_INTEREST",
    "ADD_TO_LOSER_BLOCK","OVERNIGHT_BLOCK","REENTRY_BLOCK","AVG_DOWN_BLOCK"
]

def _t(hhmmss: str) -> time:
    hh, mm, ss = [int(x) for x in hhmmss.split(":")]
    return time(hh, mm, ss)

def ensure_tables(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    cur.executescript("""
    CREATE TABLE IF NOT EXISTS runs(
      run_id TEXT PRIMARY KEY,
      symbol TEXT,
      timeframe TEXT,
      date_start TEXT,
      date_end TEXT,
      holding_mode TEXT,
      strategy_version TEXT,
      spec_version TEXT,
      created_utc TEXT,
      notes TEXT
    );

    CREATE TABLE IF NOT EXISTS signals(
      run_id TEXT,
      signal_id TEXT,
      ts TEXT,
      direction INTEGER,
      features_json TEXT,
      PRIMARY KEY (run_id, signal_id)
    );

    CREATE TABLE IF NOT EXISTS trades(
      run_id TEXT,
      signal_id TEXT,
      entry_ts TEXT,
      exit_ts TEXT,
      side TEXT,
      entry_px REAL,
      exit_px REAL,
      stop_px REAL,
      target_px REAL,
      pnl_points REAL,
      bars_held INTEGER,
      minutes_held INTEGER,
      exit_reason TEXT,
      PRIMARY KEY (run_id, signal_id)
    );

    CREATE TABLE IF NOT EXISTS gate_decisions(
      run_id TEXT,
      gate_id TEXT,
      signal_id TEXT,
      decision TEXT,
      denial_code TEXT,
      denial_detail TEXT,
      equity_at_decision REAL,
      risk_at_decision REAL,
      ts TEXT,
      PRIMARY KEY (run_id, gate_id, signal_id)
    );

    CREATE TABLE IF NOT EXISTS trades_pass(
      run_id TEXT,
      gate_id TEXT,
      trade_id TEXT,
      signal_id TEXT,
      entry_ts TEXT,
      exit_ts TEXT,
      side TEXT,
      entry_px REAL,
      exit_px REAL,
      stop_px REAL,
      target_px REAL,
      pnl REAL,
      pnl_points REAL,
      size REAL,
      mae_points REAL,
      mfe_points REAL,
      hold_minutes INTEGER,
      exit_reason TEXT,
      PRIMARY KEY (run_id, gate_id, trade_id)
    );

    CREATE TABLE IF NOT EXISTS gate_daily_stats(
      run_id TEXT,
      gate_id TEXT,
      session_date TEXT,
      trades_taken INTEGER,
      pnl_day REAL,
      dd_day REAL,
      kill_switch_hit INTEGER,
      PRIMARY KEY (run_id, gate_id, session_date)
    );

    CREATE TABLE IF NOT EXISTS gate_metrics(
      run_id TEXT,
      gate_id TEXT,
      trade_count INTEGER,
      win_rate REAL,
      pf REAL,
      expectancy REAL,
      maxdd REAL,
      worst_day REAL,
      worst_trade REAL,
      avg_hold REAL,
      zero_trade_day_pct REAL,
      ending_equity REAL,
      PRIMARY KEY (run_id, gate_id)
    );
    """)
    conn.commit()

def load_gate_config(path: Path) -> Dict:
    return json.loads(path.read_text(encoding="utf-8"))

def load_gate_pack(gates_dir: Path, gate_ids: List[str]) -> Dict[str, Dict]:
    out: Dict[str, Dict] = {}
    for gid in gate_ids:
        if gid == "G1":
            f = gates_dir / "G1.json"
        else:
            f = gates_dir / f"{gid}.json"
        out[gid] = load_gate_config(f)
    return out

def _is_in_session(ts_et: pd.Timestamp, rules: Dict) -> Tuple[bool, Optional[str]]:
    if not rules.get("enabled", False):
        return True, None
    t = ts_et.timetz()
    if t < _t(rules["no_entry_before"]):
        return False, "SESSION_PRE_945"
    if t > _t(rules["no_new_entry_after"]):
        return False, "SESSION_POST_300"
    if t >= _t(rules["force_flat_at"]):
        return False, "SESSION_FORCE_FLAT"
    return True, None

def _vix_ok(vix_regime: str, rule: Dict) -> Tuple[bool, Optional[str]]:
    if not rule.get("enabled", False):
        return True, None
    allowed = set(rule.get("allowed_regimes", []))
    if vix_regime not in allowed:
        return False, "VOL_VIX_BLOCK"
    return True, None

def compute_trade_metrics(trades: pd.DataFrame) -> Dict[str, float]:
    if trades.empty:
        return dict(trade_count=0, win_rate=np.nan, pf=np.nan, expectancy=np.nan,
                    maxdd=0.0, worst_day=0.0, worst_trade=0.0, avg_hold=np.nan,
                    zero_trade_day_pct=np.nan, ending_equity=np.nan)
    pnl = trades["pnl"].astype(float)
    wins = pnl[pnl > 0]
    losses = pnl[pnl < 0].abs()
    pf = (wins.sum() / losses.sum()) if losses.sum() > 0 else np.inf
    win_rate = float((pnl > 0).mean())
    expectancy = float(pnl.mean())
    worst_trade = float(pnl.min())
    avg_hold = float(trades["hold_minutes"].mean())
    return dict(trade_count=int(len(trades)), win_rate=win_rate, pf=float(pf),
                expectancy=expectancy, worst_trade=worst_trade, avg_hold=avg_hold)

def equity_curve_from_trades(trades: pd.DataFrame, start_equity: float) -> pd.Series:
    eq = [start_equity]
    for p in trades["pnl"].astype(float).tolist():
        eq.append(eq[-1] + p)
    return pd.Series(eq)

def max_drawdown(equity: pd.Series) -> float:
    peak = equity.cummax()
    dd = equity - peak
    return float(dd.min())

def evaluate_gates(
    conn: sqlite3.Connection,
    run_id: str,
    gates: Dict[str, Dict],
    start_equity: float = 100_000.0,
) -> None:
    ensure_tables(conn)
    cur = conn.cursor()

    signals = pd.read_sql_query("SELECT * FROM signals WHERE run_id=? ORDER BY ts", conn, params=(run_id,))
    trades = pd.read_sql_query("SELECT * FROM trades WHERE run_id=? ORDER BY entry_ts", conn, params=(run_id,))
    trades = trades.set_index("signal_id", drop=False)
    if signals.empty or trades.empty:
        raise RuntimeError(f"run_id {run_id} missing signals/trades; cannot evaluate gates.")

    signals = signals.set_index("signal_id", drop=False)

    def parse_features(s: str) -> Dict:
        try:
            return json.loads(s) if s else {}
        except Exception:
            return {}
    feat = signals["features_json"].apply(parse_features)
    signals["session_date"] = feat.apply(lambda d: d.get("session_date"))
    signals["vix_regime"] = feat.apply(lambda d: d.get("vix_regime"))

    for t in ["gate_decisions","trades_pass","gate_daily_stats","gate_metrics"]:
        cur.execute(f"DELETE FROM {t} WHERE run_id=?", (run_id,))
    conn.commit()

    for gid, cfg in gates.items():
        rules = cfg["rules"]
        sess_rule = rules["session_rules"]
        vix_rule = rules["vix_regime_filter"]
        max_trades_rule = rules["max_trades_per_day"]
        loss_streak_rule = rules["max_consecutive_losses"]
        cooldown_rule = rules["cooldown_minutes_after_stop"]
        kill_rule = rules["daily_kill_switch"]
        risk_rule = rules["per_trade_risk_cap"]

        equity = start_equity
        day_trade_count: Dict[str, int] = {}
        day_start_equity: Dict[str, float] = {}
        day_peak_equity: Dict[str, float] = {}
        day_pnl: Dict[str, float] = {}
        day_kill: Dict[str, bool] = {}

        loss_streak = 0
        cooldown_until_ts: Optional[pd.Timestamp] = None

        pass_trades = []

        for _, sig in signals.iterrows():
            signal_id = sig["signal_id"]
            ts = pd.to_datetime(sig["ts"])
            ts_et = ts.tz_convert(ET_TZ) if ts.tzinfo is not None else ts.tz_localize("UTC").tz_convert(ET_TZ)
            session_date = sig["session_date"] or str(ts_et.date())
            vix_regime = sig["vix_regime"] or "UNKNOWN"

            if session_date not in day_trade_count:
                day_trade_count[session_date] = 0
                day_start_equity[session_date] = equity
                day_peak_equity[session_date] = equity
                day_pnl[session_date] = 0.0
                day_kill[session_date] = False

            decision = "PASS"
            denial_code = None
            denial_detail = None

            if decision == "PASS" and cooldown_rule.get("enabled", False) and cooldown_until_ts is not None:
                if ts_et < cooldown_until_ts:
                    decision = "FAIL"
                    denial_code = "RISK_COOLDOWN"
                    denial_detail = f"cooldown_until={cooldown_until_ts.isoformat()}"

            if decision == "PASS" and kill_rule.get("enabled", False) and day_kill.get(session_date, False):
                decision = "FAIL"
                denial_code = "RISK_DAILY_KILL"
                denial_detail = "kill_switch_already_hit"

            if decision == "PASS":
                ok, code = _is_in_session(ts_et, sess_rule)
                if not ok:
                    decision = "FAIL"
                    denial_code = code

            if decision == "PASS":
                ok, code = _vix_ok(vix_regime, vix_rule)
                if not ok:
                    decision = "FAIL"
                    denial_code = code

            if decision == "PASS" and max_trades_rule.get("enabled", False):
                if day_trade_count[session_date] >= int(max_trades_rule["max_trades"]):
                    decision = "FAIL"
                    denial_code = "RISK_MAX_TRADES"

            if decision == "PASS" and loss_streak_rule.get("enabled", False):
                if loss_streak >= int(loss_streak_rule["max_losses"]):
                    decision = "FAIL"
                    denial_code = "RISK_LOSS_STREAK"

            cur.execute(
                "INSERT OR REPLACE INTO gate_decisions(run_id, gate_id, signal_id, decision, denial_code, denial_detail, equity_at_decision, risk_at_decision, ts) VALUES (?,?,?,?,?,?,?,?,?)",
                (run_id, gid, signal_id, decision, denial_code, denial_detail, float(equity), None, ts.isoformat())
            )

            if decision == "FAIL":
                continue

            try:
                sh = trades.loc[signal_id]
            except KeyError:
                cur.execute(
                    "UPDATE gate_decisions SET decision=?, denial_code=?, denial_detail=? WHERE run_id=? AND gate_id=? AND signal_id=?",
                    ("FAIL", "DATA_MISSING_TRADE", "signal_id not found in trades for this run_id", run_id, gid, signal_id)
                )
                continue
            entry_px = float(sh["entry_px"])
            stop_px = float(sh["stop_px"])
            pnl_points = float(sh["pnl_points"])

            stop_dist = abs(entry_px - stop_px)
            size = 1.0
            if risk_rule.get("enabled", False):
                cap_pct = float(risk_rule.get("max_risk_pct_equity", 2.0)) / 100.0
                if stop_dist <= 0:
                    cur.execute(
                        "UPDATE gate_decisions SET decision=?, denial_code=?, denial_detail=? WHERE run_id=? AND gate_id=? AND signal_id=?",
                        ("FAIL","RISK_PER_TRADE_CAP","stop_dist<=0", run_id, gid, signal_id)
                    )
                    continue
                size = (equity * cap_pct) / stop_dist
                max_units = risk_rule.get("max_units", None)
                if max_units is not None and size > float(max_units):
                    cur.execute(
                        "UPDATE gate_decisions SET decision=?, denial_code=?, denial_detail=? WHERE run_id=? AND gate_id=? AND signal_id=?",
                        ("FAIL","RISK_OVERSIZE",f"size={size:.2f} > max_units={max_units}", run_id, gid, signal_id)
                    )
                    continue

            pnl = pnl_points * size
            equity += pnl
            day_trade_count[session_date] += 1
            day_pnl[session_date] += pnl
            day_peak_equity[session_date] = max(day_peak_equity[session_date], equity)

            if pnl < 0:
                loss_streak += 1
            else:
                loss_streak = 0

            exit_reason = str(sh["exit_reason"])
            if cooldown_rule.get("enabled", False) and exit_reason == "STOP":
                mins = int(cooldown_rule.get("minutes", 30))
                cooldown_until_ts = ts_et + pd.Timedelta(minutes=mins)

            if kill_rule.get("enabled", False):
                max_dd_pct = float(kill_rule.get("max_intraday_dd_pct", 2.0)) / 100.0
                dd = equity - day_peak_equity[session_date]
                if day_peak_equity[session_date] > 0 and (dd / day_peak_equity[session_date]) <= -max_dd_pct:
                    day_kill[session_date] = True

            trade_id = f"{gid}_{signal_id}"
            pass_trades.append({
                "run_id": run_id,
                "gate_id": gid,
                "trade_id": trade_id,
                "signal_id": signal_id,
                "entry_ts": sh["entry_ts"],
                "exit_ts": sh["exit_ts"],
                "side": sh["side"],
                "entry_px": float(sh["entry_px"]),
                "exit_px": float(sh["exit_px"]),
                "stop_px": float(sh["stop_px"]),
                "target_px": float(sh["target_px"]),
                "pnl": float(pnl),
                "pnl_points": float(pnl_points),
                "size": float(size),
                "mae_points": None,
                "mfe_points": None,
                "hold_minutes": int(sh["minutes_held"]),
                "exit_reason": exit_reason,
            })

        if pass_trades:
            pd.DataFrame(pass_trades).to_sql("trades_pass", conn, if_exists="append", index=False)

        daily_rows = []
        for d in sorted(day_trade_count.keys()):
            dd_day = (day_start_equity[d] + day_pnl[d]) - day_peak_equity[d]
            daily_rows.append({
                "run_id": run_id,
                "gate_id": gid,
                "session_date": d,
                "trades_taken": int(day_trade_count[d]),
                "pnl_day": float(day_pnl[d]),
                "dd_day": float(dd_day),
                "kill_switch_hit": int(bool(day_kill[d])),
            })
        if daily_rows:
            pd.DataFrame(daily_rows).to_sql("gate_daily_stats", conn, if_exists="append", index=False)

        trades = pd.read_sql_query("SELECT * FROM trades_pass WHERE run_id=? AND gate_id=? ORDER BY entry_ts", conn, params=(run_id,gid))
        trades = trades.set_index("signal_id", drop=False)
        m = compute_trade_metrics(trades)
        eq = equity_curve_from_trades(trades, start_equity)
        maxdd = max_drawdown(eq)
        daily = pd.DataFrame(daily_rows) if daily_rows else pd.DataFrame(columns=["pnl_day"])
        worst_day = float(daily["pnl_day"].min()) if not daily.empty else 0.0
        zero_trade_day_pct = float((pd.Series(day_trade_count).values == 0).mean()) if day_trade_count else np.nan

        cur.execute(
            "INSERT OR REPLACE INTO gate_metrics(run_id, gate_id, trade_count, win_rate, pf, expectancy, maxdd, worst_day, worst_trade, avg_hold, zero_trade_day_pct, ending_equity) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
            (run_id, gid, int(m["trade_count"]), m["win_rate"], m["pf"], m["expectancy"], maxdd, worst_day, m["worst_trade"], m["avg_hold"], zero_trade_day_pct, float(eq.iloc[-1]))
        )
        conn.commit()
