#!/usr/bin/env python3
from __future__ import annotations

import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

import sqlite3


REPO_ROOT = Path(__file__).resolve().parents[1]
GATE_PATH = REPO_ROOT / "gates" / "G1.json"
DB_PATH = REPO_ROOT / "runs" / "backtests.sqlite"
RESULTS_PATH = REPO_ROOT / "runs" / "artifacts" / "g1_quality_tuning_results.json"


def _load_json(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, obj: Dict[str, Any]) -> None:
    path.write_text(json.dumps(obj, indent=2, ensure_ascii=True), encoding="utf-8")


def _run_backtest() -> str:
    proc = subprocess.run(
        [sys.executable, str(REPO_ROOT / "multigate.py")],
        cwd=str(REPO_ROOT),
        check=True,
        capture_output=True,
        text=True,
    )
    run_id = ""
    for line in (proc.stdout or "").splitlines():
        if "Completed run_id=" in line:
            run_id = line.split("Completed run_id=")[-1].strip()
            break
    if not run_id:
        raise RuntimeError(f"Could not parse run_id from output:\n{proc.stdout}\n{proc.stderr}")
    return run_id


def _fetch_gate_metrics(conn: sqlite3.Connection, run_id: str, gate_id: str) -> Dict[str, Any]:
    row = conn.execute(
        """
        SELECT trade_count, win_rate, pf, expectancy, maxdd, ending_equity
        FROM gate_metrics WHERE run_id=? AND gate_id=?
        """,
        (run_id, gate_id),
    ).fetchone()
    if not row:
        return {}
    trade_count, win_rate, pf, expectancy, maxdd, ending_equity = row
    return {
        "trades": trade_count,
        "win_rate": win_rate,
        "pf": pf,
        "expectancy": expectancy,
        "maxdd": maxdd,
        "end_eq": ending_equity,
    }


def _fetch_gate_pass_fail(conn: sqlite3.Connection, run_id: str, gate_id: str) -> Dict[str, int]:
    row = conn.execute(
        """
        SELECT
          SUM(CASE WHEN decision='PASS' THEN 1 ELSE 0 END) AS pass_n,
          SUM(CASE WHEN decision='FAIL' THEN 1 ELSE 0 END) AS fail_n
        FROM gate_decisions
        WHERE run_id=? AND gate_id=?
        """,
        (run_id, gate_id),
    ).fetchone()
    if not row:
        return {"pass_n": 0, "fail_n": 0}
    return {"pass_n": int(row[0] or 0), "fail_n": int(row[1] or 0)}


def _score(m: Dict[str, Any]) -> float:
    if not m:
        return float("-inf")
    end_eq = float(m.get("end_eq") or 0.0)
    maxdd = float(m.get("maxdd") or 0.0)
    pf = float(m.get("pf") or 0.0)
    expectancy = float(m.get("expectancy") or 0.0)
    trades = float(m.get("trades") or 0.0)
    return end_eq - abs(maxdd) + (pf * 1000.0) + (expectancy * 25.0) + (trades * 0.1)


def _set_rule(rules: Dict[str, Any], path: List[str], value: Any) -> None:
    cur = rules
    for key in path[:-1]:
        cur = cur.setdefault(key, {})
    cur[path[-1]] = value


def main() -> int:
    original = _load_json(GATE_PATH)
    candidates = [
        {
            "tag": "g1_strict",
            "risk_pct": 0.5,
            "max_units": 1200,
            "max_losses": 4,
            "max_trades": 4,
            "kill": 1.0,
            "cooldown": 30,
            "vix": ["NORMAL"],
        },
        {
            "tag": "g1_balanced",
            "risk_pct": 0.75,
            "max_units": 1500,
            "max_losses": 5,
            "max_trades": 6,
            "kill": 1.5,
            "cooldown": 20,
            "vix": ["LOW", "NORMAL"],
        },
        {
            "tag": "g1_growth",
            "risk_pct": 1.0,
            "max_units": 2000,
            "max_losses": 6,
            "max_trades": 8,
            "kill": 2.0,
            "cooldown": 10,
            "vix": ["LOW", "NORMAL", "HIGH"],
        },
        {
            "tag": "g1_defensive",
            "risk_pct": 0.6,
            "max_units": 1000,
            "max_losses": 4,
            "max_trades": 5,
            "kill": 1.25,
            "cooldown": 30,
            "vix": ["NORMAL", "HIGH"],
        },
    ]

    results = []
    try:
        for cand in candidates:
            cfg = json.loads(json.dumps(original))
            rules = cfg.get("rules", {})
            _set_rule(rules, ["per_trade_risk_cap", "enabled"], True)
            _set_rule(rules, ["per_trade_risk_cap", "max_risk_pct_equity"], cand["risk_pct"])
            _set_rule(rules, ["per_trade_risk_cap", "max_units"], cand["max_units"])
            _set_rule(rules, ["max_consecutive_losses", "enabled"], True)
            _set_rule(rules, ["max_consecutive_losses", "max_losses"], cand["max_losses"])
            _set_rule(rules, ["max_trades_per_day", "enabled"], True)
            _set_rule(rules, ["max_trades_per_day", "max_trades"], cand["max_trades"])
            _set_rule(rules, ["daily_kill_switch", "enabled"], True)
            _set_rule(rules, ["daily_kill_switch", "max_intraday_dd_pct"], cand["kill"])
            _set_rule(rules, ["cooldown_minutes_after_stop", "enabled"], True)
            _set_rule(rules, ["cooldown_minutes_after_stop", "minutes"], cand["cooldown"])
            _set_rule(rules, ["vix_regime_filter", "enabled"], True)
            _set_rule(rules, ["vix_regime_filter", "allowed_regimes"], cand["vix"])

            _write_json(GATE_PATH, cfg)
            run_id = _run_backtest()

            conn = sqlite3.connect(str(DB_PATH))
            g0 = _fetch_gate_metrics(conn, run_id, "G0")
            g1 = _fetch_gate_metrics(conn, run_id, "G1")
            g1_pf = _fetch_gate_pass_fail(conn, run_id, "G1")
            conn.close()

            entry = {
                "tag": cand["tag"],
                "risk_pct": cand["risk_pct"],
                "max_units": cand["max_units"],
                "max_losses": cand["max_losses"],
                "max_trades": cand["max_trades"],
                "kill": cand["kill"],
                "cooldown": cand["cooldown"],
                "vix": cand["vix"],
                "run_id": run_id,
                "G0_trades": g0.get("trades"),
                "G0_win_rate": g0.get("win_rate"),
                "G0_pf": g0.get("pf"),
                "G0_expectancy": g0.get("expectancy"),
                "G0_maxdd": g0.get("maxdd"),
                "G0_end_eq": g0.get("end_eq"),
                "G1_trades": g1.get("trades"),
                "G1_win_rate": g1.get("win_rate"),
                "G1_pf": g1.get("pf"),
                "G1_expectancy": g1.get("expectancy"),
                "G1_maxdd": g1.get("maxdd"),
                "G1_end_eq": g1.get("end_eq"),
                "G1_pass_n": g1_pf.get("pass_n"),
                "G1_fail_n": g1_pf.get("fail_n"),
            }
            entry["score"] = _score(g1)
            results.append(entry)
    finally:
        _write_json(GATE_PATH, original)

    ranked = sorted(results, key=lambda r: r.get("score", float("-inf")), reverse=True)
    payload = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "ranked": ranked,
        "best": ranked[0] if ranked else None,
    }
    RESULTS_PATH.parent.mkdir(parents=True, exist_ok=True)
    _write_json(RESULTS_PATH, payload)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
