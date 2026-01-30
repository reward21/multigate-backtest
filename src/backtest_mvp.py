from __future__ import annotations
from dataclasses import dataclass
import pandas as pd
from datetime import datetime

ET = "America/New_York"

def in_entry_window(ts_et: pd.Timestamp) -> bool:
    t = ts_et.timetz()
    return (t >= datetime(2000,1,1,9,45).time()) and (t <= datetime(2000,1,1,15,0).time())

def is_force_flat(ts_et: pd.Timestamp) -> bool:
    return ts_et.timetz() >= datetime(2000,1,1,15,30).time()

def time_bucket(ts_et: pd.Timestamp) -> str:
    t = ts_et.timetz()
    if t < datetime(2000,1,1,11,0).time():
        return "09:45-11:00"
    if t < datetime(2000,1,1,13,0).time():
        return "11:00-13:00"
    return "13:00-15:00"

def run_backtest(df: pd.DataFrame, allowed_regimes={"NORMAL"}):
    df = df.copy()
    df["date_et"] = df.index.tz_convert(ET).normalize()
    position = None
    stopped_out = {}
    pullback = {}
    equity_val = 0.0
    equity = []
    trades = []

    for ts, row in df.iterrows():
        ts_et = ts.tz_convert(ET)
        day = ts_et.normalize()
        pullback.setdefault(day, {"long": False, "short": False})
        stopped_out.setdefault(day, False)

        if pd.isna(row.get("atr14")) or pd.isna(row.get("sma50")) or pd.isna(row.get("vwap")):
            equity.append((ts, equity_val))
            continue

        if position is not None and is_force_flat(ts_et):
            exit_px = row["close"]
            pnl = (exit_px - position["entry_px"]) * (1 if position["side"]=="LONG" else -1)
            equity_val += pnl
            trades.append({**position, "exit_ts": ts, "exit_px": exit_px, "pnl": pnl, "exit_reason": "FORCE_FLAT"})
            position = None

        if position is not None:
            position["bars_held"] += 1
            if position["side"] == "LONG":
                hit_stop = row["low"] <= position["stop"]
                hit_tgt = row["high"] >= position["target"]
                if hit_stop and hit_tgt:
                    exit_px, reason = position["stop"], "STOP"
                elif hit_stop:
                    exit_px, reason = position["stop"], "STOP"
                elif hit_tgt:
                    exit_px, reason = position["target"], "TARGET"
                elif position["bars_held"] >= 12:
                    exit_px, reason = row["close"], "TIME_STOP"
                else:
                    equity.append((ts, equity_val))
                    continue
                pnl = exit_px - position["entry_px"]
            else:
                hit_stop = row["high"] >= position["stop"]
                hit_tgt = row["low"] <= position["target"]
                if hit_stop and hit_tgt:
                    exit_px, reason = position["stop"], "STOP"
                elif hit_stop:
                    exit_px, reason = position["stop"], "STOP"
                elif hit_tgt:
                    exit_px, reason = position["target"], "TARGET"
                elif position["bars_held"] >= 12:
                    exit_px, reason = row["close"], "TIME_STOP"
                else:
                    equity.append((ts, equity_val))
                    continue
                pnl = position["entry_px"] - exit_px

            equity_val += pnl
            if reason == "STOP":
                stopped_out[day] = True
            trades.append({**position, "exit_ts": ts, "exit_px": exit_px, "pnl": pnl, "exit_reason": reason})
            position = None
            equity.append((ts, equity_val))
            continue

        # flat: entry checks
        if row.get("vix_regime","UNKNOWN") not in allowed_regimes:
            equity.append((ts, equity_val))
            continue
        if not in_entry_window(ts_et) or stopped_out[day]:
            equity.append((ts, equity_val))
            continue

        uptrend = (row["close"] > row["sma50"]) and (row["sma50_slope"] > 0)
        downtrend = (row["close"] < row["sma50"]) and (row["sma50_slope"] < 0)

        if uptrend:
            dip_vwap = row["close"] < row["vwap"]
            dip_atr = (row["roll20_high"] - row["close"]) >= (0.5 * row["atr14"])
            if dip_vwap or dip_atr:
                pullback[day]["long"] = True
        if downtrend:
            rally_vwap = row["close"] > row["vwap"]
            rally_atr = (row["close"] - row["roll20_low"]) >= (0.5 * row["atr14"])
            if rally_vwap or rally_atr:
                pullback[day]["short"] = True

        if uptrend and pullback[day]["long"] and (row["close"] > row["vwap"]):
            entry_px = float(row["close"])
            atr = float(row["atr14"])
            position = {
                "entry_ts": ts,
                "side": "LONG",
                "entry_px": entry_px,
                "stop": entry_px - 1.0*atr,
                "target": entry_px + 1.5*atr,
                "bars_held": 0,
                "vix_regime": row.get("vix_regime","UNKNOWN"),
                "weekday": ts_et.day_name(),
                "entry_bucket": time_bucket(ts_et),
                "day": day.tz_localize(None),
            }
            pullback[day]["long"] = False
        elif downtrend and pullback[day]["short"] and (row["close"] < row["vwap"]):
            entry_px = float(row["close"])
            atr = float(row["atr14"])
            position = {
                "entry_ts": ts,
                "side": "SHORT",
                "entry_px": entry_px,
                "stop": entry_px + 1.0*atr,
                "target": entry_px - 1.5*atr,
                "bars_held": 0,
                "vix_regime": row.get("vix_regime","UNKNOWN"),
                "weekday": ts_et.day_name(),
                "entry_bucket": time_bucket(ts_et),
                "day": day.tz_localize(None),
            }

        equity.append((ts, equity_val))

    trades_df = pd.DataFrame(trades)
    equity_df = pd.DataFrame(equity, columns=["timestamp","equity"]).set_index("timestamp").sort_index()
    return trades_df, equity_df
