# multigate.py
from __future__ import annotations
from src.config import get_config, paths_from_config
from src.viz import write_equity_curve_png
from src.reporting import write_run_report_md
from src.features import join_vix_regime


import json
import re
import sqlite3
import uuid
from datetime import datetime, timezone, time as dt_time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

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


def _as_bool(v: object, default: bool = False) -> bool:
    if v is None:
        return default
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return bool(v)
    if isinstance(v, str):
        return v.strip().lower() in {"1", "true", "yes", "y", "on"}
    return bool(v)


def _parse_clock(value: object, *, default: str) -> dt_time:
    s = str(value if value is not None else default).strip()
    parts = [int(x) for x in s.split(":")]
    if len(parts) == 2:
        hh, mm = parts
        ss = 0
    elif len(parts) == 3:
        hh, mm, ss = parts
    else:
        raise ValueError(f"Invalid time format {s!r}; expected HH:MM or HH:MM:SS")
    return dt_time(hh, mm, ss)


def _resolve_parquet_from_source_json(path: Path) -> Path:
    obj = json.loads(path.read_text(encoding="utf-8"))
    output = obj.get("output") if isinstance(obj, dict) else None
    pq = output.get("parquet") if isinstance(output, dict) else None
    if not pq:
        raise ValueError(f"No output.parquet key found in sidecar: {path}")
    pq_raw = Path(str(pq))
    pq_path = pq_raw.expanduser()
    if not pq_path.is_absolute():
        pq_path = (path.parent / pq_path).resolve()
    if pq_path.exists():
        return pq_path

    # Some historical sidecars carry absolute paths from a prior workspace root.
    sibling = (path.parent / pq_raw.name).resolve()
    if sibling.exists():
        return sibling

    raise FileNotFoundError(
        f"Parquet referenced by sidecar not found: {pq_path}. Also checked sibling fallback: {sibling}"
    )


def _safe_json_load(path: Optional[Path]) -> Dict[str, Any]:
    if path is None:
        return {}
    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}


def _infer_vendor_from_path(path: Path) -> str:
    low = str(path).lower()
    for vendor in ("databento", "alpaca", "vix", "sample"):
        if f"/{vendor}/" in low:
            return vendor
    return "unknown"


def _infer_symbol_from_path(path: Path) -> str:
    parts = [p for p in path.parts if p]
    low_parts = [p.lower() for p in parts]

    for i, part in enumerate(low_parts):
        if part in {"ingested", "raw"} and i + 2 < len(parts):
            raw = parts[i + 2]
            token = re.split(r"[_\-.]", raw)[0]
            if token:
                return token.upper()

    stem = path.stem
    token = re.split(r"[_\-.]", stem)[0]
    return token.upper() if token else "UNK"


def _infer_timeframe(text: str) -> str:
    s = (text or "").lower()
    m = re.search(r"ohlcv[-_]?(\d+[a-z]+)", s)
    if m:
        return m.group(1)
    m = re.search(r"\b(\d+)(min|m|h|hour|d|day)\b", s)
    if m:
        return f"{m.group(1)}{m.group(2)}"
    return ""


def resolve_data_inputs(path: Path) -> Tuple[Path, Optional[Path], Dict[str, Any]]:
    p = Path(path).expanduser()
    if not p.is_absolute():
        p = (HERE / p).resolve()
    if not p.exists():
        raise FileNotFoundError(f"Data path not found: {p}")

    sidecar_path: Optional[Path] = None

    if p.is_file():
        suffix = p.suffix.lower()
        if suffix in {".parquet", ".csv"}:
            resolved = p
            candidate = p.with_suffix(".source.json")
            if candidate.exists():
                sidecar_path = candidate
            return resolved, sidecar_path, _safe_json_load(sidecar_path)
        if p.name.endswith(".source.json"):
            sidecar_path = p
            resolved = _resolve_parquet_from_source_json(p)
            return resolved, sidecar_path, _safe_json_load(sidecar_path)
        raise ValueError(f"Unsupported data file: {p} (expected .parquet/.csv/.source.json)")

    # Directory mode: pick latest parquet (works for data/ingested folders).
    candidates = sorted(
        (q for q in p.rglob("*.parquet") if q.is_file()),
        key=lambda q: q.stat().st_mtime,
        reverse=True,
    )
    if not candidates:
        raise FileNotFoundError(f"No parquet files found under directory: {p}")
    resolved = candidates[0]
    candidate = resolved.with_suffix(".source.json")
    if candidate.exists():
        sidecar_path = candidate
    return resolved, sidecar_path, _safe_json_load(sidecar_path)


def read_table_auto(path: Path) -> pd.DataFrame:
    resolved, _, _ = resolve_data_inputs(path)
    suf = resolved.suffix.lower()
    if suf == ".parquet":
        return pd.read_parquet(resolved)
    if suf == ".csv":
        return pd.read_csv(resolved)
    raise ValueError(f"Unsupported data format: {resolved} (use .parquet or .csv)")


def read_table_auto_with_meta(path: Path) -> Tuple[pd.DataFrame, Path, Optional[Path], Dict[str, Any]]:
    resolved, sidecar_path, sidecar_obj = resolve_data_inputs(path)
    suf = resolved.suffix.lower()
    if suf == ".parquet":
        return pd.read_parquet(resolved), resolved, sidecar_path, sidecar_obj
    if suf == ".csv":
        return pd.read_csv(resolved), resolved, sidecar_path, sidecar_obj
    raise ValueError(f"Unsupported data format: {resolved} (use .parquet or .csv)")


def normalize_spy_5m(df: pd.DataFrame) -> pd.DataFrame:
    # normalize column names
    df = df.copy()
    df.columns = [str(c).strip().lower() for c in df.columns]

    # common timestamp column names
    ts_candidates = ["ts", "timestamp", "time", "datetime", "date", "t", "ts_event"]
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
    if out.index.tz is None:
        out.index = out.index.tz_localize("UTC")
    else:
        out.index = out.index.tz_convert("UTC")
    out.index.name = "ts"

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
        for alt in ("v", "vol", "size"):
            if alt in out.columns:
                rename_map[alt] = "volume"
                break

    out = out.rename(columns=rename_map)

    needed = {"open", "high", "low", "close", "volume"}
    missing = [c for c in needed if c not in out.columns]
    if missing:
        raise ValueError(f"Missing required OHLCV columns: {missing}. Have: {list(out.columns)}")

    for c in needed:
        out[c] = pd.to_numeric(out[c], errors="coerce")
    out = out.dropna(subset=list(needed))

    # sort & drop duplicates
    out = out[~out.index.duplicated(keep="last")].sort_index()
    return out


def apply_rth_filter(
    df: pd.DataFrame,
    *,
    timezone_name: str,
    enforce_rth: bool,
    rth_start: dt_time,
    rth_end: dt_time,
) -> pd.DataFrame:
    if not enforce_rth:
        return df
    ts_local = df.index.tz_convert(timezone_name)
    mask = (ts_local.time >= rth_start) & (ts_local.time <= rth_end)
    return df.loc[mask].copy()


def _classify_regime(day_range_pct: Optional[float]) -> str:
    if day_range_pct is None:
        return "UNKNOWN"
    if day_range_pct < 1.0:
        return "LOW"
    if day_range_pct < 2.5:
        return "NORMAL"
    if day_range_pct < 4.0:
        return "HIGH"
    return "EXTREME"


def build_data_context(
    *,
    requested_path: Path,
    resolved_path: Path,
    source_sidecar_path: Optional[Path],
    source_sidecar_obj: Dict[str, Any],
    spy_raw: pd.DataFrame,
    spy_norm: pd.DataFrame,
    spy_rth: pd.DataFrame,
    timezone_name: str,
    enforce_rth: bool,
    rth_start: dt_time,
    rth_end: dt_time,
) -> Dict[str, Any]:
    req = source_sidecar_obj.get("request") if isinstance(source_sidecar_obj.get("request"), dict) else {}
    req_params = req.get("params") if isinstance(req.get("params"), dict) else {}

    symbol = (
        source_sidecar_obj.get("symbol")
        or req_params.get("symbol")
        or req_params.get("symbols")
        or _infer_symbol_from_path(resolved_path)
    )
    if isinstance(symbol, list):
        symbol = symbol[0] if symbol else "UNK"
    symbol = str(symbol).upper()

    vendor = (
        source_sidecar_obj.get("vendor")
        or source_sidecar_obj.get("provider")
        or _infer_vendor_from_path(resolved_path)
    )
    dataset = (
        source_sidecar_obj.get("dataset_id")
        or req_params.get("dataset")
        or resolved_path.parent.name
    )
    schema = source_sidecar_obj.get("schema") or req_params.get("schema") or ""
    if not schema and "ohlcv" in str(dataset).lower():
        schema = dataset
    feed = source_sidecar_obj.get("feed") or req_params.get("feed") or ""
    timeframe = (
        req_params.get("timeframe")
        or _infer_timeframe(str(schema))
        or _infer_timeframe(str(dataset))
        or _infer_timeframe(resolved_path.name)
        or "unknown"
    )

    return {
        "requested_data_path": str(requested_path),
        "resolved_data_path": str(resolved_path),
        "source_sidecar_path": str(source_sidecar_path) if source_sidecar_path else "",
        "source_vendor": str(vendor).lower(),
        "source_dataset": str(dataset),
        "source_schema": str(schema),
        "source_feed": str(feed),
        "source_symbol": symbol,
        "bar_timeframe": str(timeframe),
        "db_path": str(DB_PATH),
        "raw_row_count": int(len(spy_raw)),
        "normalized_row_count": int(len(spy_norm)),
        "post_rth_row_count": int(len(spy_rth)),
        "raw_columns": [str(c) for c in spy_raw.columns],
        "normalized_columns": [str(c) for c in spy_norm.columns],
        "timezone": timezone_name,
        "rth_enabled": bool(enforce_rth),
        "rth_start": rth_start.isoformat(),
        "rth_end": rth_end.isoformat(),
    }


def _normalize_bias_name(v: Any) -> str:
    s = str(v or "").strip().lower().replace("-", "_")
    if s in {"short", "short_only"}:
        return "short_only"
    if s in {"short_heavy", "mostly_short", "predominantly_short"}:
        return "short_heavy"
    if s in {"long", "long_only"}:
        return "long_only"
    if s in {"long_heavy", "mostly_long", "predominantly_long"}:
        return "long_heavy"
    return "long_only"


def _resolve_symbol_legs(runtime_cfg: Dict[str, Any]) -> List[Dict[str, str]]:
    raw_legs = runtime_cfg.get("symbol_legs")
    legs: List[Dict[str, str]] = []
    if isinstance(raw_legs, list):
        for i, item in enumerate(raw_legs):
            if not isinstance(item, dict):
                continue
            data_path = str(item.get("data_path") or "").strip()
            if not data_path:
                continue
            symbol = str(item.get("symbol") or f"LEG{i+1}").strip().upper()
            bias = _normalize_bias_name(item.get("bias"))
            legs.append({"symbol": symbol, "data_path": data_path, "bias": bias})
    if legs:
        return legs
    default_bias = _normalize_bias_name(runtime_cfg.get("default_bias", "long_only"))
    return [{"symbol": "", "data_path": str(DATA_PATH), "bias": default_bias}]


def make_trades_from_signals(spy: pd.DataFrame, run_id: str, *, symbol: str, bias: str) -> pd.DataFrame:
    """
    Placeholder session strategy with configurable directional bias.

    - 1 trade per day per symbol: enter first bar, exit last bar.
    - `short_heavy` places mostly SHORT trades (occasional LONG rebalance day).
    - `long_only`/`long_heavy` place LONG trades.
    """
    df = spy.copy()
    df["date_utc"] = df.index.tz_convert("UTC").normalize()
    bias_norm = _normalize_bias_name(bias)
    sym = str(symbol or "UNK").upper()

    rows = []
    for day_idx, (date, g) in enumerate(df.groupby("date_utc", sort=True)):
        if g.empty:
            continue
        entry_ts = g.index[0]
        exit_ts = g.index[-1]
        entry_px = float(g["close"].iloc[0])
        exit_px = float(g["close"].iloc[-1])

        if bias_norm == "short_only":
            side = "SHORT"
        elif bias_norm == "short_heavy":
            # 6 of 7 sessions short to keep this leg predominantly short.
            side = "LONG" if (day_idx % 7 == 6) else "SHORT"
        elif bias_norm == "long_heavy":
            side = "SHORT" if (day_idx % 7 == 6) else "LONG"
        else:
            side = "LONG"

        if side == "SHORT":
            stop_px = entry_px * (1 + 0.005)
            target_px = entry_px * (1 - 0.010)
            pnl_points = entry_px - exit_px
        else:
            stop_px = entry_px * (1 - 0.005)
            target_px = entry_px * (1 + 0.010)
            pnl_points = exit_px - entry_px

        minutes_held = int((exit_ts - entry_ts).total_seconds() // 60)
        bars_held = int(len(g))
        signal_id = f"{run_id}_{sym}_{date.strftime('%Y%m%d')}"

        rows.append(
            {
                "run_id": run_id,
                "signal_id": signal_id,
                "symbol": sym,
                "strategy_bias": bias_norm,
                "entry_ts": entry_ts.isoformat(),
                "exit_ts": exit_ts.isoformat(),
                "side": side,
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


def build_signals_from_trades(
    trades: pd.DataFrame,
    symbol_spy: Dict[str, pd.DataFrame],
    symbol_context: Dict[str, Dict[str, Any]],
    *,
    timezone_name: str,
    vix_df: Optional[pd.DataFrame] = None,
) -> pd.DataFrame:
    per_symbol_day_stats: Dict[str, Dict[pd.Timestamp, Dict[str, Any]]] = {}
    for sym, spy in symbol_spy.items():
        tmp = spy.copy()
        vix_by_date: Dict[pd.Timestamp, Any] = {}
        vix_close_by_date: Dict[pd.Timestamp, Any] = {}
        if vix_df is not None and not vix_df.empty:
            try:
                joined = join_vix_regime(tmp, vix_df)
                joined["date_utc"] = joined.index.tz_convert("UTC").normalize()
                vix_by_date = joined.groupby("date_utc", sort=True)["vix_regime"].first().to_dict()
                if "vix_close" in joined.columns:
                    vix_close_by_date = joined.groupby("date_utc", sort=True)["vix_close"].first().to_dict()
            except Exception:
                vix_by_date = {}
                vix_close_by_date = {}
        tmp["date_utc"] = tmp.index.tz_convert("UTC").normalize()
        day_stats: Dict[pd.Timestamp, Dict[str, Any]] = {}
        for date_utc, g in tmp.groupby("date_utc", sort=True):
            session_open = float(g["open"].iloc[0])
            session_close = float(g["close"].iloc[-1])
            session_high = float(g["high"].max())
            session_low = float(g["low"].min())
            session_volume = float(g["volume"].sum())
            day_range_pct = ((session_high - session_low) / session_open * 100.0) if session_open else None
            day_ret_pct = ((session_close - session_open) / session_open * 100.0) if session_open else None
            vix_regime = vix_by_date.get(date_utc, _classify_regime(day_range_pct))
            day_stats[date_utc] = {
                "session_open": round(session_open, 6),
                "session_close": round(session_close, 6),
                "session_high": round(session_high, 6),
                "session_low": round(session_low, 6),
                "session_volume": round(session_volume, 2),
                "day_range_pct": round(day_range_pct, 6) if day_range_pct is not None else None,
                "day_return_pct": round(day_ret_pct, 6) if day_ret_pct is not None else None,
                "vix_regime": str(vix_regime) if vix_regime is not None else "UNKNOWN",
                "vix_close": vix_close_by_date.get(date_utc),
                "bars_in_session": int(len(g)),
            }
        per_symbol_day_stats[sym] = day_stats

    signal_rows: List[Dict[str, Any]] = []
    for _, tr in trades.iterrows():
        entry_ts = pd.to_datetime(tr["entry_ts"], errors="coerce", utc=True)
        exit_ts = pd.to_datetime(tr["exit_ts"], errors="coerce", utc=True)
        if pd.isna(entry_ts) or pd.isna(exit_ts):
            continue

        symbol = str(tr.get("symbol", "UNK") or "UNK").upper()
        data_context = symbol_context.get(symbol, {})
        schema = str(data_context.get("source_schema") or "unknown")
        timeframe = str(data_context.get("bar_timeframe") or "unknown")
        vendor = str(data_context.get("source_vendor") or "unknown")
        bias = str(tr.get("strategy_bias", "") or "")
        session_key = entry_ts.normalize()
        st = per_symbol_day_stats.get(symbol, {}).get(session_key, {})
        side = str(tr.get("side", "LONG")).upper()
        direction = 1 if side == "LONG" else -1 if side == "SHORT" else 0
        entry_px = float(tr.get("entry_px", 0.0))
        stop_px = float(tr.get("stop_px", 0.0))
        target_px = float(tr.get("target_px", 0.0))
        exit_px = float(tr.get("exit_px", 0.0))
        pnl_points = float(tr.get("pnl_points", 0.0))
        risk_points = abs(entry_px - stop_px)
        reward_points = abs(target_px - entry_px)
        rr = (reward_points / risk_points) if risk_points > 0 else None
        ret_pct = ((exit_px - entry_px) / entry_px * 100.0) if entry_px else None

        signal_desc = (
            f"{symbol} {side} session trade from first to final bar "
            f"(bias={bias or 'n/a'}, schema={schema}, timeframe={timeframe})."
        )

        features = {
            "signal_type": "SESSION_OPEN_TO_CLOSE",
            "signal_description": signal_desc,
            "strategy_rationale": (
                f"Paired multi-symbol hedge strategy: {symbol} configured with bias={bias or 'long_only'}, "
                "entered at first session bar and exited at close for gate benchmarking."
            ),
            "symbol": symbol,
            "strategy_bias": bias,
            "direction": side,
            "source_vendor": vendor,
            "source_dataset": data_context.get("source_dataset"),
            "source_schema": schema,
            "source_feed": data_context.get("source_feed"),
            "bar_timeframe": timeframe,
            "session_date": str(entry_ts.tz_convert(timezone_name).date()),
            "signal_ts_et": entry_ts.tz_convert(timezone_name).strftime("%Y-%m-%d %H:%M:%S %Z"),
            "entry_ts_utc": entry_ts.isoformat(),
            "exit_ts_utc": exit_ts.isoformat(),
            "entry_ts_et": entry_ts.tz_convert(timezone_name).strftime("%Y-%m-%d %H:%M:%S %Z"),
            "exit_ts_et": exit_ts.tz_convert(timezone_name).strftime("%Y-%m-%d %H:%M:%S %Z"),
            "entry_px": round(entry_px, 6),
            "stop_px": round(stop_px, 6),
            "target_px": round(target_px, 6),
            "exit_px": round(exit_px, 6),
            "pnl_points": round(pnl_points, 6),
            "realized_return_pct": round(ret_pct, 6) if ret_pct is not None else None,
            "planned_rr": round(rr, 6) if rr is not None else None,
            "bars_held": int(tr.get("bars_held", 0)),
            "minutes_held": int(tr.get("minutes_held", 0)),
            "exit_reason": str(tr.get("exit_reason", "")),
            "vix_regime": st.get("vix_regime", "UNKNOWN"),
            "vix_close": st.get("vix_close"),
            "session_open": st.get("session_open"),
            "session_close": st.get("session_close"),
            "session_high": st.get("session_high"),
            "session_low": st.get("session_low"),
            "session_volume": st.get("session_volume"),
            "day_range_pct": st.get("day_range_pct"),
            "day_return_pct": st.get("day_return_pct"),
            "bars_in_session": st.get("bars_in_session"),
        }

        signal_rows.append(
            {
                "run_id": tr["run_id"],
                "signal_id": tr["signal_id"],
                "ts": entry_ts.isoformat(),
                "direction": direction,
                "features_json": json.dumps(features, separators=(",", ":"), ensure_ascii=True),
            }
        )

    return pd.DataFrame(signal_rows)


def insert_run_row(
    conn: sqlite3.Connection,
    run_id: str,
    spy: pd.DataFrame,
    report_path: str,
    equity_curve_path: str,
    data_context: Dict[str, Any],
) -> None:
    # matches your current runs schema (PRAGMA table_info(runs) you pasted earlier)
    created_at_utc = datetime.now(timezone.utc).isoformat()
    date_start_et = spy.index.min().tz_convert("America/New_York").date().isoformat()
    date_end_et = spy.index.max().tz_convert("America/New_York").date().isoformat()

    params = {
        "data_path": str(data_context.get("requested_data_path") or DATA_PATH),
        "strategy_version": STRATEGY_VERSION,
        "spec_version": SPEC_VERSION,
        "gate_ids": GATE_IDS,
        "start_equity": START_EQUITY,
        **data_context,
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
            equity_curve_path,
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

def _load_vix_dataset(cfg: Dict[str, Any]) -> Tuple[Optional[pd.DataFrame], Dict[str, Any]]:
    features_cfg = cfg.get("data_features", {}) or {}
    if not _as_bool(features_cfg.get("compute_vix_regime"), default=True):
        return None, {}

    candidates = []
    explicit = features_cfg.get("vix_data_path")
    if explicit:
        candidates.append(Path(str(explicit)))
    candidates.extend(
        [
            HERE / "data" / "ingested" / "vix" / "cboe-daily",
            HERE / "data" / "ingested" / "vix" / "fred-daily",
        ]
    )

    for candidate in candidates:
        if candidate.exists():
            vix_df, resolved, sidecar_path, sidecar_obj = read_table_auto_with_meta(candidate)
            vix_ctx = {
                "vix_requested_path": str(candidate),
                "vix_resolved_path": str(resolved),
                "vix_sidecar_path": str(sidecar_path) if sidecar_path else "",
                "vix_symbol": sidecar_obj.get("symbol") or sidecar_obj.get("request", {}).get("params", {}).get("symbol"),
                "vix_provider": sidecar_obj.get("provider") or sidecar_obj.get("vendor") or "unknown",
                "vix_schema": sidecar_obj.get("schema") or sidecar_obj.get("request", {}).get("params", {}).get("schema"),
                "vix_rows": int(len(vix_df)),
            }
            return vix_df, vix_ctx

    print("[vix] No ingested VIX dataset found; falling back to intraday range proxy.")
    return None, {}

def main() -> int:
    run_id = utc_run_id()
    runtime_cfg = cfg.get("runtime", {}) or {}
    tz_name = str(runtime_cfg.get("timezone", "America/New_York"))
    enforce_rth = _as_bool(runtime_cfg.get("enforce_rth"), default=True)
    rth_start = _parse_clock(runtime_cfg.get("rth_start"), default="09:45")
    rth_end = _parse_clock(runtime_cfg.get("rth_end"), default="16:00")
    legs = _resolve_symbol_legs(runtime_cfg)
    symbol_spy: Dict[str, pd.DataFrame] = {}
    symbol_context: Dict[str, Dict[str, Any]] = {}
    leg_context_rows: List[Dict[str, Any]] = []
    trades_parts: List[pd.DataFrame] = []

    for leg in legs:
        requested_path = Path(leg["data_path"]).expanduser()
        spy_raw, resolved_data_path, source_sidecar_path, source_sidecar_obj = read_table_auto_with_meta(requested_path)
        spy_norm = normalize_spy_5m(spy_raw)
        spy = apply_rth_filter(
            spy_norm,
            timezone_name=tz_name,
            enforce_rth=enforce_rth,
            rth_start=rth_start,
            rth_end=rth_end,
        )
        if spy.empty:
            raise RuntimeError(
                f"No rows after normalization/RTH filter for symbol leg {leg.get('symbol') or 'UNK'} from {requested_path}."
            )

        data_context = build_data_context(
            requested_path=requested_path,
            resolved_path=resolved_data_path,
            source_sidecar_path=source_sidecar_path,
            source_sidecar_obj=source_sidecar_obj,
            spy_raw=spy_raw,
            spy_norm=spy_norm,
            spy_rth=spy,
            timezone_name=tz_name,
            enforce_rth=enforce_rth,
            rth_start=rth_start,
            rth_end=rth_end,
        )

        symbol = str(leg.get("symbol") or data_context.get("source_symbol") or "UNK").upper()
        bias = _normalize_bias_name(leg.get("bias"))
        data_context["source_symbol"] = symbol
        data_context["strategy_bias"] = bias
        symbol_spy[symbol] = spy
        symbol_context[symbol] = data_context
        leg_context_rows.append(
            {
                "symbol": symbol,
                "bias": bias,
                "requested_data_path": data_context.get("requested_data_path"),
                "resolved_data_path": data_context.get("resolved_data_path"),
                "source_vendor": data_context.get("source_vendor"),
                "source_dataset": data_context.get("source_dataset"),
                "source_schema": data_context.get("source_schema"),
                "bar_timeframe": data_context.get("bar_timeframe"),
                "rows_post_rth": data_context.get("post_rth_row_count"),
            }
        )
        trades_leg = make_trades_from_signals(spy, run_id, symbol=symbol, bias=bias)
        if not trades_leg.empty:
            trades_parts.append(trades_leg)

    if not trades_parts:
        raise RuntimeError("No trades generated for configured symbol legs.")

    trades = pd.concat(trades_parts, ignore_index=True)
    vix_df, vix_ctx = _load_vix_dataset(cfg)
    ref_spy = pd.concat([df[["close"]] for df in symbol_spy.values()], axis=0).sort_index()

    symbols = sorted(symbol_context.keys())
    vendors = sorted({str(c.get("source_vendor") or "unknown") for c in symbol_context.values()})
    datasets = sorted({str(c.get("source_dataset") or "unknown") for c in symbol_context.values()})
    schemas = sorted({str(c.get("source_schema") or "unknown") for c in symbol_context.values()})
    timeframes = sorted({str(c.get("bar_timeframe") or "unknown") for c in symbol_context.values()})
    requested_paths = [str(c.get("requested_data_path") or "") for c in symbol_context.values()]
    resolved_paths = [str(c.get("resolved_data_path") or "") for c in symbol_context.values()]

    first_symbol = symbols[0]
    data_context = dict(symbol_context[first_symbol])
    data_context.update(
        {
            "source_symbol": ",".join(symbols),
            "source_vendor": vendors[0] if len(vendors) == 1 else "mixed",
            "source_dataset": datasets[0] if len(datasets) == 1 else "mixed",
            "source_schema": schemas[0] if len(schemas) == 1 else "mixed",
            "bar_timeframe": timeframes[0] if len(timeframes) == 1 else "mixed",
            "requested_data_path": " | ".join(requested_paths),
            "resolved_data_path": " | ".join(resolved_paths),
            "raw_row_count": int(sum(int(c.get("raw_row_count") or 0) for c in symbol_context.values())),
            "normalized_row_count": int(sum(int(c.get("normalized_row_count") or 0) for c in symbol_context.values())),
            "post_rth_row_count": int(sum(int(c.get("post_rth_row_count") or 0) for c in symbol_context.values())),
            "symbol_legs": leg_context_rows,
            **vix_ctx,
        }
    )

    # connect DB + write
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH))
    import src.gate_eval as ge
    if hasattr(ge, "ensure_tables"):
        ge.ensure_tables(conn)

    # write trades into existing table schema
    # (must match your PRAGMA table_info(trades))
    required_cols = [
        "run_id","signal_id","entry_ts","exit_ts","side",
        "entry_px","exit_px","stop_px","target_px",
        "pnl_points","bars_held","minutes_held","exit_reason"
    ]
    trades_sql = trades[required_cols].copy()

    # If no trades, stop early (but don’t crash gates)
    if trades_sql.empty:
        raise RuntimeError("No trades generated (even the placeholder strategy made none). Check DATA_PATH and parsing.")

    trades_sql.to_sql("trades", conn, if_exists="append", index=False)

    # gate eval writes gate_decisions + gate_metrics
    gates = load_gate_pack(GATES_DIR, GATE_IDS)
    # -------------------------
    # Persist signals + trades BEFORE gate eval
    # gate_eval requires BOTH tables to already contain this run_id.
    # -------------------------

    # --- enriched signals table for gate_eval + reporting ---
    signals_df = build_signals_from_trades(
        trades,
        symbol_spy=symbol_spy,
        symbol_context=symbol_context,
        timezone_name=tz_name,
        vix_df=vix_df,
    )

    signals_df.to_sql("signals", conn, if_exists="append", index=False)


    # --- end signals ---
    evaluate_gates(conn, run_id, gates, start_equity=START_EQUITY)


    # write run row before report generation so report metadata queries can see it.
    # --- equity curve chart ---
    equity_png = HERE / "runs" / "artifacts" / "charts" / f"{run_id}_equity.png"
    report_path = HERE / "runs" / "artifacts" / "reports" / f"{run_id}_multigate_report.md"
    insert_run_row(
        conn,
        run_id,
        ref_spy,
        str(report_path),
        str(equity_png),
        data_context,
    )

    write_equity_curve_png(conn, run_id, equity_png, START_EQUITY)

    # --- report md ---
    report_md = report_path
    write_run_report_md(conn, run_id, report_md, start_equity=START_EQUITY)

    conn.commit()
    conn.close()

    print(f"✅ Completed run_id={run_id}")
    print(f"DB: {DB_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
