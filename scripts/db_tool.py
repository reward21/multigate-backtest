#!/usr/bin/env python3
"""SQLite helper for multigate-backtest.

Provides:
- terminal-first querying (`tables`, `schema`, `query`, `recent-runs`)
- lightweight browser UI (`web`) for visual/interactive exploration
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import socket
import sqlite3
import statistics
import subprocess
import time
import uuid
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple
from urllib.parse import parse_qs, urlparse
import urllib.error
import urllib.request

try:
    from tabulate import tabulate
except Exception:  # pragma: no cover
    tabulate = None


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DB_PATH = REPO_ROOT / "runs" / "backtests.sqlite"
DEFAULT_MEMORY_DB_PATH = REPO_ROOT / "runs" / "llm_memory.sqlite"


@dataclass
class QueryResult:
    columns: List[str]
    rows: List[Tuple[Any, ...]]
    elapsed_ms: float


def connect_ro(db_path: Path) -> sqlite3.Connection:
    uri = f"file:{db_path.resolve()}?mode=ro"
    return sqlite3.connect(uri, uri=True)


def connect_rw(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    return sqlite3.connect(str(db_path.resolve()))


def ensure_llm_memory_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS llm_memory (
          memory_id INTEGER PRIMARY KEY AUTOINCREMENT,
          created_at_utc TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
          updated_at_utc TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
          run_id TEXT NOT NULL,
          analysis_mode TEXT NOT NULL,
          model TEXT NOT NULL,
          user_prompt TEXT NOT NULL DEFAULT '',
          query_sql TEXT NOT NULL DEFAULT '',
          query_limit INTEGER NOT NULL DEFAULT 0,
          query_row_count INTEGER NOT NULL DEFAULT 0,
          include_report INTEGER NOT NULL DEFAULT 0,
          context_truncated INTEGER NOT NULL DEFAULT 0,
          context_chars INTEGER NOT NULL DEFAULT 0,
          context_hash TEXT NOT NULL DEFAULT '',
          response_markdown TEXT NOT NULL DEFAULT '',
          error_text TEXT NOT NULL DEFAULT ''
        );
        CREATE INDEX IF NOT EXISTS idx_llm_memory_run_id_created
          ON llm_memory(run_id, created_at_utc DESC, memory_id DESC);
        CREATE INDEX IF NOT EXISTS idx_llm_memory_created
          ON llm_memory(created_at_utc DESC, memory_id DESC);
        CREATE INDEX IF NOT EXISTS idx_llm_memory_run_mode_created
          ON llm_memory(run_id, analysis_mode, created_at_utc DESC, memory_id DESC);
        CREATE INDEX IF NOT EXISTS idx_llm_memory_context_hash
          ON llm_memory(context_hash);
        CREATE TABLE IF NOT EXISTS llm_chunk_stages (
          stage_id TEXT PRIMARY KEY,
          created_at_utc TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
          updated_at_utc TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
          run_id TEXT NOT NULL,
          analysis_mode TEXT NOT NULL DEFAULT 'chunk',
          model TEXT NOT NULL,
          user_prompt TEXT NOT NULL DEFAULT '',
          base_sql TEXT NOT NULL,
          include_report INTEGER NOT NULL DEFAULT 0,
          timeout_s INTEGER NOT NULL DEFAULT 300,
          context_max_chars INTEGER NOT NULL DEFAULT 90000,
          chunk_size INTEGER NOT NULL,
          total_rows INTEGER NOT NULL DEFAULT 0,
          total_chunks INTEGER NOT NULL DEFAULT 0,
          processed_chunks INTEGER NOT NULL DEFAULT 0,
          error_chunks INTEGER NOT NULL DEFAULT 0,
          status TEXT NOT NULL DEFAULT 'staged',
          last_error TEXT NOT NULL DEFAULT ''
        );
        CREATE TABLE IF NOT EXISTS llm_chunk_items (
          item_id INTEGER PRIMARY KEY AUTOINCREMENT,
          stage_id TEXT NOT NULL,
          chunk_index INTEGER NOT NULL,
          offset_rows INTEGER NOT NULL,
          limit_rows INTEGER NOT NULL,
          row_count INTEGER NOT NULL DEFAULT 0,
          chunk_thumb TEXT NOT NULL DEFAULT '',
          context_hash TEXT NOT NULL DEFAULT '',
          memory_id INTEGER,
          status TEXT NOT NULL DEFAULT 'queued',
          started_at_utc TEXT,
          finished_at_utc TEXT,
          error_text TEXT NOT NULL DEFAULT '',
          UNIQUE(stage_id, chunk_index)
        );
        CREATE INDEX IF NOT EXISTS idx_llm_chunk_items_stage_status_idx
          ON llm_chunk_items(stage_id, status, chunk_index);
        CREATE INDEX IF NOT EXISTS idx_llm_chunk_items_stage_thumb
          ON llm_chunk_items(stage_id, chunk_thumb);
        CREATE INDEX IF NOT EXISTS idx_llm_chunk_stages_run_created
          ON llm_chunk_stages(run_id, created_at_utc DESC);
        """
    )
    conn.commit()


def tune_llm_memory_db(conn: sqlite3.Connection) -> None:
    # Sidecar memory DB: favor local durability/speed for frequent small writes.
    try:
        conn.execute("PRAGMA journal_mode=WAL;")
    except Exception:
        pass
    try:
        conn.execute("PRAGMA synchronous=NORMAL;")
    except Exception:
        pass
    try:
        conn.execute("PRAGMA temp_store=MEMORY;")
    except Exception:
        pass
    try:
        conn.execute("PRAGMA cache_size=-8000;")
    except Exception:
        pass


def save_llm_memory_entry(
    conn: sqlite3.Connection,
    *,
    run_id: str,
    analysis_mode: str,
    model: str,
    user_prompt: str,
    query_sql: str,
    query_limit: int,
    query_row_count: int,
    include_report: bool,
    context_truncated: bool,
    context_chars: int,
    context_hash: str,
    response_markdown: str,
    error_text: str = "",
) -> int:
    cur = conn.execute(
        """
        INSERT INTO llm_memory (
          run_id, analysis_mode, model, user_prompt, query_sql, query_limit, query_row_count,
          include_report, context_truncated, context_chars, context_hash, response_markdown, error_text
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            str(run_id or "").strip(),
            str(analysis_mode or "run").strip(),
            str(model or "").strip(),
            str(user_prompt or ""),
            str(query_sql or ""),
            int(query_limit or 0),
            int(query_row_count or 0),
            1 if include_report else 0,
            1 if context_truncated else 0,
            int(context_chars or 0),
            str(context_hash or ""),
            str(response_markdown or ""),
            str(error_text or ""),
        ),
    )
    conn.commit()
    return int(cur.lastrowid)


def _now_utc_iso() -> str:
    cur = sqlite3.connect(":memory:").execute("SELECT strftime('%Y-%m-%dT%H:%M:%fZ', 'now')")
    return str(cur.fetchone()[0] or "")


def _mini_thumbprint(*parts: Any) -> str:
    joined = "|".join(str(p or "") for p in parts)
    return hashlib.blake2s(joined.encode("utf-8"), digest_size=4).hexdigest()


def _count_sql_rows(conn: sqlite3.Connection, sql: str) -> int:
    res = run_sql(conn, f"SELECT COUNT(*) AS n FROM ({sql.strip().rstrip(';')})", limit=1)
    if not res.rows:
        return 0
    try:
        return int(res.rows[0][0] or 0)
    except Exception:
        return 0


def _make_chunk_sql(base_sql: str, *, chunk_limit: int, chunk_offset: int) -> str:
    n = max(1, int(chunk_limit))
    off = max(0, int(chunk_offset))
    cleaned = base_sql.strip().rstrip(";")
    return f"SELECT * FROM ({cleaned}) LIMIT {n} OFFSET {off}"


def _load_stage(conn: sqlite3.Connection, stage_id: str) -> Dict[str, Any]:
    cur = conn.execute(
        """
        SELECT
          stage_id, created_at_utc, updated_at_utc, run_id, analysis_mode, model, user_prompt, base_sql,
          include_report, timeout_s, context_max_chars, chunk_size, total_rows, total_chunks,
          processed_chunks, error_chunks, status, last_error
        FROM llm_chunk_stages
        WHERE stage_id = ?
        LIMIT 1
        """,
        (stage_id,),
    )
    row = cur.fetchone()
    if not row:
        return {}
    cols = [d[0] for d in (cur.description or [])]
    out = {cols[i]: row[i] for i in range(len(cols))}
    total = max(int(out.get("total_chunks") or 0), 0)
    done = max(int(out.get("processed_chunks") or 0) + int(out.get("error_chunks") or 0), 0)
    out["progress_pct"] = round((100.0 * done / total), 2) if total > 0 else 0.0
    out["remaining_chunks"] = max(total - done, 0)
    return out


def _refresh_stage_counts(conn: sqlite3.Connection, stage_id: str) -> Dict[str, Any]:
    cur = conn.execute(
        """
        SELECT
          SUM(CASE WHEN status='processed' THEN 1 ELSE 0 END) AS processed_chunks,
          SUM(CASE WHEN status='error' THEN 1 ELSE 0 END) AS error_chunks,
          SUM(CASE WHEN status='queued' THEN 1 ELSE 0 END) AS queued_chunks,
          SUM(CASE WHEN status='running' THEN 1 ELSE 0 END) AS running_chunks,
          COUNT(*) AS total_chunks
        FROM llm_chunk_items
        WHERE stage_id = ?
        """,
        (stage_id,),
    )
    row = cur.fetchone() or (0, 0, 0, 0, 0)
    processed_chunks, error_chunks, queued_chunks, running_chunks, total_chunks = [int(x or 0) for x in row]
    status = "running"
    if total_chunks == 0:
        status = "staged"
    elif queued_chunks == 0 and running_chunks == 0:
        status = "completed"
    elif error_chunks > 0 and processed_chunks == 0:
        status = "failed"
    conn.execute(
        """
        UPDATE llm_chunk_stages
        SET
          processed_chunks = ?,
          error_chunks = ?,
          total_chunks = ?,
          status = ?,
          updated_at_utc = (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
        WHERE stage_id = ?
        """,
        (processed_chunks, error_chunks, total_chunks, status, stage_id),
    )
    conn.commit()
    return _load_stage(conn, stage_id)


def create_chunk_stage(
    data_conn: sqlite3.Connection,
    mem_conn: sqlite3.Connection,
    *,
    run_id: str,
    base_sql: str,
    model: str,
    user_prompt: str,
    include_report: bool,
    timeout_s: int,
    context_max_chars: int,
    chunk_size: int,
    max_rows: int,
) -> Dict[str, Any]:
    cleaned_sql = base_sql.strip().rstrip(";")
    if not cleaned_sql:
        raise ValueError("base_sql cannot be empty")
    total_rows_actual = _count_sql_rows(data_conn, cleaned_sql)
    limit_rows = total_rows_actual
    if int(max_rows or 0) > 0:
        limit_rows = min(limit_rows, int(max_rows))
    limit_rows = max(0, limit_rows)
    size = max(1, min(int(chunk_size), 5000))
    total_chunks = int(math.ceil(limit_rows / size)) if limit_rows > 0 else 0
    stage_id = f"stg_{uuid.uuid4().hex[:12]}"

    mem_conn.execute(
        """
        INSERT INTO llm_chunk_stages (
          stage_id, run_id, analysis_mode, model, user_prompt, base_sql, include_report,
          timeout_s, context_max_chars, chunk_size, total_rows, total_chunks, status
        ) VALUES (?, ?, 'chunk', ?, ?, ?, ?, ?, ?, ?, ?, ?, 'staged')
        """,
        (
            stage_id,
            run_id,
            model,
            user_prompt,
            cleaned_sql,
            1 if include_report else 0,
            int(timeout_s),
            int(context_max_chars),
            size,
            limit_rows,
            total_chunks,
        ),
    )
    if total_chunks > 0:
        rows: List[Tuple[Any, ...]] = []
        for chunk_index in range(total_chunks):
            offset_rows = chunk_index * size
            limit_chunk = min(size, limit_rows - offset_rows)
            thumb = _mini_thumbprint(stage_id, run_id, chunk_index, offset_rows, limit_chunk)
            rows.append((stage_id, chunk_index, offset_rows, limit_chunk, thumb, "queued"))
        mem_conn.executemany(
            """
            INSERT INTO llm_chunk_items (stage_id, chunk_index, offset_rows, limit_rows, chunk_thumb, status)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
    mem_conn.commit()
    return _load_stage(mem_conn, stage_id)


def list_stage_chunks(conn: sqlite3.Connection, stage_id: str, limit: int = 200) -> List[Dict[str, Any]]:
    n = max(1, min(int(limit), 1000))
    cur = conn.execute(
        """
        SELECT
          item_id, stage_id, chunk_index, offset_rows, limit_rows, row_count, chunk_thumb, context_hash,
          memory_id, status, started_at_utc, finished_at_utc, error_text
        FROM llm_chunk_items
        WHERE stage_id = ?
        ORDER BY chunk_index ASC
        LIMIT ?
        """,
        (stage_id, n),
    )
    cols = [d[0] for d in (cur.description or [])]
    out: List[Dict[str, Any]] = []
    for row in cur.fetchall():
        out.append({cols[i]: row[i] for i in range(len(cols))})
    return out


def list_llm_memory_entries(conn: sqlite3.Connection, *, run_id: str = "", limit: int = 20) -> List[Dict[str, Any]]:
    n = max(1, min(int(limit), 200))
    rid = str(run_id or "").strip()
    if rid:
        cur = conn.execute(
            """
            SELECT
              memory_id, created_at_utc, run_id, analysis_mode, model, user_prompt, query_limit,
              query_row_count, include_report, context_truncated, context_chars, context_hash,
              LENGTH(response_markdown) AS response_chars, error_text
            FROM llm_memory
            WHERE run_id = ?
            ORDER BY created_at_utc DESC, memory_id DESC
            LIMIT ?
            """,
            (rid, n),
        )
    else:
        cur = conn.execute(
            """
            SELECT
              memory_id, created_at_utc, run_id, analysis_mode, model, user_prompt, query_limit,
              query_row_count, include_report, context_truncated, context_chars, context_hash,
              LENGTH(response_markdown) AS response_chars, error_text
            FROM llm_memory
            ORDER BY created_at_utc DESC, memory_id DESC
            LIMIT ?
            """,
            (n,),
        )

    cols = [d[0] for d in (cur.description or [])]
    rows = cur.fetchall()
    out: List[Dict[str, Any]] = []
    for row in rows:
        out.append({cols[i]: row[i] for i in range(len(cols))})
    return out


def _is_safe_readonly_sql(sql: str) -> bool:
    s = sql.strip()
    if not s:
        return False
    first = s.split(None, 1)[0].upper()
    return first in {"SELECT", "WITH", "PRAGMA", "EXPLAIN"}


def run_sql(conn: sqlite3.Connection, sql: str, limit: Optional[int] = None) -> QueryResult:
    cleaned = sql.strip().rstrip(";")
    if not _is_safe_readonly_sql(cleaned):
        raise ValueError("Only read-only SQL is allowed (SELECT/WITH/PRAGMA/EXPLAIN).")

    first = cleaned.split(None, 1)[0].upper()
    final_sql = cleaned
    if limit is not None and first in {"SELECT", "WITH"}:
        n = max(int(limit), 1)
        final_sql = f"SELECT * FROM ({cleaned}) LIMIT {n}"

    t0 = time.perf_counter()
    cur = conn.execute(final_sql)
    rows = cur.fetchall()
    elapsed = (time.perf_counter() - t0) * 1000.0
    cols = [d[0] for d in (cur.description or [])]
    return QueryResult(columns=cols, rows=rows, elapsed_ms=elapsed)


def print_result(res: QueryResult) -> None:
    if not res.columns:
        print("(no columns)")
        return
    if tabulate is not None:
        print(tabulate(res.rows, headers=res.columns, tablefmt="github"))
    else:
        print(" | ".join(res.columns))
        print("-+-".join("-" * len(c) for c in res.columns))
        for row in res.rows:
            print(" | ".join("" if v is None else str(v) for v in row))
    print(f"\nrows={len(res.rows)} elapsed_ms={res.elapsed_ms:.2f}")


def list_tables(conn: sqlite3.Connection, with_counts: bool = True) -> QueryResult:
    q = """
    SELECT name
    FROM sqlite_master
    WHERE type='table' AND name NOT LIKE 'sqlite_%'
    ORDER BY name
    """
    names = [r[0] for r in conn.execute(q).fetchall()]
    if not with_counts:
        return QueryResult(columns=["table"], rows=[(n,) for n in names], elapsed_ms=0.0)
    rows: List[Tuple[Any, ...]] = []
    t0 = time.perf_counter()
    for n in names:
        cnt = conn.execute(f"SELECT COUNT(*) FROM {n}").fetchone()[0]
        rows.append((n, cnt))
    elapsed = (time.perf_counter() - t0) * 1000.0
    return QueryResult(columns=["table", "rows"], rows=rows, elapsed_ms=elapsed)


def table_schema(conn: sqlite3.Connection, table: str) -> QueryResult:
    cur = conn.execute(f"PRAGMA table_info({table})")
    rows = cur.fetchall()
    cols = [d[0] for d in (cur.description or [])]
    return QueryResult(columns=cols, rows=rows, elapsed_ms=0.0)


def _as_float(value: Any) -> Optional[float]:
    try:
        if value is None:
            return None
        return float(value)
    except Exception:
        return None


def _round_or_none(value: Optional[float], digits: int = 4) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return None
    return round(float(value), digits)


def _safe_json_loads(text: str) -> Dict[str, Any]:
    try:
        obj = json.loads(text or "{}")
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}


def _ollama_base_url() -> str:
    url = os.environ.get("OLLAMA_URL", "http://127.0.0.1:11434").strip().rstrip("/")
    if "/api/" in url:
        url = url.split("/api/", 1)[0]
    return url or "http://127.0.0.1:11434"


def _env_int(name: str, default: int, *, min_value: int = 1, max_value: int = 1000000) -> int:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    try:
        n = int(raw)
    except Exception:
        return default
    return max(min_value, min(max_value, n))


def _coerce_int(value: Any, default: int, *, min_value: int, max_value: int) -> int:
    try:
        n = int(value)
    except Exception:
        n = default
    return max(min_value, min(max_value, n))


def _coerce_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    s = str(value).strip().lower()
    if s in {"1", "true", "yes", "y", "on"}:
        return True
    if s in {"0", "false", "no", "n", "off"}:
        return False
    return default


def ollama_list_models() -> List[str]:
    base = _ollama_base_url()
    try:
        with urllib.request.urlopen(base + "/api/tags", timeout=10) as resp:
            payload = json.loads(resp.read().decode("utf-8", errors="ignore"))
        models: List[str] = []
        for item in payload.get("models", []):
            name = str(item.get("name") or "").strip()
            if name:
                models.append(name)
        if models:
            return models
    except Exception:
        pass

    try:
        cp = subprocess.run(["ollama", "list"], capture_output=True, text=True, check=False)
        out: List[str] = []
        for line in (cp.stdout or "").splitlines()[1:]:
            parts = line.split()
            if parts:
                out.append(parts[0].strip())
        return [m for m in out if m]
    except Exception:
        return []


def ollama_generate(prompt: str, model: str = "", timeout_s: Optional[int] = None) -> Dict[str, Any]:
    model_name = (model or os.environ.get("OLLAMA_MODEL", "llama3.2:1b")).strip()
    if not model_name:
        model_name = "llama3.2:1b"
    default_timeout_s = _env_int("OLLAMA_TIMEOUT_S", 300, min_value=30, max_value=7200)
    effective_timeout_s = _coerce_int(
        timeout_s if timeout_s is not None else default_timeout_s,
        default_timeout_s,
        min_value=10,
        max_value=7200,
    )

    base = _ollama_base_url()
    url = base + "/api/generate"
    payload = json.dumps({"model": model_name, "prompt": prompt, "stream": False}).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    try:
        with urllib.request.urlopen(req, timeout=effective_timeout_s) as resp:
            raw = resp.read().decode("utf-8", errors="ignore")
    except (TimeoutError, socket.timeout):
        raise TimeoutError(
            f"Ollama request timed out after {effective_timeout_s}s. "
            "Lower row limit, disable report context, or increase timeout."
        )
    except urllib.error.HTTPError as ex:
        body = ex.read().decode("utf-8", errors="ignore")
        body_short = body[:500] if body else ""
        msg = f"Ollama HTTP {ex.code}"
        if body_short:
            msg += f": {body_short}"
        raise RuntimeError(msg)
    except urllib.error.URLError as ex:
        reason = getattr(ex, "reason", ex)
        if isinstance(reason, socket.timeout):
            raise TimeoutError(
                f"Ollama request timed out after {effective_timeout_s}s. "
                "Lower row limit, disable report context, or increase timeout."
            )
        raise RuntimeError(f"Ollama connection error: {reason}")

    data = json.loads(raw or "{}")
    answer = str(data.get("response") or "").strip()
    return {"model": model_name, "answer": answer, "timeout_s": effective_timeout_s}


def _trim_cell(value: Any, max_chars: int = 220) -> Any:
    if value is None:
        return None
    s = str(value)
    if len(s) <= max_chars:
        return value
    return s[: max_chars - 3] + "..."


def _rows_as_dicts(columns: List[str], rows: List[Tuple[Any, ...]], max_cell_chars: int = 220) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for row in rows:
        obj: Dict[str, Any] = {}
        for i, col in enumerate(columns):
            val = row[i] if i < len(row) else None
            obj[col] = _trim_cell(val, max_chars=max_cell_chars)
        out.append(obj)
    return out


def _build_default_llm_sql(run_id: str) -> str:
    rid = run_id.replace("'", "''")
    return f"""
    SELECT
      t.signal_id,
      t.side,
      t.entry_ts,
      t.exit_ts,
      t.entry_px,
      t.exit_px,
      t.pnl_points,
      t.minutes_held,
      COALESCE(gd.fail_count, 0) AS failed_gates,
      COALESCE(gd.fail_codes, '') AS fail_codes,
      COALESCE(json_extract(s.features_json, '$.vix_regime'), 'UNKNOWN') AS regime,
      COALESCE(json_extract(s.features_json, '$.signal_description'), '') AS signal_description
    FROM trades t
    LEFT JOIN signals s
      ON s.run_id = t.run_id
     AND s.signal_id = t.signal_id
    LEFT JOIN (
      SELECT
        run_id,
        signal_id,
        SUM(CASE WHEN decision='FAIL' THEN 1 ELSE 0 END) AS fail_count,
        GROUP_CONCAT(CASE WHEN decision='FAIL' THEN gate_id || ':' || COALESCE(denial_code, 'FAIL') END, ' | ') AS fail_codes
      FROM gate_decisions
      WHERE run_id = '{rid}'
      GROUP BY run_id, signal_id
    ) gd
      ON gd.run_id = t.run_id
     AND gd.signal_id = t.signal_id
    WHERE t.run_id = '{rid}'
    ORDER BY t.entry_ts
    """


def _serialize_insights_for_llm(insights: Dict[str, Any], include_report: bool) -> Dict[str, Any]:
    report = str(insights.get("report_markdown") or "")
    report_max = _env_int("OLLAMA_REPORT_MAX_CHARS", 8000, min_value=1000, max_value=100000)
    report_excerpt = report[:report_max]
    out = {
        "run_id": insights.get("run_id"),
        "run_context": insights.get("run_context"),
        "basic_metrics": insights.get("basic_metrics"),
        "complex_metrics": insights.get("complex_metrics"),
        "narrative": insights.get("narrative"),
    }
    if include_report:
        out["report_excerpt"] = report_excerpt
        out["report_excerpt_truncated"] = len(report) > len(report_excerpt)
    return out


def ollama_analyze_run(
    conn: sqlite3.Connection,
    *,
    run_id: str,
    sql: str,
    query_limit: int,
    model: str,
    user_prompt: str,
    include_report: bool,
    timeout_s: Optional[int] = None,
    context_max_chars: Optional[int] = None,
) -> Dict[str, Any]:
    insights = build_run_insights(conn, run_id)
    final_sql = (sql or "").strip() or _build_default_llm_sql(run_id)
    q_limit = max(1, min(int(query_limit), 5000))
    query_res = run_sql(conn, final_sql, limit=q_limit)
    row_dicts = _rows_as_dicts(query_res.columns, query_res.rows, max_cell_chars=240)

    context_payload = {
        "insights": _serialize_insights_for_llm(insights, include_report=include_report),
        "query_result": {
            "columns": query_res.columns,
            "row_count": len(query_res.rows),
            "rows": row_dicts,
            "elapsed_ms": round(query_res.elapsed_ms, 3),
            "limit_used": q_limit,
        },
    }
    context_json = json.dumps(context_payload, ensure_ascii=True)
    context_max = context_max_chars or _env_int("OLLAMA_CONTEXT_MAX_CHARS", 90000, min_value=15000, max_value=400000)
    context_truncated = False
    context_hash = hashlib.sha256(context_json.encode("utf-8")).hexdigest()
    if len(context_json) > context_max:
        context_json = context_json[:context_max]
        context_truncated = True

    task_prompt = (user_prompt or "").strip()
    if not task_prompt:
        task_prompt = (
            "Analyze this backtest run and trades in depth. Explain what signaled, where gates blocked opportunities, "
            "regime behavior, and concrete strategy/risk improvements."
        )

    prompt = (
        "You are a quant backtest analyst. Work only from provided context. "
        "If data is missing or truncated, say so clearly.\n\n"
        "Return markdown with these sections exactly:\n"
        "1) Executive Summary\n"
        "2) Data Source and Schema Validation\n"
        "3) Signal Diagnostics (what signaled, why, and quality)\n"
        "4) Trade-by-Trade Patterns and Outliers\n"
        "5) Regime Analysis\n"
        "6) Gate Blocking and Missed Opportunities\n"
        "7) Recommended Next Experiments\n"
        "8) Data Gaps / Uncertainty\n\n"
        f"User objective:\n{task_prompt}\n\n"
        f"Context JSON:\n{context_json}\n"
    )

    generated = ollama_generate(prompt, model=model, timeout_s=timeout_s)
    return {
        "model": generated["model"],
        "answer": generated["answer"],
        "timeout_s": generated["timeout_s"],
        "run_id": insights.get("run_id"),
        "query_row_count": len(query_res.rows),
        "query_columns": query_res.columns,
        "query_elapsed_ms": round(query_res.elapsed_ms, 3),
        "context_truncated": context_truncated,
        "context_chars": len(context_json),
        "context_hash": context_hash,
        "prompt_chars": len(prompt),
    }


def process_next_chunk_for_stage(
    conn: sqlite3.Connection,
    mem_conn: sqlite3.Connection,
    *,
    stage_id: str,
) -> Dict[str, Any]:
    stage = _load_stage(mem_conn, stage_id)
    if not stage:
        raise ValueError(f"stage not found: {stage_id}")

    cur = mem_conn.execute(
        """
        SELECT item_id, chunk_index, offset_rows, limit_rows, chunk_thumb
        FROM llm_chunk_items
        WHERE stage_id = ? AND status = 'queued'
        ORDER BY chunk_index ASC
        LIMIT 1
        """,
        (stage_id,),
    )
    next_item = cur.fetchone()
    if not next_item:
        refreshed = _refresh_stage_counts(mem_conn, stage_id)
        return {
            "ok": True,
            "stage": refreshed,
            "done": True,
            "message": "No queued chunks remaining.",
        }

    item_id, chunk_index, offset_rows, limit_rows, chunk_thumb = next_item
    mem_conn.execute(
        """
        UPDATE llm_chunk_items
        SET status='running', started_at_utc=(strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
        WHERE item_id = ?
        """,
        (int(item_id),),
    )
    mem_conn.execute(
        """
        UPDATE llm_chunk_stages
        SET status='running', updated_at_utc=(strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
        WHERE stage_id = ?
        """,
        (stage_id,),
    )
    mem_conn.commit()

    chunk_sql = _make_chunk_sql(
        str(stage.get("base_sql") or ""),
        chunk_limit=int(limit_rows),
        chunk_offset=int(offset_rows),
    )
    err_text = ""
    memory_id: Optional[int] = None
    data: Dict[str, Any] = {}
    try:
        data = ollama_analyze_run(
            conn,
            run_id=str(stage.get("run_id") or ""),
            sql=chunk_sql,
            query_limit=int(limit_rows),
            model=str(stage.get("model") or ""),
            user_prompt=str(stage.get("user_prompt") or ""),
            include_report=bool(int(stage.get("include_report") or 0)),
            timeout_s=int(stage.get("timeout_s") or 300),
            context_max_chars=int(stage.get("context_max_chars") or 90000),
        )
        memory_id = save_llm_memory_entry(
            mem_conn,
            run_id=str(data.get("run_id") or stage.get("run_id") or ""),
            analysis_mode="chunk",
            model=str(data.get("model") or stage.get("model") or ""),
            user_prompt=str(stage.get("user_prompt") or ""),
            query_sql=chunk_sql,
            query_limit=int(limit_rows),
            query_row_count=_coerce_int(data.get("query_row_count", 0), 0, min_value=0, max_value=10000000),
            include_report=bool(int(stage.get("include_report") or 0)),
            context_truncated=bool(data.get("context_truncated", False)),
            context_chars=_coerce_int(data.get("context_chars", 0), 0, min_value=0, max_value=100000000),
            context_hash=str(data.get("context_hash") or ""),
            response_markdown=str(data.get("answer") or ""),
            error_text="",
        )
        mem_conn.execute(
            """
            UPDATE llm_chunk_items
            SET
              status='processed',
              row_count=?,
              context_hash=?,
              memory_id=?,
              error_text='',
              finished_at_utc=(strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
            WHERE item_id = ?
            """,
            (
                _coerce_int(data.get("query_row_count", 0), 0, min_value=0, max_value=10000000),
                str(data.get("context_hash") or ""),
                memory_id,
                int(item_id),
            ),
        )
        mem_conn.commit()
    except Exception as ex:
        err_text = str(ex)
        memory_id = save_llm_memory_entry(
            mem_conn,
            run_id=str(stage.get("run_id") or ""),
            analysis_mode="chunk",
            model=str(stage.get("model") or ""),
            user_prompt=str(stage.get("user_prompt") or ""),
            query_sql=chunk_sql,
            query_limit=int(limit_rows),
            query_row_count=0,
            include_report=bool(int(stage.get("include_report") or 0)),
            context_truncated=False,
            context_chars=0,
            context_hash="",
            response_markdown="",
            error_text=err_text,
        )
        mem_conn.execute(
            """
            UPDATE llm_chunk_items
            SET
              status='error',
              row_count=0,
              context_hash='',
              memory_id=?,
              error_text=?,
              finished_at_utc=(strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
            WHERE item_id = ?
            """,
            (memory_id, err_text, int(item_id)),
        )
        mem_conn.execute(
            """
            UPDATE llm_chunk_stages
            SET
              last_error=?,
              updated_at_utc=(strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
            WHERE stage_id = ?
            """,
            (err_text, stage_id),
        )
        mem_conn.commit()

    refreshed = _refresh_stage_counts(mem_conn, stage_id)
    return {
        "ok": err_text == "",
        "stage": refreshed,
        "done": bool(refreshed.get("remaining_chunks", 0) == 0),
        "chunk": {
            "stage_id": stage_id,
            "item_id": int(item_id),
            "chunk_index": int(chunk_index),
            "offset_rows": int(offset_rows),
            "limit_rows": int(limit_rows),
            "chunk_thumb": str(chunk_thumb or ""),
            "memory_id": memory_id,
            "status": "error" if err_text else "processed",
            "error": err_text,
            "query_row_count": data.get("query_row_count", 0) if data else 0,
            "context_hash": data.get("context_hash", "") if data else "",
            "answer_preview": (str(data.get("answer") or "")[:400] if data else ""),
        },
    }


def _resolve_run_id(conn: sqlite3.Connection, requested: str | None) -> str:
    rid = (requested or "").strip()
    if rid:
        return rid
    row = conn.execute("SELECT MAX(run_id) FROM runs").fetchone()
    latest = str(row[0]).strip() if row and row[0] is not None else ""
    if not latest:
        raise ValueError("No runs found in database.")
    return latest


def _get_run_row(conn: sqlite3.Connection, run_id: str) -> Dict[str, Any]:
    cur = conn.execute(
        """
        SELECT run_id, created_at_utc, date_start_et, date_end_et, params_json, report_path, equity_curve_path
        FROM runs
        WHERE run_id = ?
        LIMIT 1
        """,
        (run_id,),
    )
    row = cur.fetchone()
    if not row:
        return {}
    cols = [d[0] for d in (cur.description or [])]
    return {cols[i]: row[i] for i in range(len(cols))}


def _infer_symbol_from_data_path(data_path: str) -> str:
    p = (data_path or "").lower()
    if not p:
        return "UNK"

    if "/ingested/" in p:
        after = p.split("/ingested/", 1)[1]
        parts = [x for x in after.split("/") if x]
        # Typical pattern: provider/symbol/...
        if len(parts) >= 2:
            return parts[1].upper()

    if "/raw/" in p:
        after = p.split("/raw/", 1)[1]
        parts = [x for x in after.split("/") if x]
        # Typical pattern: provider/symbol_or_file
        if len(parts) >= 2:
            candidate = parts[1]
            for sep in ("_", "-", "."):
                if sep in candidate:
                    candidate = candidate.split(sep, 1)[0]
                    break
            return candidate.upper()

    return "UNK"


def _parse_run_context(run_row: Dict[str, Any]) -> Dict[str, Any]:
    params = _safe_json_loads(str(run_row.get("params_json") or ""))
    data_path = str(params.get("resolved_data_path") or params.get("data_path") or "")
    return {
        "source_vendor": str(params.get("source_vendor") or "unknown"),
        "source_dataset": str(params.get("source_dataset") or "unknown"),
        "source_schema": str(params.get("source_schema") or "unknown"),
        "source_feed": str(params.get("source_feed") or ""),
        "source_symbol": str(params.get("source_symbol") or _infer_symbol_from_data_path(data_path) or "UNK").upper(),
        "bar_timeframe": str(params.get("bar_timeframe") or "unknown"),
        "requested_data_path": str(params.get("requested_data_path") or params.get("data_path") or ""),
        "resolved_data_path": data_path,
        "source_sidecar_path": str(params.get("source_sidecar_path") or ""),
        "db_path": str(params.get("db_path") or DEFAULT_DB_PATH),
        "raw_row_count": params.get("raw_row_count"),
        "normalized_row_count": params.get("normalized_row_count"),
        "post_rth_row_count": params.get("post_rth_row_count"),
        "timezone": str(params.get("timezone") or ""),
        "rth_enabled": params.get("rth_enabled"),
        "rth_start": str(params.get("rth_start") or ""),
        "rth_end": str(params.get("rth_end") or ""),
        "start_equity": _as_float(params.get("start_equity")) or 10_000.0,
    }


def _resolve_report_path(run_row: Dict[str, Any], run_id: str) -> Optional[Path]:
    raw = str(run_row.get("report_path") or "").strip()
    if raw:
        p = Path(raw).expanduser()
        if not p.is_absolute():
            p = (REPO_ROOT / p).resolve()
        if p.exists():
            return p
    fallback = REPO_ROOT / "runs" / "artifacts" / "reports" / f"{run_id}_multigate_report.md"
    return fallback if fallback.exists() else None


def _resolve_chart_path(run_row: Dict[str, Any], run_id: str) -> Optional[Path]:
    raw = str(run_row.get("equity_curve_path") or "").strip()
    if raw:
        p = Path(raw).expanduser()
        if not p.is_absolute():
            p = (REPO_ROOT / p).resolve()
        if p.exists():
            return p

    fallback = REPO_ROOT / "runs" / "artifacts" / "charts" / f"{run_id}_equity.png"
    if fallback.exists():
        return fallback

    report_path = _resolve_report_path(run_row, run_id)
    if report_path:
        sibling = report_path.parent.parent / "charts" / f"{run_id}_equity.png"
        if sibling.exists():
            return sibling
    return None


def _build_narrative(
    run_id: str,
    context: Dict[str, Any],
    basic: Dict[str, Any],
    complex_m: Dict[str, Any],
    blocked_best_gate: str,
    blocked_best_points: Optional[float],
    top_regime: str,
    top_regime_n: Optional[int],
) -> str:
    symbol = context.get("source_symbol", "UNK")
    trades = basic.get("trades") or 0
    win = basic.get("win_rate_pct")
    total = basic.get("total_pnl_points")
    pf = complex_m.get("profit_factor")
    mdd = complex_m.get("max_drawdown_pct")

    line1 = (
        f"Run {run_id} traded {symbol} ({context.get('bar_timeframe', 'unknown')}, "
        f"{context.get('source_schema', 'unknown')}) from vendor {context.get('source_vendor', 'unknown')} "
        f"across {trades} trades."
    )
    if win is not None and total is not None:
        line2 = f"Win rate was {win:.2f}% with total PnL of {total:.2f} points."
    else:
        line2 = "Insufficient trade data for win-rate or PnL summary."

    if pf is not None and mdd is not None:
        line3 = f"Profit factor was {pf:.3f} and max drawdown was {mdd:.2f}%."
    else:
        line3 = "Complex risk metrics are partially unavailable for this run."

    line4 = "Blocked-opportunity analysis unavailable."
    if blocked_best_points is not None:
        line4 = f"Largest blocked winner pool came from {blocked_best_gate} with {blocked_best_points:.2f} potential points."

    line5 = "Regime distribution unavailable."
    if top_regime_n is not None:
        line5 = f"Most frequent regime was {top_regime} ({top_regime_n} signals)."

    return " ".join([line1, line2, line3, line4, line5])


def build_run_insights(conn: sqlite3.Connection, requested_run_id: str | None) -> Dict[str, Any]:
    run_id = _resolve_run_id(conn, requested_run_id)
    run_row = _get_run_row(conn, run_id)
    if not run_row:
        raise ValueError(f"run_id not found: {run_id}")

    context = _parse_run_context(run_row)
    symbol = context["source_symbol"]
    start_equity = context["start_equity"]

    trade_rows = conn.execute(
        """
        SELECT signal_id, side, entry_ts, exit_ts, entry_px, exit_px, pnl_points, minutes_held
        FROM trades
        WHERE run_id = ?
        ORDER BY entry_ts
        """,
        (run_id,),
    ).fetchall()
    pnls = [float(r[6]) for r in trade_rows if r[6] is not None]
    holds = [float(r[7]) for r in trade_rows if r[7] is not None]

    trade_count = len(pnls)
    wins = [x for x in pnls if x > 0]
    losses = [x for x in pnls if x < 0]
    total_pnl = sum(pnls) if pnls else 0.0
    mean_pnl = statistics.mean(pnls) if pnls else None
    median_pnl = statistics.median(pnls) if pnls else None
    best = max(pnls) if pnls else None
    worst = min(pnls) if pnls else None
    win_rate = (len(wins) / trade_count * 100.0) if trade_count else None
    avg_hold = statistics.mean(holds) if holds else None

    pass_rows = conn.execute("SELECT COUNT(*), COUNT(DISTINCT signal_id) FROM trades_pass WHERE run_id = ?", (run_id,)).fetchone()
    pass_count = int(pass_rows[0] or 0) if pass_rows else 0
    pass_distinct = int(pass_rows[1] or 0) if pass_rows else 0

    drow = conn.execute(
        """
        SELECT
          SUM(CASE WHEN decision='PASS' THEN 1 ELSE 0 END) AS pass_n,
          COUNT(*) AS total_n
        FROM gate_decisions
        WHERE run_id = ?
        """,
        (run_id,),
    ).fetchone()
    pass_dec = int(drow[0] or 0) if drow else 0
    dec_total = int(drow[1] or 0) if drow else 0
    gate_pass_rate = (pass_dec / dec_total * 100.0) if dec_total else None

    blocked_totals = conn.execute(
        """
        SELECT
          SUM(CASE WHEN t.pnl_points > 0 THEN 1 ELSE 0 END) AS blocked_winners,
          COALESCE(SUM(CASE WHEN t.pnl_points > 0 THEN t.pnl_points ELSE 0 END), 0.0) AS blocked_winner_points
        FROM gate_decisions gd
        LEFT JOIN trades t
          ON t.run_id = gd.run_id
         AND t.signal_id = gd.signal_id
        WHERE gd.run_id = ? AND gd.decision = 'FAIL'
        """,
        (run_id,),
    ).fetchone()
    blocked_winners = int(blocked_totals[0] or 0) if blocked_totals else 0
    blocked_winner_points = float(blocked_totals[1] or 0.0) if blocked_totals else 0.0

    blocked_best = conn.execute(
        """
        SELECT
          gd.gate_id,
          COALESCE(SUM(CASE WHEN t.pnl_points > 0 THEN t.pnl_points ELSE 0 END), 0.0) AS missed_points
        FROM gate_decisions gd
        LEFT JOIN trades t
          ON t.run_id = gd.run_id
         AND t.signal_id = gd.signal_id
        WHERE gd.run_id = ? AND gd.decision = 'FAIL'
        GROUP BY gd.gate_id
        ORDER BY missed_points DESC
        LIMIT 1
        """,
        (run_id,),
    ).fetchone()
    blocked_best_gate = str(blocked_best[0]) if blocked_best and blocked_best[0] else "n/a"
    blocked_best_points = float(blocked_best[1]) if blocked_best and blocked_best[1] is not None else None

    regime_row = conn.execute(
        """
        SELECT
          COALESCE(json_extract(features_json, '$.vix_regime'), 'UNKNOWN') AS regime,
          COUNT(*) AS n
        FROM signals
        WHERE run_id = ?
        GROUP BY COALESCE(json_extract(features_json, '$.vix_regime'), 'UNKNOWN')
        ORDER BY n DESC
        LIMIT 1
        """,
        (run_id,),
    ).fetchone()
    top_regime = str(regime_row[0]) if regime_row and regime_row[0] is not None else "UNKNOWN"
    top_regime_n = int(regime_row[1]) if regime_row and regime_row[1] is not None else None

    # Equity-derived complex metrics
    eq = start_equity
    peak = start_equity
    max_dd_points = 0.0
    max_dd_pct = 0.0
    for p in pnls:
        eq += p
        if eq > peak:
            peak = eq
        dd = eq - peak
        if dd < max_dd_points:
            max_dd_points = dd
            if peak > 0:
                max_dd_pct = (dd / peak) * 100.0

    avg_win = statistics.mean(wins) if wins else None
    avg_loss = statistics.mean(losses) if losses else None
    loss_abs_sum = abs(sum(losses)) if losses else 0.0
    profit_factor = (sum(wins) / loss_abs_sum) if loss_abs_sum > 0 else (float("inf") if wins else None)
    volatility = statistics.pstdev(pnls) if len(pnls) > 1 else None
    sharpe_like = None
    if volatility and volatility > 0 and mean_pnl is not None:
        sharpe_like = mean_pnl / volatility * math.sqrt(len(pnls))
    payoff_ratio = (avg_win / abs(avg_loss)) if (avg_win is not None and avg_loss is not None and avg_loss != 0) else None
    recovery_factor = (total_pnl / abs(max_dd_points)) if max_dd_points < 0 else None

    basic = {
        "run_id": run_id,
        "symbol": symbol,
        "source_vendor": context["source_vendor"],
        "source_dataset": context["source_dataset"],
        "source_schema": context["source_schema"],
        "source_feed": context["source_feed"],
        "bar_timeframe": context["bar_timeframe"],
        "resolved_data_path": context["resolved_data_path"],
        "db_path": context["db_path"],
        "trades": trade_count,
        "pass_trades_rows": pass_count,
        "pass_trades_distinct": pass_distinct,
        "gate_decisions": dec_total,
        "win_rate_pct": _round_or_none(win_rate, 2),
        "total_pnl_points": _round_or_none(total_pnl, 4),
        "avg_pnl_points": _round_or_none(mean_pnl, 4),
        "median_pnl_points": _round_or_none(median_pnl, 4),
        "best_trade_points": _round_or_none(best, 4),
        "worst_trade_points": _round_or_none(worst, 4),
        "avg_hold_minutes": _round_or_none(avg_hold, 2),
        "raw_rows": context["raw_row_count"],
        "normalized_rows": context["normalized_row_count"],
        "post_rth_rows": context["post_rth_row_count"],
    }
    complex_m = {
        "start_equity": _round_or_none(start_equity, 2),
        "end_equity": _round_or_none(start_equity + total_pnl, 2),
        "profit_factor": _round_or_none(profit_factor, 4),
        "max_drawdown_points": _round_or_none(max_dd_points, 4),
        "max_drawdown_pct": _round_or_none(max_dd_pct, 4),
        "expectancy_points": _round_or_none(mean_pnl, 4),
        "volatility_points": _round_or_none(volatility, 4),
        "sharpe_like": _round_or_none(sharpe_like, 4),
        "avg_win_points": _round_or_none(avg_win, 4),
        "avg_loss_points": _round_or_none(avg_loss, 4),
        "payoff_ratio": _round_or_none(payoff_ratio, 4),
        "recovery_factor": _round_or_none(recovery_factor, 4),
        "gate_pass_rate_pct": _round_or_none(gate_pass_rate, 2),
        "blocked_winner_count": blocked_winners,
        "blocked_winner_points": _round_or_none(blocked_winner_points, 4),
        "top_blocking_gate": blocked_best_gate,
        "top_blocking_gate_points": _round_or_none(blocked_best_points, 4),
        "top_regime": top_regime,
        "top_regime_signals": top_regime_n,
    }

    report_path = _resolve_report_path(run_row, run_id)
    report_md = ""
    if report_path and report_path.exists():
        try:
            report_md = report_path.read_text(encoding="utf-8")
        except Exception:
            report_md = ""

    chart_path = _resolve_chart_path(run_row, run_id)
    narrative = _build_narrative(
        run_id,
        context,
        basic,
        complex_m,
        blocked_best_gate,
        blocked_best_points,
        top_regime,
        top_regime_n,
    )

    return {
        "run_id": run_id,
        "symbol": symbol,
        "run_context": context,
        "basic_metrics": basic,
        "complex_metrics": complex_m,
        "narrative": narrative,
        "report_markdown": report_md,
        "report_path": str(report_path) if report_path else "",
        "chart_available": bool(chart_path and chart_path.exists()),
        "chart_path": str(chart_path) if chart_path else "",
    }


class DbWebHandler(BaseHTTPRequestHandler):
    conn: sqlite3.Connection
    mem_conn: Optional[sqlite3.Connection] = None
    memory_db_path: str = ""
    default_limit: int

    def _send_json(self, payload: Dict[str, Any], status: int = 200) -> None:
        body = json.dumps(payload, indent=2, default=str).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_html(self, html: str) -> None:
        body = html.encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_bytes(self, body: bytes, content_type: str, status: int = 200) -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        if parsed.path == "/":
            self._send_html(_html_page())
            return
        if parsed.path == "/api/tables":
            res = list_tables(self.conn, with_counts=True)
            items = [{"table": r[0], "rows": r[1]} for r in res.rows]
            self._send_json({"ok": True, "tables": items})
            return
        if parsed.path == "/api/schema":
            table = parse_qs(parsed.query).get("table", [""])[0].strip()
            if not table:
                self._send_json({"ok": False, "error": "missing 'table' query parameter"}, status=400)
                return
            try:
                res = table_schema(self.conn, table)
            except Exception as ex:
                self._send_json({"ok": False, "error": str(ex)}, status=400)
                return
            self._send_json({"ok": True, "columns": res.columns, "rows": res.rows})
            return
        if parsed.path == "/api/insights":
            requested = parse_qs(parsed.query).get("run_id", [""])[0]
            try:
                payload = build_run_insights(self.conn, requested)
                payload["ok"] = True
                self._send_json(payload)
            except Exception as ex:
                self._send_json({"ok": False, "error": str(ex)}, status=400)
            return
        if parsed.path == "/api/chart":
            requested = parse_qs(parsed.query).get("run_id", [""])[0]
            try:
                run_id = _resolve_run_id(self.conn, requested)
                run_row = _get_run_row(self.conn, run_id)
                chart_path = _resolve_chart_path(run_row, run_id)
                if not chart_path or not chart_path.exists():
                    self._send_json({"ok": False, "error": f"chart not found for run_id={run_id}"}, status=404)
                    return
                body = chart_path.read_bytes()
                self._send_bytes(body, "image/png", status=200)
            except Exception as ex:
                self._send_json({"ok": False, "error": str(ex)}, status=400)
            return
        if parsed.path == "/api/ollama/models":
            try:
                models = ollama_list_models()
                default_model = os.environ.get("OLLAMA_MODEL", "llama3.2:1b").strip() or "llama3.2:1b"
                self._send_json({"ok": True, "models": models, "default_model": default_model, "base_url": _ollama_base_url()})
            except Exception as ex:
                self._send_json({"ok": False, "error": str(ex)}, status=400)
            return
        if parsed.path == "/api/ollama/memory":
            try:
                if self.mem_conn is None:
                    self._send_json({"ok": False, "error": "memory db not configured"}, status=503)
                    return
                requested = parse_qs(parsed.query)
                run_id = requested.get("run_id", [""])[0]
                limit = _coerce_int(requested.get("limit", ["20"])[0], 20, min_value=1, max_value=200)
                rows = list_llm_memory_entries(self.mem_conn, run_id=run_id, limit=limit)
                self._send_json(
                    {
                        "ok": True,
                        "memory_db_path": self.memory_db_path,
                        "run_id": run_id.strip(),
                        "rows": rows,
                        "row_count": len(rows),
                    }
                )
            except Exception as ex:
                self._send_json({"ok": False, "error": str(ex)}, status=400)
            return
        if parsed.path == "/api/ollama/stage":
            try:
                if self.mem_conn is None:
                    self._send_json({"ok": False, "error": "memory db not configured"}, status=503)
                    return
                requested = parse_qs(parsed.query)
                stage_id = str(requested.get("stage_id", [""])[0] or "").strip()
                if not stage_id:
                    self._send_json({"ok": False, "error": "missing stage_id"}, status=400)
                    return
                stage = _load_stage(self.mem_conn, stage_id)
                if not stage:
                    self._send_json({"ok": False, "error": f"stage not found: {stage_id}"}, status=404)
                    return
                self._send_json({"ok": True, "stage": stage})
            except Exception as ex:
                self._send_json({"ok": False, "error": str(ex)}, status=400)
            return
        if parsed.path == "/api/ollama/chunks":
            try:
                if self.mem_conn is None:
                    self._send_json({"ok": False, "error": "memory db not configured"}, status=503)
                    return
                requested = parse_qs(parsed.query)
                stage_id = str(requested.get("stage_id", [""])[0] or "").strip()
                limit = _coerce_int(requested.get("limit", ["200"])[0], 200, min_value=1, max_value=1000)
                if not stage_id:
                    self._send_json({"ok": False, "error": "missing stage_id"}, status=400)
                    return
                stage = _load_stage(self.mem_conn, stage_id)
                if not stage:
                    self._send_json({"ok": False, "error": f"stage not found: {stage_id}"}, status=404)
                    return
                chunks = list_stage_chunks(self.mem_conn, stage_id, limit=limit)
                self._send_json({"ok": True, "stage": stage, "chunks": chunks, "row_count": len(chunks)})
            except Exception as ex:
                self._send_json({"ok": False, "error": str(ex)}, status=400)
            return
        self._send_json({"ok": False, "error": "not found"}, status=404)

    def do_POST(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        if parsed.path not in {
            "/api/query",
            "/api/ollama/analyze",
            "/api/ollama/stage",
            "/api/ollama/process-next",
            "/api/ollama/process-all",
        }:
            self._send_json({"ok": False, "error": "not found"}, status=404)
            return
        try:
            n = int(self.headers.get("Content-Length", "0"))
        except Exception:
            n = 0
        raw = self.rfile.read(max(n, 0))
        try:
            payload = json.loads(raw.decode("utf-8") or "{}")
        except Exception:
            self._send_json({"ok": False, "error": "invalid JSON body"}, status=400)
            return

        if parsed.path == "/api/query":
            sql = str(payload.get("sql", "")).strip()
            limit = payload.get("limit", self.default_limit)
            try:
                lim_int = int(limit) if limit is not None else None
                res = run_sql(self.conn, sql, lim_int)
                rows = [list(r) for r in res.rows]
                self._send_json(
                    {
                        "ok": True,
                        "columns": res.columns,
                        "rows": rows,
                        "row_count": len(rows),
                        "elapsed_ms": round(res.elapsed_ms, 3),
                    }
                )
            except Exception as ex:
                self._send_json({"ok": False, "error": str(ex)}, status=400)
            return

        if parsed.path == "/api/ollama/analyze":
            requested_run = str(payload.get("run_id", "")).strip()
            sql = str(payload.get("sql", "") or "").strip()
            limit = _coerce_int(payload.get("limit", 120), 120, min_value=1, max_value=5000)
            model = str(payload.get("model", "") or "").strip()
            prompt = str(payload.get("prompt", "") or "")
            include_report = _coerce_bool(payload.get("include_report", False), default=False)
            timeout_s = _coerce_int(payload.get("timeout_s", 300), 300, min_value=10, max_value=7200)
            context_max = _coerce_int(payload.get("context_max_chars", 90000), 90000, min_value=15000, max_value=400000)
            save_memory = _coerce_bool(payload.get("save_memory", True), default=True)
            mode_raw = str(payload.get("analysis_mode", "query" if sql else "run")).strip().lower()
            analysis_mode = mode_raw if mode_raw in {"run", "query"} else ("query" if sql else "run")
            resolved_run = requested_run
            try:
                resolved_run = _resolve_run_id(self.conn, requested_run)
                data = ollama_analyze_run(
                    self.conn,
                    run_id=resolved_run,
                    sql=sql,
                    query_limit=limit,
                    model=model,
                    user_prompt=prompt,
                    include_report=include_report,
                    timeout_s=timeout_s,
                    context_max_chars=context_max,
                )
                memory_id = None
                if save_memory and self.mem_conn is not None:
                    memory_id = save_llm_memory_entry(
                        self.mem_conn,
                        run_id=str(data.get("run_id") or resolved_run),
                        analysis_mode=analysis_mode,
                        model=str(data.get("model") or model),
                        user_prompt=prompt,
                        query_sql=sql,
                        query_limit=limit,
                        query_row_count=_coerce_int(data.get("query_row_count", 0), 0, min_value=0, max_value=10000000),
                        include_report=include_report,
                        context_truncated=bool(data.get("context_truncated", False)),
                        context_chars=_coerce_int(data.get("context_chars", 0), 0, min_value=0, max_value=100000000),
                        context_hash=str(data.get("context_hash") or ""),
                        response_markdown=str(data.get("answer") or ""),
                    )
                data["memory_saved"] = bool(memory_id)
                data["memory_id"] = memory_id
                data["ok"] = True
                self._send_json(data, status=200)
            except TimeoutError as ex:
                err = str(ex)
                memory_id = None
                if save_memory and self.mem_conn is not None:
                    try:
                        memory_id = save_llm_memory_entry(
                            self.mem_conn,
                            run_id=(resolved_run or requested_run or ""),
                            analysis_mode=analysis_mode,
                            model=model,
                            user_prompt=prompt,
                            query_sql=sql,
                            query_limit=limit,
                            query_row_count=0,
                            include_report=include_report,
                            context_truncated=False,
                            context_chars=0,
                            context_hash="",
                            response_markdown="",
                            error_text=err,
                        )
                    except Exception:
                        memory_id = None
                self._send_json({"ok": False, "error": err, "memory_id": memory_id}, status=504)
            except Exception as ex:
                err = str(ex)
                memory_id = None
                if save_memory and self.mem_conn is not None:
                    try:
                        memory_id = save_llm_memory_entry(
                            self.mem_conn,
                            run_id=(resolved_run or requested_run or ""),
                            analysis_mode=analysis_mode,
                            model=model,
                            user_prompt=prompt,
                            query_sql=sql,
                            query_limit=limit,
                            query_row_count=0,
                            include_report=include_report,
                            context_truncated=False,
                            context_chars=0,
                            context_hash="",
                            response_markdown="",
                            error_text=err,
                        )
                    except Exception:
                        memory_id = None
                self._send_json({"ok": False, "error": err, "memory_id": memory_id}, status=400)
            return
        if parsed.path == "/api/ollama/stage":
            try:
                if self.mem_conn is None:
                    self._send_json({"ok": False, "error": "memory db not configured"}, status=503)
                    return
                requested_run = str(payload.get("run_id", "")).strip()
                resolved_run = _resolve_run_id(self.conn, requested_run)
                sql = str(payload.get("sql", "") or "").strip()
                base_sql = sql or _build_default_llm_sql(resolved_run)
                model = str(payload.get("model", "") or "").strip()
                prompt = str(payload.get("prompt", "") or "")
                include_report = _coerce_bool(payload.get("include_report", False), default=False)
                timeout_s = _coerce_int(payload.get("timeout_s", 300), 300, min_value=10, max_value=7200)
                context_max = _coerce_int(payload.get("context_max_chars", 90000), 90000, min_value=15000, max_value=400000)
                chunk_size = _coerce_int(payload.get("chunk_size", 40), 40, min_value=1, max_value=5000)
                max_rows = _coerce_int(payload.get("max_rows", 0), 0, min_value=0, max_value=10000000)
                stage = create_chunk_stage(
                    self.conn,
                    self.mem_conn,
                    run_id=resolved_run,
                    base_sql=base_sql,
                    model=model,
                    user_prompt=prompt,
                    include_report=include_report,
                    timeout_s=timeout_s,
                    context_max_chars=context_max,
                    chunk_size=chunk_size,
                    max_rows=max_rows,
                )
                self._send_json({"ok": True, "stage": stage})
            except Exception as ex:
                self._send_json({"ok": False, "error": str(ex)}, status=400)
            return
        if parsed.path == "/api/ollama/process-next":
            try:
                if self.mem_conn is None:
                    self._send_json({"ok": False, "error": "memory db not configured"}, status=503)
                    return
                stage_id = str(payload.get("stage_id", "") or "").strip()
                if not stage_id:
                    self._send_json({"ok": False, "error": "missing stage_id"}, status=400)
                    return
                out = process_next_chunk_for_stage(self.conn, self.mem_conn, stage_id=stage_id)
                out["ok"] = bool(out.get("ok", False))
                self._send_json(out, status=200 if out["ok"] else 400)
            except Exception as ex:
                self._send_json({"ok": False, "error": str(ex)}, status=400)
            return
        if parsed.path == "/api/ollama/process-all":
            try:
                if self.mem_conn is None:
                    self._send_json({"ok": False, "error": "memory db not configured"}, status=503)
                    return
                stage_id = str(payload.get("stage_id", "") or "").strip()
                max_chunks = _coerce_int(payload.get("max_chunks", 20), 20, min_value=1, max_value=1000)
                if not stage_id:
                    self._send_json({"ok": False, "error": "missing stage_id"}, status=400)
                    return
                chunks: List[Dict[str, Any]] = []
                hard_error = ""
                for _ in range(max_chunks):
                    out = process_next_chunk_for_stage(self.conn, self.mem_conn, stage_id=stage_id)
                    chunk = out.get("chunk")
                    if isinstance(chunk, dict):
                        chunks.append(
                            {
                                "chunk_index": chunk.get("chunk_index"),
                                "chunk_thumb": chunk.get("chunk_thumb"),
                                "status": chunk.get("status"),
                                "memory_id": chunk.get("memory_id"),
                                "query_row_count": chunk.get("query_row_count", 0),
                                "error": chunk.get("error", ""),
                            }
                        )
                    if not bool(out.get("ok", False)):
                        hard_error = str((chunk or {}).get("error") or "chunk processing failed")
                        break
                    if bool(out.get("done", False)):
                        break
                stage = _load_stage(self.mem_conn, stage_id)
                self._send_json(
                    {
                        "ok": hard_error == "",
                        "stage": stage,
                        "chunks": chunks,
                        "processed_now": len(chunks),
                        "error": hard_error,
                    },
                    status=200 if hard_error == "" else 400,
                )
            except Exception as ex:
                self._send_json({"ok": False, "error": str(ex)}, status=400)
            return

    def log_message(self, fmt: str, *args: Any) -> None:
        # Keep stdout clean; this is a local tool.
        return


def _html_page() -> str:
    return """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Multigate SQLite Explorer</title>
  <style>
    :root {
      --left-pane: 360px;
      --page-pad: clamp(4px, 0.6vw, 10px);
      --bg:#f6f4ef;
      --panel:#ffffff;
      --text:#1f2933;
      --muted:#5f6c76;
      --line:#d7dadd;
      --accent:#1d6f42;
    }
    body { margin:0; font-family: "SF Pro Text", "Segoe UI", "Helvetica Neue", Arial, sans-serif; background:var(--bg); color:var(--text); }
    .wrap { width: 100vw; max-width: none; margin: 0; padding: var(--page-pad); box-sizing: border-box; }
    .grid { display:grid; grid-template-columns: var(--left-pane) 10px minmax(0, 1fr); gap:12px; align-items:start; }
    .card { background:var(--panel); border:1px solid var(--line); border-radius:12px; padding:14px; box-shadow: 0 1px 2px rgba(0,0,0,0.04); }
    .stretch { min-height: calc(100vh - 110px); }
    .splitter {
      width: 10px;
      border-radius: 8px;
      background: linear-gradient(180deg, #ececec, #e3e3e3);
      border: 1px solid #d7d7d7;
      cursor: col-resize;
      user-select: none;
      min-height: 460px;
    }
    h1 { font-size:20px; margin:0 0 6px 0; }
    h2 { font-size:14px; margin:0 0 8px 0; color:var(--muted); letter-spacing: 0.02em; text-transform: uppercase; }
    .help { margin: 0 0 10px 0; font-size: 13px; color: var(--muted); }
    textarea { width:100%; min-height:130px; height:180px; max-height:45vh; resize:vertical; overflow:auto; font: 13px/1.4 ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; border:1px solid var(--line); border-radius:8px; padding:10px; box-sizing:border-box; }
    input, select, button { font:inherit; }
    .row { display:flex; gap:8px; align-items:center; margin-top:8px; flex-wrap:wrap; }
    .btn { border:1px solid var(--line); background:#fff; padding:6px 10px; border-radius:8px; cursor:pointer; }
    .btn.primary { background:var(--accent); color:#fff; border-color:var(--accent); }
    .preset-bar { display:flex; gap:8px; align-items:center; flex-wrap:wrap; margin:10px 0 2px 0; }
    .muted { color:var(--muted); font-size:12px; }
    .tables-card { display:flex; flex-direction:column; }
    .list { flex:1; min-height: 360px; overflow:auto; border:1px solid var(--line); border-radius:8px; }
    .list button { width:100%; text-align:left; border:0; border-bottom:1px solid var(--line); padding:8px; background:#fff; cursor:pointer; }
    .list button:hover { background:#f0f0f0; }
    .query-card { display:flex; flex-direction:column; min-width:0; }
    #schema { margin-top: 8px; max-height: 180px; overflow:auto; border-radius:8px; }
    #schema pre { white-space: pre-wrap; word-break: break-word; overflow-wrap: anywhere; max-height: 170px; overflow:auto; }
    .workspace { display:grid; grid-template-columns: minmax(0, 1fr) minmax(320px, 460px); gap:12px; align-items:start; margin-top: 10px; min-height: 430px; max-height: calc(100vh - 260px); overflow-x:auto; overflow-y:auto; flex:1; }
    .result-pane { min-width:0; display:flex; flex-direction:column; min-height: 0; }
    .insights-pane { min-width:0; max-height: calc(100vh - 250px); overflow:auto; display:flex; flex-direction:column; gap:10px; }
    .insight-card { border:1px solid var(--line); border-radius:8px; background:#fbfbfa; padding:8px; }
    .insight-title { font-size:12px; font-weight:700; color:#44505a; text-transform: uppercase; letter-spacing:0.03em; margin:0 0 6px 0; }
    .kv-table { width:100%; border-collapse: collapse; margin-top:0; font: 12px/1.35 ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; }
    .kv-table td { border:1px solid var(--line); padding:5px 6px; vertical-align:top; }
    .kv-table td:first-child { width:52%; color:#4a5560; }
    .text-box { border:1px solid var(--line); border-radius:8px; background:#fff; padding:8px; max-height:240px; overflow:auto; white-space: pre-wrap; word-break: break-word; overflow-wrap: anywhere; font: 12px/1.35 ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; }
    .llm-controls { display:flex; flex-wrap:wrap; gap:6px; align-items:center; margin-bottom:8px; }
    .llm-controls select, .llm-controls input { min-height:30px; }
    .llm-controls input[type="number"] { width:90px; }
    .llm-controls input[type="text"] { min-width:180px; }
    .llm-controls select { min-width:220px; max-width:100%; }
    .llm-prompt { width:100%; min-height:70px; max-height:180px; resize:vertical; overflow:auto; margin-bottom:8px; font: 12px/1.35 ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; border:1px solid var(--line); border-radius:8px; padding:8px; box-sizing:border-box; background:#fff; }
    .llm-output { max-height:360px; overflow:auto; }
    .llm-progress { display:flex; gap:8px; align-items:center; margin:6px 0; }
    .llm-progress progress { width:100%; height:14px; }
    .llm-stage-log { min-height:80px; max-height:180px; }
    #report-markdown.text-box { max-height: 380px; }
    .chart-box { border:1px solid var(--line); border-radius:8px; background:#fff; padding:6px; min-height:220px; max-height:360px; overflow:auto; display:block; }
    .chart-box img { display:block; width:auto; min-width:100%; max-width:none; height:auto; max-height:300px; object-fit:contain; margin:0 auto; }
    table { border-collapse: collapse; width:100%; margin-top:10px; font: 12.5px/1.35 ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; }
    th, td { border:1px solid var(--line); padding:6px 8px; text-align:left; vertical-align:top; max-width: 560px; word-break: break-word; overflow-wrap: anywhere; }
    th { background:#f2f2f2; position: sticky; top:0; }
    .table-wrap { overflow:auto; border:1px solid var(--line); border-radius:8px; height:550px; min-height:260px; max-height:82vh; resize:vertical; }
    .table-wrap table { width:max-content; min-width:100%; margin-top:0; }
    .table-wrap.nowrap th, .table-wrap.nowrap td { white-space: nowrap; word-break: normal; overflow-wrap: normal; }
    pre { background:#f2f2f2; border:1px solid var(--line); border-radius:8px; padding:8px; overflow:auto; white-space: pre-wrap; word-break: break-word; overflow-wrap: anywhere; font: 12px/1.35 ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; }
    @media (max-width: 980px) {
      .grid { grid-template-columns: 1fr; }
      .splitter { display:none; }
      .stretch { min-height: 0; }
      .table-wrap { height: 420px; max-height: 74vh; }
      .workspace { grid-template-columns: 1fr; min-height: 0; max-height: none; overflow: visible; }
      .insights-pane { max-height: none; }
    }
  </style>
</head>
<body>
  <div class="wrap">
    <h1>Multigate SQLite Explorer</h1>
    <p class="help">Choose a preset or table, adjust controls, then run. SQL/JSON containers wrap text and use scroll boxes; layout/editor/results remain resizable.</p>
    <div class="grid">
      <div class="card stretch tables-card">
        <h2>Tables</h2>
        <div id="tables" class="list"></div>
      </div>
      <div id="splitter" class="splitter" title="Drag to resize panels"></div>
      <div class="card stretch query-card">
        <h2>Query</h2>
        <textarea id="sql">SELECT run_id, created_at_utc, date_start_et, date_end_et FROM runs ORDER BY created_at_utc DESC;</textarea>
        <div class="preset-bar">
          <span class="muted">Presets:</span>
          <button id="preset-latest" class="btn">Latest Run</button>
          <button id="preset-gate-health" class="btn">Gate Health</button>
          <button id="preset-denials" class="btn">Denials</button>
          <button id="preset-pnl" class="btn">PnL Drilldown</button>
        </div>
        <div class="row">
          <label>Run ID <input id="run-id" type="text" placeholder="blank = latest run" style="min-width: 290px;" /></label>
          <button id="use-latest-run" class="btn">Use Latest</button>
          <label>Limit <input id="limit" type="number" min="1" value="200" /></label>
          <label>Query H <input id="query-height" type="range" min="130" max="520" step="10" value="180" /></label>
          <label>Table H <input id="table-height" type="range" min="240" max="920" step="10" value="550" /></label>
          <button id="run" class="btn primary">Run Query</button>
          <button id="toggle-wrap" class="btn">No Wrap</button>
          <button id="narrower" class="btn">Narrower Left</button>
          <button id="wider" class="btn">Wider Left</button>
          <span id="meta" class="muted"></span>
        </div>
        <div id="schema"></div>
        <div class="workspace">
          <div class="result-pane">
            <div id="result" class="table-wrap"></div>
          </div>
          <div class="insights-pane">
            <div class="insight-card">
              <div class="insight-title">Basic Derived</div>
              <div id="basic-metrics"></div>
            </div>
            <div class="insight-card">
              <div class="insight-title">Complex Derived</div>
              <div id="complex-metrics"></div>
            </div>
            <div class="insight-card">
              <div class="insight-title">Narrative</div>
              <div id="narrative" class="text-box"></div>
            </div>
            <div class="insight-card">
              <div class="insight-title">Local LLM Analysis (Ollama)</div>
              <div class="llm-controls">
                <label>Model
                  <select id="ollama-model"></select>
                </label>
                <button id="ollama-refresh" class="btn">Refresh Models</button>
                <label>Rows <input id="ollama-limit" type="number" min="10" max="5000" value="60" /></label>
                <label>Timeout(s) <input id="ollama-timeout" type="number" min="10" max="7200" value="300" /></label>
                <label><input id="ollama-include-report" type="checkbox" /> include report excerpt</label>
                <label><input id="ollama-save-memory" type="checkbox" checked /> save memory</label>
                <button id="ollama-load-memory" class="btn">Load Memory</button>
                <button id="ollama-analyze-run" class="btn">Analyze Run</button>
                <button id="ollama-analyze-query" class="btn">Analyze Query</button>
                <label>Chunk <input id="ollama-chunk-size" type="number" min="1" max="5000" value="40" /></label>
                <label>MaxRows <input id="ollama-max-rows" type="number" min="0" max="10000000" value="400" /></label>
                <label>Batch <input id="ollama-max-chunks" type="number" min="1" max="1000" value="20" /></label>
                <button id="ollama-stage-create" class="btn">Stage Chunks</button>
                <button id="ollama-stage-next" class="btn">Process Next</button>
                <button id="ollama-stage-all" class="btn">Process Batch</button>
                <button id="ollama-stage-refresh" class="btn">Refresh Stage</button>
                <label>Stage ID <input id="ollama-stage-id" type="text" placeholder="stg_xxx" /></label>
              </div>
              <textarea id="ollama-prompt" class="llm-prompt">Focus on trade quality, signal rationale, blocked opportunities by gate, and concrete strategy/risk improvements.</textarea>
              <div class="llm-progress">
                <progress id="ollama-stage-progress" max="100" value="0"></progress>
                <span id="ollama-stage-progress-label" class="muted">0%</span>
              </div>
              <div id="ollama-meta" class="muted" style="margin-bottom:6px;"></div>
              <pre id="ollama-stage-log" class="text-box llm-stage-log"></pre>
              <pre id="ollama-output" class="text-box llm-output"></pre>
            </div>
            <div class="insight-card">
              <div class="insight-title">Detailed Report</div>
              <pre id="report-markdown" class="text-box"></pre>
            </div>
            <div class="insight-card">
              <div class="insight-title">Equity Chart</div>
              <div class="chart-box">
                <img id="equity-chart" alt="equity chart" />
              </div>
              <div id="chart-note" class="muted" style="margin-top:6px;"></div>
            </div>
          </div>
        </div>
      </div>
    </div>
  </div>
  <script>
    const tablesDiv = document.getElementById("tables");
    const sqlEl = document.getElementById("sql");
    const runIdEl = document.getElementById("run-id");
    const limitEl = document.getElementById("limit");
    const metaEl = document.getElementById("meta");
    const resultEl = document.getElementById("result");
    const schemaEl = document.getElementById("schema");
    const splitterEl = document.getElementById("splitter");
    const queryHeightEl = document.getElementById("query-height");
    const tableHeightEl = document.getElementById("table-height");
    const wrapBtn = document.getElementById("toggle-wrap");
    const basicMetricsEl = document.getElementById("basic-metrics");
    const complexMetricsEl = document.getElementById("complex-metrics");
    const narrativeEl = document.getElementById("narrative");
    const reportMarkdownEl = document.getElementById("report-markdown");
    const equityChartEl = document.getElementById("equity-chart");
    const chartNoteEl = document.getElementById("chart-note");
    const ollamaModelEl = document.getElementById("ollama-model");
    const ollamaLimitEl = document.getElementById("ollama-limit");
    const ollamaTimeoutEl = document.getElementById("ollama-timeout");
    const ollamaIncludeReportEl = document.getElementById("ollama-include-report");
    const ollamaSaveMemoryEl = document.getElementById("ollama-save-memory");
    const ollamaPromptEl = document.getElementById("ollama-prompt");
    const ollamaMetaEl = document.getElementById("ollama-meta");
    const ollamaOutputEl = document.getElementById("ollama-output");
    const ollamaChunkSizeEl = document.getElementById("ollama-chunk-size");
    const ollamaMaxRowsEl = document.getElementById("ollama-max-rows");
    const ollamaMaxChunksEl = document.getElementById("ollama-max-chunks");
    const ollamaStageIdEl = document.getElementById("ollama-stage-id");
    const ollamaStageProgressEl = document.getElementById("ollama-stage-progress");
    const ollamaStageProgressLabelEl = document.getElementById("ollama-stage-progress-label");
    const ollamaStageLogEl = document.getElementById("ollama-stage-log");
    const RUN_META_CTE = `WITH run_meta AS (
  SELECT
    __RUN_ID__ AS run_id,
    lower(
      COALESCE(
        (SELECT json_extract(params_json, '$.resolved_data_path') FROM runs WHERE run_id=__RUN_ID__),
        (SELECT json_extract(params_json, '$.data_path') FROM runs WHERE run_id=__RUN_ID__),
        ''
      )
    ) AS data_path
),
parts AS (
  SELECT
    run_id,
    data_path,
    CASE
      WHEN instr(data_path, '/ingested/') > 0 THEN substr(data_path, instr(data_path, '/ingested/') + 10)
      ELSE ''
    END AS after_ingested,
    CASE
      WHEN instr(data_path, '/raw/') > 0 THEN substr(data_path, instr(data_path, '/raw/') + 5)
      ELSE ''
    END AS after_raw
  FROM run_meta
),
norm AS (
  SELECT
    run_id,
    data_path,
    after_ingested,
    CASE
      WHEN after_ingested <> '' AND instr(after_ingested, '/') > 0
      THEN substr(after_ingested, instr(after_ingested, '/') + 1)
      ELSE ''
    END AS ing_after_provider,
    CASE
      WHEN after_raw <> '' AND instr(after_raw, '/') > 0
      THEN substr(after_raw, instr(after_raw, '/') + 1)
      ELSE ''
    END AS raw_after_provider
  FROM parts
),
symbols AS (
  SELECT
    run_id,
    CASE
      WHEN ing_after_provider <> '' AND instr(ing_after_provider, '/') > 1
      THEN substr(ing_after_provider, 1, instr(ing_after_provider, '/') - 1)
      ELSE ''
    END AS ing_symbol,
    CASE
      WHEN raw_after_provider <> '' AND instr(raw_after_provider, '/') > 1
      THEN substr(raw_after_provider, 1, instr(raw_after_provider, '/') - 1)
      ELSE raw_after_provider
    END AS raw_candidate
  FROM norm
),
run_meta_symbol AS (
  SELECT
    run_id,
    upper(
      COALESCE(
        NULLIF(ing_symbol, ''),
        NULLIF(
          CASE
            WHEN raw_candidate = '' THEN ''
            WHEN instr(raw_candidate, '_') > 1 THEN substr(raw_candidate, 1, instr(raw_candidate, '_') - 1)
            WHEN instr(raw_candidate, '-') > 1 THEN substr(raw_candidate, 1, instr(raw_candidate, '-') - 1)
            WHEN instr(raw_candidate, '.') > 1 THEN substr(raw_candidate, 1, instr(raw_candidate, '.') - 1)
            ELSE raw_candidate
          END,
          ''
        ),
        'UNK'
      )
    ) AS symbol
  FROM symbols
)`;
    const QUERY_TEMPLATES = {
      latest: `${RUN_META_CTE}
SELECT
  rm.run_id AS run_id,
  COALESCE((SELECT json_extract(params_json, '$.source_symbol') FROM runs WHERE run_id=__RUN_ID__), rm.symbol) AS traded_symbol,
  COALESCE((SELECT json_extract(params_json, '$.source_vendor') FROM runs WHERE run_id=__RUN_ID__), 'unknown') AS source_vendor,
  COALESCE((SELECT json_extract(params_json, '$.source_dataset') FROM runs WHERE run_id=__RUN_ID__), 'unknown') AS source_dataset,
  COALESCE((SELECT json_extract(params_json, '$.source_schema') FROM runs WHERE run_id=__RUN_ID__), 'unknown') AS source_schema,
  COALESCE((SELECT json_extract(params_json, '$.bar_timeframe') FROM runs WHERE run_id=__RUN_ID__), 'unknown') AS bar_timeframe,
  COALESCE((SELECT json_extract(params_json, '$.db_path') FROM runs WHERE run_id=__RUN_ID__), '') AS db_path,
  COALESCE((SELECT json_extract(params_json, '$.resolved_data_path') FROM runs WHERE run_id=__RUN_ID__), '') AS resolved_data_path,
  (SELECT created_at_utc FROM runs WHERE run_id=__RUN_ID__) AS created_at_utc,
  (SELECT date_start_et FROM runs WHERE run_id=__RUN_ID__) AS date_start_et,
  (SELECT date_end_et FROM runs WHERE run_id=__RUN_ID__) AS date_end_et,
  (SELECT COUNT(*) FROM trades WHERE run_id=__RUN_ID__) AS trades,
  (SELECT COUNT(*) FROM trades_pass WHERE run_id=__RUN_ID__) AS trades_pass,
  (SELECT COUNT(*) FROM gate_decisions WHERE run_id=__RUN_ID__) AS decisions,
  (SELECT COUNT(*) FROM gate_metrics WHERE run_id=__RUN_ID__) AS metrics
FROM run_meta_symbol rm;`,
      gateHealth: `${RUN_META_CTE}
SELECT
  rm.symbol AS symbol,
  gd.gate_id AS gate,
  SUM(CASE WHEN gd.decision='PASS' THEN 1 ELSE 0 END) AS pass_count,
  SUM(CASE WHEN gd.decision='FAIL' THEN 1 ELSE 0 END) AS fail_count,
  ROUND(
    100.0 * SUM(CASE WHEN gd.decision='PASS' THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0),
    2
  ) AS pass_pct
FROM gate_decisions gd
JOIN run_meta_symbol rm ON gd.run_id = rm.run_id
GROUP BY gd.gate_id
ORDER BY gd.gate_id;`,
      denials: `${RUN_META_CTE}
SELECT
  gd.signal_id AS trade_id,
  rm.symbol || ' ' || COALESCE(
    t.side,
    CASE
      WHEN s.direction = 1 THEN 'LONG'
      WHEN s.direction = -1 THEN 'SHORT'
      ELSE CAST(s.direction AS TEXT)
    END,
    '?'
  ) || ' @' ||
    CASE WHEN t.entry_px IS NULL THEN 'n/a' ELSE printf('%.2f', t.entry_px) END ||
    ' -> ' ||
    CASE WHEN t.exit_px IS NULL THEN 'n/a' ELSE printf('%.2f', t.exit_px) END AS trade_name,
  COALESCE(
    t.side,
    CASE
      WHEN s.direction = 1 THEN 'LONG'
      WHEN s.direction = -1 THEN 'SHORT'
      ELSE CAST(s.direction AS TEXT)
    END,
    '?'
  ) AS direction,
  gd.ts AS decision_ts_utc,
  gd.gate_id AS gate,
  gd.denial_code,
  COALESCE(gd.denial_detail, '') AS detail
FROM gate_decisions gd
JOIN run_meta_symbol rm ON gd.run_id = rm.run_id
LEFT JOIN trades t
  ON t.run_id = gd.run_id
 AND t.signal_id = gd.signal_id
LEFT JOIN signals s
  ON s.run_id = gd.run_id
 AND s.signal_id = gd.signal_id
WHERE gd.run_id = rm.run_id
  AND gd.decision='FAIL'
ORDER BY gd.ts DESC, gd.gate_id;`,
      pnl: `${RUN_META_CTE}
SELECT
  t.signal_id AS trade_id,
  rm.symbol || ' ' || t.side || ' @' || printf('%.2f', t.entry_px) || ' -> ' || printf('%.2f', t.exit_px) AS trade_name,
  t.side AS direction,
  t.entry_ts AS entry_utc,
  t.exit_ts AS exit_utc,
  ROUND(t.entry_px, 4) AS entry_price,
  ROUND(t.exit_px, 4) AS exit_price,
  ROUND(t.pnl_points, 4) AS pnl_points,
  t.minutes_held,
  t.exit_reason
FROM trades t
JOIN run_meta_symbol rm ON t.run_id = rm.run_id
ORDER BY t.pnl_points DESC;`,
      tradesDetailed: `${RUN_META_CTE}
SELECT
  t.signal_id AS trade_id,
  rm.symbol || ' ' || t.side || ' @' || printf('%.2f', t.entry_px) || ' -> ' || printf('%.2f', t.exit_px) AS trade_name,
  t.side AS direction,
  t.entry_ts AS entry_utc,
  t.exit_ts AS exit_utc,
  ROUND(t.entry_px, 4) AS entry_price,
  ROUND(t.stop_px, 4) AS stop_price,
  ROUND(t.target_px, 4) AS target_price,
  ROUND(t.exit_px, 4) AS exit_price,
  ROUND(t.pnl_points, 4) AS pnl_points,
  t.bars_held,
  t.minutes_held,
  t.exit_reason,
  COALESCE(SUM(CASE WHEN gd.decision='FAIL' THEN 1 ELSE 0 END), 0) AS failed_gates,
  COALESCE(GROUP_CONCAT(CASE WHEN gd.decision='FAIL' THEN gd.gate_id || ':' || COALESCE(gd.denial_code, 'FAIL') END, ' | '), '') AS gate_fail_summary
FROM trades t
JOIN run_meta_symbol rm
  ON t.run_id = rm.run_id
LEFT JOIN gate_decisions gd
  ON gd.run_id = t.run_id
 AND gd.signal_id = t.signal_id
WHERE t.run_id = rm.run_id
GROUP BY
  t.signal_id, rm.symbol, t.side, t.entry_ts, t.exit_ts, t.entry_px, t.stop_px, t.target_px,
  t.exit_px, t.pnl_points, t.bars_held, t.minutes_held, t.exit_reason
ORDER BY t.entry_ts DESC;`,
      gateMatrix: `${RUN_META_CTE}
SELECT
  t.signal_id AS trade_id,
  rm.symbol || ' ' || t.side || ' @' || printf('%.2f', t.entry_px) || ' -> ' || printf('%.2f', t.exit_px) AS trade_name,
  t.side AS direction,
  t.entry_ts AS entry_utc,
  t.exit_ts AS exit_utc,
  ROUND(t.pnl_points, 4) AS pnl_points,
  MAX(CASE WHEN gd.gate_id='G0' THEN gd.decision END) AS G0,
  MAX(CASE WHEN gd.gate_id='G1' THEN gd.decision END) AS G1,
  MAX(CASE WHEN gd.gate_id='G2' THEN gd.decision END) AS G2,
  MAX(CASE WHEN gd.gate_id='G3' THEN gd.decision END) AS G3,
  MAX(CASE WHEN gd.gate_id='G4' THEN gd.decision END) AS G4,
  MAX(CASE WHEN gd.gate_id='G5' THEN gd.decision END) AS G5
FROM trades t
JOIN run_meta_symbol rm
  ON t.run_id = rm.run_id
LEFT JOIN gate_decisions gd
  ON gd.run_id = t.run_id
 AND gd.signal_id = t.signal_id
WHERE t.run_id = rm.run_id
GROUP BY t.signal_id, rm.symbol, t.side, t.entry_px, t.exit_px, t.entry_ts, t.exit_ts, t.pnl_points
ORDER BY t.entry_ts DESC;`,
      tradesPassDetailed: `${RUN_META_CTE}
SELECT
  tp.signal_id AS trade_id,
  rm.symbol || ' ' || tp.side || ' @' || printf('%.2f', tp.entry_px) || ' -> ' || printf('%.2f', tp.exit_px) AS trade_name,
  tp.side AS direction,
  tp.gate_id AS gate,
  tp.entry_ts AS entry_utc,
  tp.exit_ts AS exit_utc,
  ROUND(tp.entry_px, 4) AS entry_price,
  ROUND(tp.exit_px, 4) AS exit_price,
  ROUND(tp.size, 4) AS size,
  ROUND(tp.pnl, 4) AS pnl_dollars,
  ROUND(tp.pnl_points, 4) AS pnl_points,
  tp.hold_minutes,
  tp.exit_reason
FROM trades_pass tp
JOIN run_meta_symbol rm ON tp.run_id = rm.run_id
ORDER BY tp.entry_ts DESC, tp.gate_id;`,
      signalsDetailed: `${RUN_META_CTE}
SELECT
  s.signal_id AS signal_id,
  COALESCE(json_extract(s.features_json, '$.symbol'), rm.symbol) AS symbol,
  s.ts AS signal_ts_utc,
  CASE
    WHEN s.direction = 1 THEN 'LONG'
    WHEN s.direction = -1 THEN 'SHORT'
    ELSE COALESCE(CAST(s.direction AS TEXT), '')
  END AS direction,
  COALESCE(json_extract(s.features_json, '$.vix_regime'), 'UNKNOWN') AS regime,
  COALESCE(json_extract(s.features_json, '$.signal_description'), '') AS signal_description,
  COALESCE(json_extract(s.features_json, '$.strategy_rationale'), '') AS strategy_rationale,
  COALESCE(json_extract(s.features_json, '$.entry_ts_et'), '') AS entry_ts_et,
  COALESCE(json_extract(s.features_json, '$.exit_ts_et'), '') AS exit_ts_et,
  COALESCE(json_extract(s.features_json, '$.entry_px'), '') AS entry_px,
  COALESCE(json_extract(s.features_json, '$.exit_px'), '') AS exit_px,
  COALESCE(json_extract(s.features_json, '$.pnl_points'), '') AS pnl_points,
  COALESCE(s.features_json, '') AS features_json_raw
FROM signals s
JOIN run_meta_symbol rm ON s.run_id = rm.run_id
ORDER BY s.ts DESC;`
    };

    const PRESETS = {
      latest: QUERY_TEMPLATES.latest,
      gateHealth: QUERY_TEMPLATES.gateHealth,
      denials: QUERY_TEMPLATES.denials,
      pnl: QUERY_TEMPLATES.pnl,
    };

    const HUMAN_VIEWS = [
      { name: "Trades (Detailed)", template: QUERY_TEMPLATES.tradesDetailed, schemaTable: "trades" },
      { name: "Trade Gate Matrix", template: QUERY_TEMPLATES.gateMatrix, schemaTable: "gate_decisions" },
      { name: "Trade Denials Timeline", template: QUERY_TEMPLATES.denials, schemaTable: "gate_decisions" },
      { name: "Trades Pass (by Gate)", template: QUERY_TEMPLATES.tradesPassDetailed, schemaTable: "trades_pass" },
      { name: "Signals (Detailed)", template: QUERY_TEMPLATES.signalsDetailed, schemaTable: "signals" },
    ];

    const RAW_TABLE_TEMPLATES = {
      trades: QUERY_TEMPLATES.tradesDetailed,
      trades_pass: QUERY_TEMPLATES.tradesPassDetailed,
      gate_decisions: QUERY_TEMPLATES.denials,
      signals: QUERY_TEMPLATES.signalsDetailed,
      runs: "SELECT run_id, created_at_utc, date_start_et, date_end_et, report_path FROM runs ORDER BY created_at_utc DESC;",
      gate_metrics: "SELECT * FROM gate_metrics WHERE run_id=__RUN_ID__ ORDER BY gate_id;",
      gate_daily_stats: "SELECT * FROM gate_daily_stats WHERE run_id=__RUN_ID__ ORDER BY gate_id, session_date;",
    };

    let activeTemplate = "";
    let currentStageId = "";

    function esc(v) {
      if (v === null || v === undefined) return "";
      return String(v).replaceAll("&","&amp;").replaceAll("<","&lt;").replaceAll(">","&gt;");
    }

    function prettyLabel(key) {
      return String(key || "")
        .replaceAll("_", " ")
        .replaceAll(/\\b\\w/g, (ch) => ch.toUpperCase());
    }

    function renderKVTable(el, dataObj) {
      const entries = Object.entries(dataObj || {});
      if (!entries.length) {
        el.innerHTML = `<div class="muted">No data</div>`;
        return;
      }
      const rows = entries
        .map(([k, v]) => {
          const vv = v === null || v === undefined || v === "" ? "n/a" : String(v);
          return `<tr><td>${esc(prettyLabel(k))}</td><td>${esc(vv)}</td></tr>`;
        })
        .join("");
      el.innerHTML = `<table class="kv-table"><tbody>${rows}</tbody></table>`;
    }

    function sqlRunLiteral() {
      const raw = runIdEl.value.trim();
      if (!raw) return "(SELECT MAX(run_id) FROM runs)";
      const escaped = raw.replaceAll("'", "''");
      return `'${escaped}'`;
    }

    function renderTemplate(templateSql) {
      return String(templateSql || "").replaceAll("__RUN_ID__", sqlRunLiteral());
    }

    function setTemplateAndRender(templateSql) {
      activeTemplate = templateSql || "";
      if (activeTemplate) sqlEl.value = renderTemplate(activeTemplate);
    }

    function setLeftPane(px) {
      const maxByScreen = Math.max(420, Math.floor(window.innerWidth * 0.5));
      const n = Math.max(240, Math.min(maxByScreen, Number(px) || 360));
      document.documentElement.style.setProperty("--left-pane", `${n}px`);
    }

    function appendStageLog(line) {
      const stamp = new Date().toISOString();
      const existing = String(ollamaStageLogEl.textContent || "");
      const next = `${existing}${existing ? "\\n" : ""}[${stamp}] ${line}`;
      const lines = next.split("\\n");
      const clipped = lines.slice(Math.max(0, lines.length - 250)).join("\\n");
      ollamaStageLogEl.textContent = clipped;
      ollamaStageLogEl.scrollTop = ollamaStageLogEl.scrollHeight;
    }

    function applyStage(stage) {
      if (!stage || typeof stage !== "object") return;
      currentStageId = String(stage.stage_id || currentStageId || "");
      if (currentStageId) {
        ollamaStageIdEl.value = currentStageId;
      }
      const done = Number(stage.processed_chunks || 0) + Number(stage.error_chunks || 0);
      const total = Number(stage.total_chunks || 0);
      const pct = Number(stage.progress_pct || 0);
      ollamaStageProgressEl.value = Math.max(0, Math.min(100, pct));
      ollamaStageProgressLabelEl.textContent = `${pct.toFixed(1)}% (${done}/${total}) status=${stage.status || "unknown"}`;
    }

    async function refreshChunkList(stageId) {
      const sid = String(stageId || ollamaStageIdEl.value || "").trim();
      if (!sid) return;
      try {
        const r = await fetch(`/api/ollama/chunks?stage_id=${encodeURIComponent(sid)}&limit=120`);
        const data = await r.json();
        if (!data.ok) {
          appendStageLog(`chunk list error: ${data.error || "unknown"}`);
          return;
        }
        applyStage(data.stage || {});
        const chunks = Array.isArray(data.chunks) ? data.chunks : [];
        const summary = chunks
          .slice(-10)
          .map((c) => `#${c.chunk_index} ${c.status} rows=${c.row_count || 0} thumb=${c.chunk_thumb || ""} mem=${c.memory_id || ""}${c.error_text ? ` err=${c.error_text}` : ""}`)
          .join("\\n");
        if (summary) {
          appendStageLog(`recent chunks:\\n${summary}`);
        }
      } catch (err) {
        appendStageLog(`chunk list error: ${String(err)}`);
      }
    }

    async function loadStage(stageId) {
      const sid = String(stageId || ollamaStageIdEl.value || "").trim();
      if (!sid) {
        ollamaMetaEl.textContent = "set a stage id first";
        return;
      }
      try {
        const r = await fetch(`/api/ollama/stage?stage_id=${encodeURIComponent(sid)}`);
        const data = await r.json();
        if (!data.ok) {
          ollamaMetaEl.textContent = `stage load error: ${data.error || "unknown"}`;
          return;
        }
        applyStage(data.stage || {});
        const st = data.stage || {};
        ollamaMetaEl.textContent = `stage=${st.stage_id || sid} model=${st.model || "unknown"} chunks=${st.total_chunks || 0} processed=${st.processed_chunks || 0} errors=${st.error_chunks || 0}`;
        appendStageLog(`loaded stage ${st.stage_id || sid}`);
        await refreshChunkList(sid);
      } catch (err) {
        ollamaMetaEl.textContent = `stage load error: ${String(err)}`;
      }
    }

    async function createStage() {
      const runId = runIdEl.value.trim();
      const model = (ollamaModelEl.value || "").trim();
      const prompt = (ollamaPromptEl.value || "").trim();
      const timeoutS = Number(ollamaTimeoutEl.value || 300);
      const includeReport = !!ollamaIncludeReportEl.checked;
      const chunkSize = Number(ollamaChunkSizeEl.value || 40);
      const maxRows = Number(ollamaMaxRowsEl.value || 0);
      const payload = {
        run_id: runId,
        model: model,
        prompt: prompt,
        timeout_s: Number.isFinite(timeoutS) ? timeoutS : 300,
        include_report: includeReport,
        context_max_chars: 90000,
        chunk_size: Number.isFinite(chunkSize) ? chunkSize : 40,
        max_rows: Number.isFinite(maxRows) ? maxRows : 0,
        sql: sqlEl.value || "",
      };
      ollamaMetaEl.textContent = "creating chunk stage...";
      try {
        const r = await fetch("/api/ollama/stage", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(payload),
        });
        const data = await r.json();
        if (!data.ok) {
          ollamaMetaEl.textContent = `stage create error: ${data.error || "unknown"}`;
          return;
        }
        applyStage(data.stage || {});
        appendStageLog(`created stage ${data.stage?.stage_id || ""} total_chunks=${data.stage?.total_chunks || 0}`);
        ollamaMetaEl.textContent = `stage created: ${data.stage?.stage_id || ""}`;
      } catch (err) {
        ollamaMetaEl.textContent = `stage create error: ${String(err)}`;
      }
    }

    async function processNextChunk() {
      const sid = String(ollamaStageIdEl.value || currentStageId || "").trim();
      if (!sid) {
        ollamaMetaEl.textContent = "set/create a stage first";
        return;
      }
      ollamaMetaEl.textContent = `processing next chunk for ${sid}...`;
      try {
        const r = await fetch("/api/ollama/process-next", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ stage_id: sid }),
        });
        const data = await r.json();
        if (!data.ok) {
          ollamaMetaEl.textContent = `process error: ${data.error || data.chunk?.error || "unknown"}`;
          if (data.chunk) appendStageLog(`#${data.chunk.chunk_index} error ${data.chunk.error || ""}`);
          return;
        }
        applyStage(data.stage || {});
        if (data.chunk) {
          const c = data.chunk;
          appendStageLog(`#${c.chunk_index} ${c.status} rows=${c.query_row_count || 0} thumb=${c.chunk_thumb || ""} mem=${c.memory_id || ""}`);
        } else {
          appendStageLog(data.message || "no queued chunks remaining");
        }
        ollamaMetaEl.textContent = `processed next chunk for ${sid}`;
      } catch (err) {
        ollamaMetaEl.textContent = `process error: ${String(err)}`;
      }
    }

    async function processChunkBatch() {
      const sid = String(ollamaStageIdEl.value || currentStageId || "").trim();
      if (!sid) {
        ollamaMetaEl.textContent = "set/create a stage first";
        return;
      }
      const maxChunks = Number(ollamaMaxChunksEl.value || 20);
      ollamaMetaEl.textContent = `processing batch for ${sid}...`;
      try {
        const r = await fetch("/api/ollama/process-all", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            stage_id: sid,
            max_chunks: Number.isFinite(maxChunks) ? maxChunks : 20,
          }),
        });
        const data = await r.json();
        if (!data.ok) {
          ollamaMetaEl.textContent = `batch error: ${data.error || "unknown"}`;
          return;
        }
        applyStage(data.stage || {});
        const chunks = Array.isArray(data.chunks) ? data.chunks : [];
        for (const c of chunks) {
          appendStageLog(`#${c.chunk_index} ${c.status} rows=${c.query_row_count || 0} thumb=${c.chunk_thumb || ""} mem=${c.memory_id || ""}${c.error ? ` err=${c.error}` : ""}`);
        }
        ollamaMetaEl.textContent = `batch processed=${data.processed_now || 0} stage=${sid}`;
      } catch (err) {
        ollamaMetaEl.textContent = `batch error: ${String(err)}`;
      }
    }

    function adjustLeftPane(delta) {
      const cur = parseInt(getComputedStyle(document.documentElement).getPropertyValue("--left-pane"), 10) || 320;
      setLeftPane(cur + delta);
    }

    function addSection(title) {
      const el = document.createElement("div");
      el.className = "muted";
      el.style.padding = "8px 8px 6px 8px";
      el.style.fontWeight = "600";
      el.textContent = title;
      tablesDiv.appendChild(el);
    }

    async function loadTables() {
      const r = await fetch("/api/tables");
      const data = await r.json();
      if (!data.ok) return;
      tablesDiv.innerHTML = "";

      addSection("Human Views");
      for (const hv of HUMAN_VIEWS) {
        const b = document.createElement("button");
        b.innerHTML = `<strong>${esc(hv.name)}</strong>`;
        b.style.background = "#f7fbf8";
        b.onclick = async () => {
          setTemplateAndRender(hv.template);
          await runQuery();
          if (hv.schemaTable) await loadSchema(hv.schemaTable);
        };
        tablesDiv.appendChild(b);
      }

      addSection("Raw Tables");
      for (const t of data.tables || []) {
        const b = document.createElement("button");
        b.innerHTML = `<strong>${esc(t.table)}</strong> <span class="muted">(${t.rows} rows)</span>`;
        b.onclick = async () => {
          const tpl = RAW_TABLE_TEMPLATES[t.table] || `SELECT * FROM ${t.table} ORDER BY ROWID DESC;`;
          setTemplateAndRender(tpl);
          await runQuery();
          await loadSchema(t.table);
        };
        tablesDiv.appendChild(b);
      }
    }

    async function fetchLatestRunId() {
      const r = await fetch("/api/query", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ sql: "SELECT MAX(run_id) AS run_id FROM runs", limit: 1 }),
      });
      const data = await r.json();
      if (!data.ok || !data.rows || !data.rows.length) return "";
      return String(data.rows[0][0] || "").trim();
    }

    async function loadSchema(table) {
      const r = await fetch(`/api/schema?table=${encodeURIComponent(table)}`);
      const data = await r.json();
      if (!data.ok) {
        schemaEl.innerHTML = `<pre>${esc(data.error)}</pre>`;
        return;
      }
      schemaEl.innerHTML = `<pre>${esc(JSON.stringify(data.rows, null, 2))}</pre>`;
    }

    async function loadOllamaModels() {
      try {
        const r = await fetch("/api/ollama/models");
        const data = await r.json();
        if (!data.ok) {
          ollamaMetaEl.textContent = `model fetch error: ${data.error || "unknown"}`;
          return;
        }

        const models = Array.isArray(data.models) ? data.models : [];
        const preferred = String(data.default_model || "").trim();
        ollamaModelEl.innerHTML = "";

        if (!models.length) {
          const opt = document.createElement("option");
          opt.value = preferred || "llama3.2:1b";
          opt.textContent = preferred || "llama3.2:1b";
          ollamaModelEl.appendChild(opt);
          ollamaMetaEl.textContent = `No models discovered via /api/tags. Base: ${data.base_url || "unknown"}`;
          return;
        }

        for (const m of models) {
          const opt = document.createElement("option");
          opt.value = m;
          opt.textContent = m;
          if (preferred && m === preferred) opt.selected = true;
          ollamaModelEl.appendChild(opt);
        }
        ollamaMetaEl.textContent = `Ollama: ${data.base_url || "unknown"} | models=${models.length}`;
      } catch (err) {
        ollamaMetaEl.textContent = `model fetch error: ${String(err)}`;
      }
    }

    async function loadOllamaMemory() {
      try {
        const runId = runIdEl.value.trim();
        const q = new URLSearchParams();
        if (runId) q.set("run_id", runId);
        q.set("limit", "12");
        const r = await fetch(`/api/ollama/memory?${q.toString()}`);
        const data = await r.json();
        if (!data.ok) {
          ollamaMetaEl.textContent = `memory load error: ${data.error || "unknown"}`;
          return;
        }
        const rows = Array.isArray(data.rows) ? data.rows : [];
        if (!rows.length) {
          ollamaMetaEl.textContent = "No saved LLM memory for this run yet.";
          return;
        }
        const first = rows[0] || {};
        const items = rows.map((row) => {
          const stamp = row.created_at_utc || "";
          const mode = row.analysis_mode || "";
          const model = row.model || "";
          const qrows = row.query_row_count ?? 0;
          const err = row.error_text ? ` error=${row.error_text}` : "";
          return `- [${stamp}] ${mode} ${model} rows=${qrows}${err}`;
        });
        ollamaOutputEl.textContent = items.join("\\n");
        ollamaMetaEl.textContent = `Loaded ${rows.length} memory entries from ${data.memory_db_path || "memory db"}; latest id=${first.memory_id || "n/a"}`;
      } catch (err) {
        ollamaMetaEl.textContent = `memory load error: ${String(err)}`;
      }
    }

    async function runOllamaAnalysis(mode) {
      const runId = runIdEl.value.trim();
      const model = (ollamaModelEl.value || "").trim();
      const prompt = (ollamaPromptEl.value || "").trim();
      const limit = Number(ollamaLimitEl.value || 60);
      const timeoutS = Number(ollamaTimeoutEl.value || 300);
      const includeReport = !!ollamaIncludeReportEl.checked;
      const saveMemory = !!ollamaSaveMemoryEl.checked;
      const includeSql = mode === "query";
      const payload = {
        run_id: runId,
        model: model,
        prompt: prompt,
        limit: Number.isFinite(limit) ? limit : 60,
        timeout_s: Number.isFinite(timeoutS) ? timeoutS : 300,
        include_report: includeReport,
        save_memory: saveMemory,
        analysis_mode: includeSql ? "query" : "run",
        sql: includeSql ? sqlEl.value : "",
      };

      ollamaOutputEl.textContent = "Running local model analysis...";
      ollamaMetaEl.textContent = `Submitting ${includeSql ? "current query" : "run-wide"} analysis (rows=${payload.limit}, timeout=${payload.timeout_s}s)...`;
      try {
        const r = await fetch("/api/ollama/analyze", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(payload),
        });
        const data = await r.json();
        if (!data.ok) {
          ollamaOutputEl.textContent = "";
          ollamaMetaEl.textContent = `analysis error: ${data.error || "unknown"}`;
          return;
        }

        ollamaOutputEl.textContent = data.answer || "";
        const trunc = data.context_truncated ? " | context truncated" : "";
        const mem = data.memory_saved ? ` | memory_id=${data.memory_id ?? "n/a"}` : "";
        ollamaMetaEl.textContent =
          `model=${data.model || model || "unknown"} | rows=${data.query_row_count ?? "n/a"} | elapsed_ms=${data.query_elapsed_ms ?? "n/a"} | timeout=${data.timeout_s ?? payload.timeout_s}s${trunc}${mem}`;
      } catch (err) {
        ollamaOutputEl.textContent = "";
        ollamaMetaEl.textContent = `analysis error: ${String(err)}`;
      }
    }

    async function refreshInsights() {
      const runRaw = runIdEl.value.trim();
      const r = await fetch(`/api/insights?run_id=${encodeURIComponent(runRaw)}`);
      const data = await r.json();
      if (!data.ok) {
        basicMetricsEl.innerHTML = `<div class="muted">Error: ${esc(data.error || "unknown")}</div>`;
        complexMetricsEl.innerHTML = "";
        narrativeEl.textContent = "";
        reportMarkdownEl.textContent = "";
        chartNoteEl.textContent = "";
        equityChartEl.removeAttribute("src");
        return;
      }

      if (!runRaw && data.run_id) {
        runIdEl.value = String(data.run_id);
      }

      renderKVTable(basicMetricsEl, data.basic_metrics || {});
      renderKVTable(complexMetricsEl, data.complex_metrics || {});
      narrativeEl.textContent = data.narrative || "";
      reportMarkdownEl.textContent = data.report_markdown || "No report markdown found for this run.";

      if (data.chart_available && data.run_id) {
        const src = `/api/chart?run_id=${encodeURIComponent(data.run_id)}&t=${Date.now()}`;
        equityChartEl.src = src;
        equityChartEl.style.display = "block";
        chartNoteEl.textContent = data.chart_path ? `Source: ${data.chart_path}` : "";
      } else {
        equityChartEl.removeAttribute("src");
        equityChartEl.style.display = "none";
        chartNoteEl.textContent = data.chart_path ? `Chart missing: ${data.chart_path}` : "No chart available for this run.";
      }
    }

    async function runQuery() {
      if (activeTemplate) {
        sqlEl.value = renderTemplate(activeTemplate);
      }
      const payload = { sql: sqlEl.value, limit: Number(limitEl.value || 200) };
      const r = await fetch("/api/query", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
      const data = await r.json();
      if (!data.ok) {
        metaEl.textContent = `error: ${data.error}`;
        resultEl.innerHTML = "";
        return;
      }
      metaEl.textContent = `rows=${data.row_count} elapsed_ms=${data.elapsed_ms}`;
      const headers = (data.columns || []).map(c => `<th>${esc(c)}</th>`).join("");
      const rows = (data.rows || []).map(
        row => `<tr>${row.map(v => `<td>${esc(v)}</td>`).join("")}</tr>`
      ).join("");
      resultEl.innerHTML = `<table><thead><tr>${headers}</tr></thead><tbody>${rows}</tbody></table>`;
      await refreshInsights();
    }

    function runPreset(key) {
      const q = PRESETS[key];
      if (!q) return;
      setTemplateAndRender(q);
      runQuery();
    }

    document.getElementById("run").onclick = runQuery;
    document.getElementById("use-latest-run").onclick = async () => {
      const latest = await fetchLatestRunId();
      if (latest) runIdEl.value = latest;
      if (activeTemplate) sqlEl.value = renderTemplate(activeTemplate);
      runQuery();
    };
    document.getElementById("wider").onclick = () => adjustLeftPane(24);
    document.getElementById("narrower").onclick = () => adjustLeftPane(-24);
    let userSetSqlHeight = false;
    queryHeightEl.oninput = () => {
      userSetSqlHeight = true;
      sqlEl.style.height = `${Number(queryHeightEl.value || 180)}px`;
    };
    let userSetHeight = false;
    tableHeightEl.oninput = () => {
      userSetHeight = true;
      resultEl.style.height = `${Number(tableHeightEl.value || 550)}px`;
    };
    wrapBtn.onclick = () => {
      const noWrap = resultEl.classList.toggle("nowrap");
      wrapBtn.textContent = noWrap ? "Wrap Cells" : "No Wrap";
    };
    document.getElementById("preset-latest").onclick = () => runPreset("latest");
    document.getElementById("preset-gate-health").onclick = () => runPreset("gateHealth");
    document.getElementById("preset-denials").onclick = () => runPreset("denials");
    document.getElementById("preset-pnl").onclick = () => runPreset("pnl");
    document.getElementById("ollama-refresh").onclick = loadOllamaModels;
    document.getElementById("ollama-load-memory").onclick = loadOllamaMemory;
    document.getElementById("ollama-analyze-run").onclick = () => runOllamaAnalysis("run");
    document.getElementById("ollama-analyze-query").onclick = () => runOllamaAnalysis("query");
    document.getElementById("ollama-stage-create").onclick = createStage;
    document.getElementById("ollama-stage-next").onclick = processNextChunk;
    document.getElementById("ollama-stage-all").onclick = processChunkBatch;
    document.getElementById("ollama-stage-refresh").onclick = () => loadStage("");
    ollamaStageIdEl.addEventListener("change", () => loadStage(""));
    runIdEl.addEventListener("change", () => {
      if (activeTemplate) sqlEl.value = renderTemplate(activeTemplate);
      refreshInsights();
      loadOllamaMemory();
    });
    sqlEl.addEventListener("input", () => {
      // If the user edits SQL manually, stop auto-overwriting with a template.
      activeTemplate = "";
    });

    // Drag-to-resize splitter (desktop)
    let dragging = false;
    splitterEl.addEventListener("mousedown", () => { dragging = true; document.body.style.cursor = "col-resize"; });
    window.addEventListener("mouseup", () => { dragging = false; document.body.style.cursor = ""; });
    window.addEventListener("mousemove", (ev) => {
      if (!dragging || window.innerWidth <= 980) return;
      const wrapRect = document.querySelector(".wrap").getBoundingClientRect();
      const x = ev.clientX - wrapRect.left - 12;
      setLeftPane(x);
    });

    function fitResultHeight() {
      if (userSetHeight) return;
      const target = Math.max(300, Math.min(900, Math.floor(window.innerHeight * 0.52)));
      tableHeightEl.value = String(target);
      resultEl.style.height = `${target}px`;
    }

    function fitSqlHeight() {
      if (userSetSqlHeight) return;
      const target = Math.max(150, Math.min(460, Math.floor(window.innerHeight * 0.22)));
      queryHeightEl.value = String(target);
      sqlEl.style.height = `${target}px`;
    }

    window.addEventListener("resize", () => {
      fitSqlHeight();
      fitResultHeight();
      if (window.innerWidth <= 980) setLeftPane(320);
    });

    fitSqlHeight();
    fitResultHeight();
    setLeftPane(360);
    loadOllamaModels();
    loadTables().then(async () => {
      const latest = await fetchLatestRunId();
      if (latest) runIdEl.value = latest;
      runPreset("latest");
      await loadOllamaMemory();
    });
  </script>
</body>
</html>
"""


def cmd_tables(conn: sqlite3.Connection, _args: argparse.Namespace) -> int:
    print_result(list_tables(conn, with_counts=True))
    return 0


def cmd_schema(conn: sqlite3.Connection, args: argparse.Namespace) -> int:
    print_result(table_schema(conn, args.table))
    return 0


def cmd_query(conn: sqlite3.Connection, args: argparse.Namespace) -> int:
    sql = args.sql
    if args.file:
        sql = Path(args.file).read_text(encoding="utf-8")
    if not sql:
        raise ValueError("Provide --sql or --file.")
    res = run_sql(conn, sql, limit=args.limit)
    print_result(res)
    return 0


def cmd_recent_runs(conn: sqlite3.Connection, args: argparse.Namespace) -> int:
    q = f"""
    SELECT run_id, created_at_utc, date_start_et, date_end_et, report_path
    FROM runs
    ORDER BY created_at_utc DESC
    LIMIT {int(args.limit)}
    """
    print_result(run_sql(conn, q))
    return 0


def cmd_web(conn: sqlite3.Connection, args: argparse.Namespace) -> int:
    DbWebHandler.conn = conn
    DbWebHandler.default_limit = int(args.limit)
    memory_db_path = Path(args.memory_db).expanduser().resolve()
    mem_conn = connect_rw(memory_db_path)
    ensure_llm_memory_schema(mem_conn)
    tune_llm_memory_db(mem_conn)
    DbWebHandler.mem_conn = mem_conn
    DbWebHandler.memory_db_path = str(memory_db_path)
    server = HTTPServer((args.host, int(args.port)), DbWebHandler)
    print(f"SQLite explorer running at http://{args.host}:{args.port}")
    print(f"DB: {args.db}")
    print(f"LLM memory DB: {memory_db_path}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
        mem_conn.close()
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Multigate SQLite query + web explorer")
    p.add_argument("--db", default=str(DEFAULT_DB_PATH), help=f"Path to sqlite db (default: {DEFAULT_DB_PATH})")

    sub = p.add_subparsers(dest="cmd", required=True)

    sp = sub.add_parser("tables", help="List tables (with row counts)")
    sp.set_defaults(func=cmd_tables)

    sp = sub.add_parser("schema", help="Show PRAGMA table_info for a table")
    sp.add_argument("table")
    sp.set_defaults(func=cmd_schema)

    sp = sub.add_parser("query", help="Run read-only SQL and print result")
    sp.add_argument("--sql", default="", help="SQL string")
    sp.add_argument("--file", default="", help="Path to .sql file")
    sp.add_argument("--limit", type=int, default=200, help="Result row cap for SELECT/WITH (default: 200)")
    sp.set_defaults(func=cmd_query)

    sp = sub.add_parser("recent-runs", help="Show latest runs")
    sp.add_argument("--limit", type=int, default=20)
    sp.set_defaults(func=cmd_recent_runs)

    sp = sub.add_parser("web", help="Launch browser UI")
    sp.add_argument("--host", default="127.0.0.1")
    sp.add_argument("--port", type=int, default=8765)
    sp.add_argument("--limit", type=int, default=500, help="Default row limit for UI queries")
    sp.add_argument("--memory-db", default=str(DEFAULT_MEMORY_DB_PATH), help=f"Path to LLM memory sqlite (default: {DEFAULT_MEMORY_DB_PATH})")
    sp.set_defaults(func=cmd_web)

    return p


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    db_path = Path(args.db).expanduser().resolve()
    if not db_path.exists():
        raise FileNotFoundError(f"DB file not found: {db_path}")

    conn = connect_ro(db_path)
    try:
        return int(args.func(conn, args))
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
