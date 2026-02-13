# Multigate SQLite Explorer UI

A fast local UI for exploring backtest runs in `runs/backtests.sqlite`, with built-in run insights, report/chart viewing, and local Ollama analysis.

## Quick Start

```bash
cd multigate-backtest
python scripts/db_tool.py web --host 127.0.0.1 --port 8765
```

Open `http://127.0.0.1:8765`.

## What You Can Do

- Browse tables with row counts.
- Use human-friendly presets (`Trades (Detailed)`, `Trade Gate Matrix`, `Signals (Detailed)`, etc).
- Run read-only SQL (`SELECT`, `WITH`, `PRAGMA`, `EXPLAIN`).
- Inspect schema for any table.
- View run-level metrics and artifacts: `Basic Derived`, `Complex Derived`, narrative summary, detailed markdown report, and embedded equity chart.
- Run local LLM analysis with Ollama (`Analyze Run` / `Analyze Query`).
- Persist LLM outputs to memory DB and reload history by run.

## Main Controls

- `Run ID`: blank uses latest run.
- `Use Latest`: resolves and fills latest `run_id`.
- `Limit`: row cap for query results.
- `Query H`: SQL editor height.
- `Table H`: result table height.
- `No Wrap`: toggle table cell wrapping.
- `Wider Left` / `Narrower Left`: side pane width.

## Local LLM Panel: Workflow and Definitions

### Fast one-shot workflow

1. Set `Model`, `Rows`, `Timeout(s)`, optional `include report excerpt`.
2. Click `Analyze Run` (run-wide) or `Analyze Query` (current SQL).
3. If `save memory` is enabled, the result is written to `llm_memory`.

### Chunked workflow (large analyses)

1. Set `Chunk` (rows per chunk), `MaxRows` (total staged rows cap), and `Batch` (chunks to process per click).
2. Click `Stage Chunks` to create a persistent chunk stage (`Stage ID` like `stg_xxx`).
3. Click `Process Next` for one chunk, or `Process Batch` for up to `Batch` chunks.
4. Use `Refresh Stage` or paste a known `Stage ID` to resume and inspect progress.

### Control definitions

- `Model`: Ollama model for analysis requests.
- `Refresh Models`: reloads local model list from Ollama.
- `Rows`: one-shot analysis row cap.
- `Timeout(s)`: per-request timeout.
- `include report excerpt`: includes report markdown excerpt in context.
- `save memory`: persists outputs/errors to memory DB.
- `Load Memory`: reads recent saved entries and shows compact history lines.
- `Chunk`: rows processed per chunk item.
- `MaxRows`: max staged rows (`0` means no extra cap).
- `Batch`: max chunk items processed by one `Process Batch` click.
- `Stage Chunks`: creates chunk stage and queued chunk items.
- `Process Next`: processes one queued chunk.
- `Process Batch`: processes up to `Batch` queued chunks.
- `Refresh Stage`: reloads stage state and recent chunk statuses.
- `Stage ID`: stage identifier used to resume/inspect prior stages.

### Progress and logs

- The progress bar shows `processed + error` out of `total_chunks`.
- Stage statuses: `staged`, `running`, `completed`, `failed`.
- Stage log is verbose and includes chunk index, mini thumbprint, rows analyzed, memory id, and errors.

## LLM Memory

- Default memory DB: `runs/llm_memory.sqlite`.
- Saved automatically after each analysis when `save memory` is checked.
- History can be loaded per current `run_id`.
- Tuned for local usage with WAL journaling and read indexes.
- Memory tracks one-shot entries plus chunk entries (`analysis_mode=chunk`).
- Chunk staging state is persisted in:
  - `llm_chunk_stages` (pipeline-level status/counters)
  - `llm_chunk_items` (per-chunk queue/status/thumbprint/memory linkage)

## Inspect Memory DB

Quick health check:

```bash
sqlite3 runs/llm_memory.sqlite "SELECT COUNT(*) AS rows_total, SUM(CASE WHEN error_text<>'' THEN 1 ELSE 0 END) AS error_rows, MIN(created_at_utc) AS first_row, MAX(created_at_utc) AS last_row FROM llm_memory;"
```

Recent memory rows:

```bash
sqlite3 runs/llm_memory.sqlite "SELECT memory_id, created_at_utc, run_id, analysis_mode, model, query_row_count, context_truncated, LENGTH(response_markdown) AS response_chars, substr(error_text,1,120) AS error_preview FROM llm_memory ORDER BY memory_id DESC LIMIT 20;"
```

Index and journal checks:

```bash
sqlite3 runs/llm_memory.sqlite "PRAGMA journal_mode; PRAGMA index_list('llm_memory');"
```

Expected index names:
- `idx_llm_memory_run_id_created`
- `idx_llm_memory_created`
- `idx_llm_memory_run_mode_created`
- `idx_llm_memory_context_hash`

## Useful API Endpoints

- `GET /api/tables`
- `GET /api/schema?table=trades`
- `POST /api/query`
- `GET /api/insights?run_id=<id>`
- `GET /api/chart?run_id=<id>`
- `GET /api/ollama/models`
- `POST /api/ollama/analyze`
- `GET /api/ollama/memory?run_id=<id>&limit=20`
- `POST /api/ollama/stage`
- `GET /api/ollama/stage?stage_id=<id>`
- `GET /api/ollama/chunks?stage_id=<id>&limit=120`
- `POST /api/ollama/process-next`
- `POST /api/ollama/process-all`

## Common Issues

- Ollama timeout: lower `Rows`, uncheck `include report excerpt`, and increase `Timeout(s)`.
- If model list is empty, verify Ollama is reachable at `OLLAMA_URL` (default `http://127.0.0.1:11434`).
- If performance is slow, run one analysis at a time and prefer `llama3.2:1b` for fast iteration.
