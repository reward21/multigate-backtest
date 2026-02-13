# 🧪 multigate-backtest

A small, hackable **multi‑gate backtest harness** that:

- 📄 loads run settings from `config.yaml`
- 🔐 loads secrets from `.env` (**NOT committed**)
- 🗃️ writes results to SQLite (`runs/backtests.sqlite`)
- 🚦 evaluates gates **G0–G5** from JSON configs in `gates/` ()

This repo is currently focused on **plumbing correctness + gate evaluation**, not “final strategy alpha.”

> 🐍 **Use a virtual environment.** This repo expects you to run everything from an activated `.venv` (see **Requirements** below).


## ✅ What it does

`multigate.py`:

1. 📥 reads data from `paths.data_path` in `config.yaml`
2. 🕒 normalizes timestamps and (optionally) enforces RTH rules from config
3. 🧱 generates a minimal `signals` + `trades` stream for the run
4. 🧪 evaluates each gate (G0–G5) and logs per‑signal decisions
5. 📊 writes rollups/metrics


## 🧰 Requirements

- Python 3.12+ (recommended)
- `sqlite3` 
- packages in `requirements.txt`

Install:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```


## ⚙️ Configuration

### 1) `config.yaml` (committed ✅)

This is the main editable entry point (paths, runtime settings, gate IDs, etc).

Common knobs:

- `paths.data_path`
- `paths.db_path`
- `runtime.start_equity`
- `runtime.enforce_rth` + `runtime.rth_start` / `runtime.rth_end`
- `gates.gate_ids`

> 🔎 Tip: `multigate.py` reads `runtime.start_equity` from `config.yaml` and will error if it’s missing (by design).


### 2) `.env` (🔐 NOT committed ❌)

Secrets (API keys/tokens) belong in `.env` and must **never** be committed.

Setup:

```bash
cp .env.example .env
```

Then edit `.env` and add your keys.

✅ Make sure `.env` is in `.gitignore` so it cannot be pushed.


## ▶️ Run

From repo root:

```bash
export PYTHONPATH="$PWD/src"
python multigate.py
```

You should see something like:

- `✅ Completed run_id=...`
- `DB: /.../runs/backtests.sqlite`


## 📥 Data ingestion

This repo includes an ingestion pipeline for Databento, Alpaca, and VIX.

For details and examples, see `docs/Ingest_readme.md`.


## 🗃️ DB tables you should expect

Inside `runs/backtests.sqlite`:

- `runs` — one row per `run_id`
- `signals` — signal stream for a run (minimum columns used by `gate_eval`)
- `trades` — trades keyed by `(run_id, signal_id)`
- `gate_decisions` — PASS/FAIL per gate per signal (+ `denial_code` / `denial_detail`)
- `trades_pass` — PASS trades written by gate evaluator
- `gate_daily_stats`, `gate_metrics` — rollups


## ✅ Helpful sanity checks

Latest run id:

```bash
RUN_ID="$(sqlite3 runs/backtests.sqlite "SELECT MAX(run_id) FROM runs;")"
echo "$RUN_ID"
```

Counts:

```bash
sqlite3 -header -column runs/backtests.sqlite "
SELECT
  (SELECT COUNT(*) FROM trades WHERE run_id='$RUN_ID')   AS trades,
  (SELECT COUNT(*) FROM signals WHERE run_id='$RUN_ID')  AS signals,
  (SELECT COUNT(*) FROM gate_decisions WHERE run_id='$RUN_ID') AS decisions,
  (SELECT COUNT(*) FROM gate_metrics WHERE run_id='$RUN_ID')   AS metrics,
  (SELECT COUNT(*) FROM trades_pass WHERE run_id='$RUN_ID')    AS pass_trades;
"
```

Gate PASS/FAIL breakdown:

```bash
sqlite3 -header -column runs/backtests.sqlite "
SELECT gate_id,
       SUM(CASE WHEN decision='PASS' THEN 1 ELSE 0 END) AS pass,
       SUM(CASE WHEN decision='FAIL' THEN 1 ELSE 0 END) AS fail
FROM gate_decisions
WHERE run_id='$RUN_ID'
GROUP BY gate_id
ORDER BY gate_id;
"
```

Denial reasons:

```bash
sqlite3 -header -column runs/backtests.sqlite "
SELECT gate_id, denial_code, COUNT(*) AS n
FROM gate_decisions
WHERE run_id='$RUN_ID' AND decision='FAIL'
GROUP BY gate_id, denial_code
ORDER BY gate_id, n DESC;
"
```

## 🗄️ SQLite explorer (query + visual UI)

This repo includes `scripts/db_tool.py` for clean DB access.

Terminal queries:

```bash
python scripts/db_tool.py tables
python scripts/db_tool.py recent-runs --limit 10
python scripts/db_tool.py query --sql "SELECT * FROM gate_metrics ORDER BY run_id DESC LIMIT 20;"
python scripts/db_tool.py schema trades_pass
```

Browser UI:

```bash
python scripts/db_tool.py web --host 127.0.0.1 --port 8765
```

Then open `http://127.0.0.1:8765` to browse tables, inspect schema, and run read-only SQL interactively.

Quick highlights:
- One-click presets (`Latest Run`, `Gate Health`, `Denials`, `PnL Drilldown`) and human views for trades/signals/gate diagnostics.
- Responsive layout with resizable panes, wrapped/scrollable SQL and JSON containers, and a right-side insights panel.
- Local Ollama analysis with persistent memory (`runs/llm_memory.sqlite`) and recall by `run_id`.

Full docs:
- UI quick guide: `docs/ui_readme.md`
- Developer docs (API, architecture, memory schema, troubleshooting): `docs/sqlite_explorer_dev_docs.md`


## 📝 Notes

- **Single source of truth:** edit `config.yaml` (paths, runtime toggles, gate IDs).
- **Secrets:** put API keys in `.env` (never commit it). Copy `.env.example → .env`.
- **Time handling:** timestamps are parsed as **UTC**, then converted to `runtime.timezone` (default `America/New_York`).
- **RTH filter:** if `runtime.enforce_rth: true`, entries are restricted to `09:45–16:00` ET (see `runtime.rth_start` / `runtime.rth_end`).
- **Gates:** configs live in `gates/G0.json … gates/G5.json`. Repo standard is **`gates/G1.json`** (no `G1_current_frozen.json`).
- **Run outputs:** the SQLite DB writes to `runs/backtests.sqlite` and optional charts/reports go under `runs/artifacts/` (typically local-only).
- **Repro tip:** if results look “off,” double-check `paths.data_path`, timezone, and RTH settings first.


## 🗂️ Repo layout

- `multigate.py` — multi‑gate runner
- `src/` — library code (config, ingest, features, gate eval, io)
- `gates/` — gate JSON configs (G0–G5)
- `runs/` — sqlite DB + generated artifacts (often gitignored)
- `data/` — raw/processed data (usually gitignored)
- `legacy/` — old sqlite files, old reports, old charts, etc.


## 🧹 Local-only / large files (recommended)

Keep these out of GitHub (typically via `.gitignore`):

- `.venv/`
- `__pycache__/`
- `.env`
- `runs/` (SQLite DB + generated artifacts)
- `data/` (raw + processed market data)
- `google_drive/` (downloaded exports)
- `legacy/` (optional: keep local unless you want history in the repo)
- `artifacts/` (only if this folder is generated output)
- `.DS_Store`

See `.gitignore` for the intended policy.
