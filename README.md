# gulfchain — SPY 5m backtest + Multi‑Gate evaluator (local)

This repo is a **local working directory** for running a small SPY 5‑minute backtest and then scoring the resulting trades through a configurable **multi‑gate “Risk Gate”** system.

## What you run (today)
- **Runner:** `multigate.py`
- **DB (current):** `runs/backtests.sqlite` (this is the one your script prints)
- **Gate configs:** `gates/G0.json` … `gates/G5.json`
- **Core logic:** `src/gate_eval.py`

## Quick start
```bash
cd /Users/cole/projects/gulfchain

# venv (create once)
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# run
export PYTHONPATH="$PWD/src"
python multigate.py
```

You’ll see:
- `✅ Completed run_id=...`
- `DB: /Users/cole/Projects/gulfchain/runs/backtests.sqlite`

## Data expectations
`multigate.py` reads your SPY data from `DATA_PATH` (defined near the top of the file).
It expects a timestamp column named **`ts`** or **`timestamp`**. The runner:
- parses timestamps as UTC,
- converts to **America/New_York**,
- filters to RTH (>= 09:45 ET, <= 16:00 ET),
- normalizes into your internal 5‑minute format.

## DB tables you should expect
Inside `runs/backtests.sqlite`:
- `runs` — one row per run_id
- `signals` — signal stream for a run (minimum columns used by gate_eval)
- `trades` — trades keyed by `(run_id, signal_id)`
- `gate_decisions` — PASS/FAIL per gate per signal
- `trades_pass` — PASS trades written by gate_eval
- `gate_daily_stats`, `gate_metrics` — rollups

Helpful sanity checks:
```bash
RUN_ID="$(sqlite3 runs/backtests.sqlite "SELECT MAX(run_id) FROM runs;")"

sqlite3 -header -column runs/backtests.sqlite "
SELECT
  (SELECT COUNT(*) FROM trades WHERE run_id='$RUN_ID')   AS trades,
  (SELECT COUNT(*) FROM signals WHERE run_id='$RUN_ID')  AS signals,
  (SELECT COUNT(*) FROM gate_decisions WHERE run_id='$RUN_ID') AS decisions,
  (SELECT COUNT(*) FROM gate_metrics WHERE run_id='$RUN_ID')   AS metrics,
  (SELECT COUNT(*) FROM trades_pass WHERE run_id='$RUN_ID')    AS pass_trades;
"
```

## Why you saw `G1_current_frozen.json` earlier
That name usually means: “this gate file was **pinned/frozen** as the current reference config so it doesn’t get accidentally edited while other gates are being iterated.”
In your codebase, `src/gate_eval.py` had a special-case that explicitly loaded `G1_current_frozen.json` for G1 (instead of `G1.json`). fileciteturn1file0L111-L111

You’ve already renamed it to `gates/G1.json`. ✅  
Just make sure you also update any remaining references (code + docs).

## Legacy
Anything old / superseded should live under `legacy/` (old SQLite files, old reports, old PNGs, old docs).

Recommended legacy moves (optional):
```bash
mkdir -p legacy/docs
mv README_QUICKSTART.md legacy/docs/README_QUICKSTART_legacy.md
mv README_CLEANED.md    legacy/docs/README_CLEANED_legacy.md
```

## Notes on `.venv`
Stopping the venv (“deactivate”) does **not** delete the `.venv/` folder — it stays on disk until you remove it.
To leave the venv: `deactivate`
To delete it: `rm -rf .venv`
