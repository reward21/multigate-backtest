# Gulf Chain — Risk Gate Multi-Gate Pack (G0–G5)

Generated: **2026-01-29 (CT)**  
Purpose: Config-driven Risk Gate variants to evaluate the **same signal stream / run_id** across multiple gates.

---

## 0) Core concept

For each `run_id`, replay the **same signal stream** through **six** gate configs:

- **G0**: Shadow baseline (no gate)
- **G1**: Current gate (canonical “as-is”)
- **G2**: Survival-only gate (minimal account protection)
- **G3**: Session + survival gate (time structure)
- **G4**: Adaptive “second chance” gate (controlled re-entry + controlled adds)
- **G5**: Strict tail-risk gate (drawdown discipline)

Each gate must produce:

- PASS trades executed (official equity curve)
- FAIL trades shadowed (denied signals still simulated; “shadow P&L”)
- decision logs (PASS/FAIL + denial reason codes)
- metrics + confusion-matrix-style comparisons vs shadow-all

---

## 1) Minimal config schema (per gate)

Represent each gate as one JSON/YAML config object.

**Required fields**
- `gate_id`: `"G0"` … `"G5"`
- `name`: string
- `rules`: object of enabled/disabled rules + parameters
- `denial_reason_map`: canonical code list (must match logging)

**Suggested rule keys**
- `daily_kill_switch`
  - `enabled: bool`
  - `threshold_type: "R" | "pct_equity" | "usd"`
  - `threshold_value: number`
- `per_trade_risk_cap`
  - `enabled: bool`
  - `cap_type: "R" | "pct_equity" | "usd"`
  - `cap_value: number`
- `max_trades_per_day`
  - `enabled: bool`
  - `value: int`
- `max_consecutive_losses`
  - `enabled: bool`
  - `value: int`
- `cooldown_minutes_after_stop`
  - `enabled: bool`
  - `value: int`
- `max_open_positions`
  - `enabled: bool`
  - `value: int`
- `session_rules` (ET)
  - `enabled: bool`
  - `no_entries_before: "09:45"`
  - `no_new_entries_after: "15:00"`
  - `force_flat_by: "15:30"`
- `vix_regime_filter`
  - `enabled: bool`
  - `allowed_regimes: ["LOW","MID"]` (example)
- `liquidity_filters`
  - `enabled: bool`
  - `max_bid_ask_spread: number`
  - `min_volume: int`
  - `min_open_interest: int`
- `overnight_policy`
  - `enabled: bool`
  - `allow_overnight: bool`
  - `max_pct_equity_at_risk_overnight: number`
- `reentry_policy`
  - `enabled: bool`
  - `max_reentries_per_day: int`
  - `cooldown_minutes: int`
  - `catastrophic_loss_threshold: number`
  - `require_trend_valid: bool`
  - `require_vix_allowed: bool`
- `avg_down_policy`
  - `enabled: bool`
  - `mode: "DISALLOW" | "ALLOW_STRICT"`
  - `max_adds: int`
  - `max_total_exposure_multiplier: number` (e.g., `1.25`)
  - `allowed_add_levels: [...]`
  - `require_recalc_stop: bool`
  - `require_total_risk_within_cap: bool`

---

## 2) Gate definitions (G0–G5)

### G0 — Shadow baseline (no gate)
Execute every signal (still respecting unavoidable engine constraints).  
Purpose: benchmark what gating is “buying” vs “blocking”.  
Rules: all risk rules OFF (except non-optional engine constraints).

### G1 — Current gate (canonical “as-is”)
Mirror the current rule set used as “current strict.”  
Purpose: establish baseline for what we currently believe.  
Rules: no tweaks unless you deliberately promote a change.

### G2 — Survival-only gate (minimal account protection)
Keep ONLY hard survival constraints. Remove selectivity constraints.

Suggested rules ON:
- daily kill switch
- per-trade risk cap
- max open positions = 1
- hard oversizing block (contracts/notional/exposure cap)

Suggested rules OFF:
- session structure rules (unless engine forces)
- VIX regime filter (unless explicitly required)
- liquidity filters (optional; if enabled, log as execution-quality)
- re-entry privileges beyond base
- averaging down (DISALLOW by default)

Purpose: isolate whether edge comes from survival controls only.

### G3 — Session + survival gate (time structure)
All G2 rules PLUS enforce intraday structure (ET):
- no entries before **09:45**
- no new entries after **15:00**
- force-flat by **15:30**

Purpose: isolate whether time-of-day structure is the main risk control.

### G4 — Adaptive “second chance” gate (controlled re-entry + controlled adds)
“Less rigid but disciplined” (bounded flexibility).  
Base: G3 plus:

Re-entry (max 1/day) only if ALL true:
- cooldown ≥ 30 minutes
- same-direction trend filter still valid
- VIX regime allowed
- prior loss was NOT catastrophic (define threshold)

Averaging down:
- allowed only under strict criteria (no discretion)
  1) max 1 add (no pyramiding)
  2) total exposure after add ≤ 1.25× original size
  3) add only at predefined level (e.g., VWAP retest / ATR level)
  4) stop recalculated so total risk ≤ per-trade cap

Purpose: test conditional/bounded adds + disciplined re-entry.

### G5 — Strict tail-risk gate (drawdown discipline)
Stricter than G1 to reduce blowups / tails.

Suggested rules:
- block trading in HIGH VIX regime (trade only LOW/NORMAL)
- tighten max consecutive losses
- tighten max trades/day (≤ 3)
- optionally tighten per-trade risk cap

Purpose: see if “tight survival” improves expectancy by cutting tail losses.

---

## 3) Canonical denial reason codes (required)

These codes must be logged for each denied signal in `gate_decisions`.

**Risk / sizing / survival**
- `RISK_DAILY_KILL`
- `RISK_PER_TRADE_CAP`
- `RISK_OVERSIZE`
- `RISK_MAX_TRADES`
- `RISK_LOSS_STREAK`
- `RISK_COOLDOWN`
- `RISK_MAX_OPEN_POS`

**Session structure**
- `SESSION_PRE_945`
- `SESSION_POST_300`
- `SESSION_FORCE_FLAT`

**Volatility / liquidity**
- `VOL_VIX_BLOCK`
- `LIQ_SPREAD`
- `LIQ_VOLUME`
- `LIQ_OPEN_INTEREST`

**Adds / re-entry / overnight**
- `ADD_TO_LOSER_BLOCK`
- `AVG_DOWN_BLOCK`
- `REENTRY_BLOCK`
- `OVERNIGHT_BLOCK`

**Misc / engine**
- `ENGINE_CONSTRAINT`

---

## 4) Required outputs (per run_id, every time)

A) Gate comparison table (G0–G5):  
`trade_count, win_rate, PF, expectancy, maxDD, worst_day, worst_trade, avg_hold_time, % zero-trade days`

B) Denied-but-profitable stats (G1–G5):  
`denied_profitable_count`, `denied_profitable_rate`

C) With-gate vs shadow-all:  
ending equity, maxDD, worst day, tail reduction deltas

D) Lift vs drag rules:  
which rules help vs hurt and why (tie to evidence/metrics)

---

## 5) Risk gate “confusion matrix” (trading context)

For each gate (G1–G5):
- PASS trades executed (official)
- FAIL trades shadowed
- denied-but-profitable count/rate
- denied-but-catastrophic-avoided proxy (compare worst 1% outcomes in PASS vs FAIL)

---

## 6) SQLite evidence pointers (minimum tables)

- `runs(run_id, symbol, timeframe, date_start, date_end, holding_mode, strategy_version)`
- `signals(run_id, signal_id, ts, direction, features_json)`
- `trades_shadow(run_id, signal_id, entry_ts, exit_ts, pnl, mae, mfe, hold_minutes, exit_reason)`
- `gate_decisions(run_id, gate_id, signal_id, decision, denial_code, denial_detail, equity_at_decision, risk_at_decision, ts)`
- `trades_pass(run_id, gate_id, trade_id, signal_id, entry_ts, exit_ts, pnl, mae, mfe, hold_minutes, exit_reason)`
- `gate_daily_stats(run_id, gate_id, session_date, trades_taken, pnl_day, dd_day, kill_switch_hit)`
- `gate_metrics(run_id, gate_id, trade_count, win_rate, pf, expectancy, maxdd, worst_day, worst_trade, avg_hold, zero_trade_day_pct)`

---

## 7) Recommendation rule (non-negotiable)

Recommend a “winner gate” only after adequate sample size.

Draft requirement (override only by Cole):
- **≥ 60 trading days AND ≥ 200 trades** (or explicitly adjusted)