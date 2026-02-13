# Strategy_001 — SPY VWAP Reclaim (Intraday Pullback + Reclaim)

**Status:** Draft (Spec)  
**Owner:** Cole  
**Primary Desk:** Securities & Indexes (SPY)  
**Distribution Target:** Signals (GulfSync) — *not* auto-execution  
**Last Updated:** 2026-02-11

---

## 0) Purpose and Professional Standard

This strategy is designed to be:
- **Backtest-first**
- **Rules-first**
- **Defensible**: *No signal goes out that I couldn’t defend in a room of professionals.*
- **Governed** by Quality Gate:
  - **Signal Gate:** selective; allowed to produce zero trades
  - **Risk Gate:** survival; prevents known failure modes (overtrading, oversizing, holding losers, averaging down)

---

## 1) Strategy Summary (Plain English)

We look for SPY to trend, pull back, and then **reclaim VWAP** with confirmation.  
The reclaim is treated as a “regain of control” moment where we can define risk tightly.

This is intentionally **intraday**, “A+ setup only,” and should be **regime-aware** (VIX/macro filter optional but recommended).

---

## 2) Instruments & Timeframes

### Instruments (MVP)
- **Underlying:** SPY (primary)
- **Optional (later):** ES (e-mini S&P) parity version

### Timeframes
- **Primary signal timeframe:** 5-minute bars
- **Optional confirmation timeframe:** 1-minute (for later refinement)
- **Trading hours:** Regular Trading Hours (RTH), 9:30–16:00 ET

---

## 3) Definitions

### VWAP
- Session VWAP for SPY calculated from intraday bars (RTH session).
- Must be consistent in backtest + live signal engine.

### Reclaim
A “reclaim” occurs when price was below VWAP, then:
- closes above VWAP, and
- shows continuation intent (see Entry Trigger rules)

### Pullback
A pullback is a counter-move against the prevailing direction, typically:
- price rotates back toward VWAP (or slightly through it),
- then rejects and moves back with momentum.

---

## 4) Preconditions (Signal Gate — Setup Qualification)

A signal is eligible only if ALL are true:

### 4.1 Trend Context (choose one for MVP, expand later)
**Option A (MVP):** Intraday bias via VWAP slope + structure
- VWAP slope up for longs / down for shorts (measured over last N bars)
- Price structure shows higher highs/higher lows (long) or lower lows/lower highs (short) on the 5m.

**Option B (later):** Higher timeframe alignment (15m/1h)

### 4.2 VWAP Interaction
- Price must have **meaningfully interacted** with VWAP (not just hovering).
- Must have been **on the “wrong side”** before reclaim:
  - For long: price below VWAP before reclaim
  - For short: price above VWAP before reclaim

### 4.3 Time Filter (optional but recommended)
- Avoid first 5 minutes (opening chaos)
- Avoid last 10 minutes (liquidity/closing distortions)
- Prefer “clean” windows: 9:40–11:30 and 13:00–15:30 ET (tunable)

### 4.4 Volatility / Regime Filter (optional)
- VIX regime tag from your pipeline (e.g., low/medium/high)
- Strategy may be enabled only for certain regimes (initially: “medium”)

---

## 5) Entry Triggers (Execution Rules)

### 5.1 Long Entry (VWAP Reclaim Long)
Trigger occurs when:
1) Price was below VWAP within last M bars
2) A 5m candle **closes above VWAP**
3) Next candle shows confirmation:
   - either a second close above VWAP, OR
   - a break above the reclaim candle high

**Entry price (MVP):**
- Enter at break of reclaim candle high (stop order logic), or
- Enter at next bar open if confirmation condition is already met

### 5.2 Short Entry (VWAP Reclaim Short)
Mirror logic:
1) Price was above VWAP within last M bars
2) A 5m candle closes **below VWAP**
3) Confirmation:
   - second close below VWAP OR
   - break below reclaim candle low

---

## 6) Risk Definition (Risk Gate)

### 6.1 Stop Loss (Hard)
Pick ONE for MVP:

**Stop Option A (simple):**
- Long: stop below reclaim candle low
- Short: stop above reclaim candle high

**Stop Option B (structure):**
- stop beyond recent swing low/high (last K bars)

### 6.2 Position Sizing
Size must be computed from max loss per trade:
- `risk_per_trade = account_risk_pct * account_equity`
- `position_size = risk_per_trade / (entry_price - stop_price)`

(For signals, you can publish “risk units” rather than dollar sizing.)

### 6.3 Max Trades / Day (Overtrading guardrail)
- Max 1 trade per direction per session (MVP), or
- Max 2 total trades/day with cooldown

### 6.4 No Averaging Down
- Explicitly forbidden.
- No adding to losers.
- No “re-enter immediately” unless new setup qualifies again.

### 6.5 Daily Loss Limit (Survival Rule)
- Stop issuing signals after daily drawdown reaches threshold.

---

## 7) Exits (Profit Taking + Failure Exits)

### 7.1 Primary Take Profit (MVP)
Pick ONE for MVP:

**TP Option A: R multiple**
- Take partial at +1R, trail remainder

**TP Option B: VWAP extension**
- Exit near prior day levels / intraday resistance

### 7.2 Trailing Stop (simple)
- Trail under prior 5m swing lows (long) / highs (short), OR
- Trail using an ATR multiple

### 7.3 Failure Exit (Fast invalidation)
Exit early if:
- price closes back through VWAP against position after entry, OR
- reclaim fails within N bars (no follow-through)

---

## 8) Signal Format (What the user sees)

**Signal ID:** STRAT_001  
**Fields:**
- Timestamp (ET)
- Direction (LONG/SHORT)
- Entry trigger (what event fired)
- Entry price
- Stop price (hard)
- Target plan (e.g., TP1=1R, trail rules)
- Setup context: VWAP slope, regime tag, time window
- Risk note: “No averaging down. Max 1 trade/day.”

Example:
> STRAT_001 | SPY | LONG  
> Reclaim VWAP + confirm break  
> Entry: 501.25  Stop: 500.60  TP1: 1R  Trail: swings  
> Context: VWAP slope up, Regime=MED, Window=AM  
> Governance: Max 2 trades/day, no adds, stop signals if daily loss limit hit.

---

## 9) Evidence Requirements (Quality Gate)

No signal is publishable unless:
- Backtest exists for:
  - chosen entry + stop + exit rules
  - the same RTH hours
  - the same VWAP calculation method
- Metrics meet minimum thresholds:
  - sample size minimum (N trades)
  - max drawdown within limit
  - distribution sanity (not 1 lucky outlier month)
- A “known failure mode” review has been run:
  - does it overtrade?
  - does it blow up in high volatility?
  - does it rely on unrealistic fills?

---

## 10) Strategy Lifecycle (Governance)

States:
1) **Research** — hypothesis + rules drafted
2) **Shadow** — live observation, no signals
3) **Paper** — signals issued privately, tracked
4) **Limited** — small audience / limited frequency
5) **Active** — full publish allowed
6) **Quarantine/Retire** — removed due to decay or risk

Transition rules are controlled by performance + risk metrics.

---

## 11) Implementation Notes (Repo Integration)

### MultiGate (Backtest)
- Inputs:
  - SPY 5m bars (RTH)
  - VWAP feature series
  - optional VIX regime label
- Outputs:
  - trade list + metrics table
  - evidence bundle for each signal type

### GulfSync (Signals + Governance)
- Use the strategy ID and publish format.
- Enforce Risk Gate flags:
  - daily loss limit
  - max trades/day
  - quarantine state blocks publishing

---

## 12) Open Questions (To Decide Before First Backtest Run)
- Confirmation choice: second close vs break of reclaim candle high/low (or both?)
- Stop choice: reclaim candle vs swing structure
- Exit choice: +1R partial vs trailing vs time-based
- Regime filter: on/off for MVP?

---

## 13) Next Actions (MVP)
1) Lock Entry/Stop/Exit variants for Backtest v0
2) Backtest on a multi-month window (at least 6–12 months if available)
3) Review failure modes + adjust filters
4) Define publish thresholds (minimum metrics)
5) Implement signal formatter in GulfSync

---