# Ingestion notes/docs for Multigate-backtester

## Purpose

This document serves as the canonical ingestion documentation for the Gulf Chain / Multigate Backtester environment. It details the architecture, workflow, and current status of the data ingestion system.

**Key Dates:**  
- Original ingestion workflow streamlined: February 9, 2026  
- Last updated: February 10, 2026  

### Ingestion source scripts (current)

These source scripts implement the ingestion logic for each vendor/source and are orchestrated by the dispatcher (`scripts/ingest.py`) using `ingest.yaml`:

- `src/ingest_databento.py`  
- `src/ingest_alpaca.py`  
- `src/ingest_vix.py`

## Goals of the Ingestion System

- Unified dispatcher via `scripts/ingest.py` to coordinate all ingestion tasks  
- Configuration-driven ingestion controlled by `ingest.yaml`  
- Consistent offline and API ingestion parity for all sources  
- Vendor-agnostic design allowing seamless integration of multiple data providers  
- Safety and cost guardrails to prevent excessive API usage and unexpected charges  
- Clear separation between raw data ingestion and processed/ingested outputs for deterministic workflows  

## What We Installed/Used

- `databento`  
  - [Databento Python API](https://github.com/databento/databento-python)  
  - [Databento API Reference](https://databento.com/docs/api-reference-historical?historical=python&live=python&reference=python)  
- `python-dotenv`  
  - [python-dotenv GitHub](https://github.com/theskumar/python-dotenv)  
- `alpaca-py`  
  - [Alpaca Python SDK](https://github.com/alpacahq/alpaca-py?tab=readme-ov-file#installation)  
- `PyYAML` (for parsing `ingest.yaml`)  
- `pandas` and `pyarrow` (for Parquet file handling)  
- `tabulate` (optional, for formatted table output in logs)  

## What We Created

- `scripts/ingest.py` — the authoritative dispatcher coordinating ingestion workflows  
- `ingest.yaml` — the single source of truth configuration file for ingestion parameters and sources  
- Standardized directory layout separating raw data from ingested Parquet outputs  
- Source metadata `.source.json` artifacts capturing ingestion source details and provenance  

## Configuring `ingest.yaml` from the example template

This repo uses a single ingestion config file: `ingest.yaml` (repo root). To get started, copy the example template and then edit the fields you care about.

### 1) Copy the template

- If your template file is named `example.ingest.yaml`:
  - Copy `example.ingest.yaml` → `ingest.yaml`

(If you’re using a different template naming convention, the rule is the same: copy the example file into `ingest.yaml` at the repo root.)

### 2) Enable only what you want to run

Each source vendor has:

- `enabled: true/false` — the master on/off switch (most authoritative)
- `mode: offline|api` — the default mode for that source
- `jobs:` — a list of specific ingestion jobs you can run for that source

The dispatcher will skip anything where:

- the source is disabled (`enabled: false`), or
- a job’s `mode` doesn’t match the source `mode`

### 3) Fill in a job (offline vs API)

- **Offline jobs** usually point at a local `raw_dir` (or a local Parquet path, depending on the source) and are used to convert/standardize local files into the ingested Parquet layout.
- **API jobs** define the query parameters (symbol(s), timeframe/schema, date range, etc.) and will typically:
  1) download vendor-native raw data into `data/raw/...`
  2) convert to Parquet into `data/ingested/...`

### 4) Use the cost safety rails for paid APIs

For paid / credit-based APIs (ex: Databento), keep these enabled on API jobs:

- `print_cost: true` — prints the vendor-estimated cost before download
- `require_confirm: true` — forces you to type `y` before the download proceeds
- `max_cost_usd: <float>` — hard cap; the job aborts if estimated cost exceeds this value

### 5) Run it

From the repo root:

- `python -m scripts.ingest`

That command reads `ingest.yaml` and runs only the enabled sources/jobs that match the selected mode.

## Workflow

1. Load environment variables from `.env` files  
2. Parse `ingest.yaml` configuration to identify enabled data sources and parameters  
3. Iterate over each enabled source in the configuration  
4. Enforce compatibility between source-specific `mode` and the overall job `mode`  
5. Route ingestion tasks to either offline file ingestion or API ingestion paths accordingly  
6. Write raw data files, convert to Parquet format, and write ingested outputs  
7. Emit detailed logs capturing progress, costs, and any warnings or errors  

## Plumbing / Streamlined Data Ingestion

- **Databento**  
  - Supports both offline file ingestion and live API ingestion  
  - Implements cost estimation and user confirmation before API calls  
  - Enforces `max_cost_usd` guardrails to prevent overspending  
- **Alpaca**  
  - Symbol-agnostic ingestion preserving requested timeframes  
  - Supports offline and API ingestion paths consistent with the unified workflow  
- **VIX**  
  - Aligned to the same interface and workflow as other sources for consistency and maintainability  

## Current Status

As of February 10, 2026, the ingestion system is feature-complete:  
- All vendor sources are fully wired into the unified dispatcher  
- Safety and cost guardrails are operational and enforced  
- The system is ready to support downstream feature engineering and backtesting workflows  
