# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**hf_pe_industry_predictability** is a data pipeline that computes historical US sector Price-to-Earnings (PE) ratios and market-cap weighted returns at all GICS hierarchy levels (sector, industry group, industry, sub-industry) from WRDS data.

The pipeline is structured as three sequential stages that can run independently:
1. **Extract** — pull raw data from WRDS (CRSP, Compustat, CCM links)
2. **Transform** — process and link raw data into analytical datasets
3. **Compute** — aggregate to sector-level PE and returns

## Development Commands

### Setup
```bash
uv sync              # Install all dependencies (uses uv instead of pip)
```

### Running the Pipeline
```bash
python main.py extract              # Pull raw data from WRDS (~30-60 min)
python main.py extract --force       # Force re-pull all data
python main.py transform             # Transform to processed datasets (~5-10 min)
python main.py compute               # Aggregate to sector-level (~2-3 min)
python main.py run                   # Run full pipeline end-to-end
python main.py status                # Show pipeline status and row counts
```

### Development (no tests/linting yet)
```bash
python main.py <command>            # All development runs through main.py CLI
```

## Project Architecture

### Directory Structure
```
pipeline/
├── config.py                 # All paths, constants, WRDS table names, GICS definitions
├── auth.py                   # WRDS connection management (loads from .env)
├── extract/
│   ├── crsp.py              # Pull CRSP daily stock prices (1963-present)
│   ├── compustat.py         # Pull Compustat quarterly fundamentals
│   ├── ccm_link.py          # Pull CRSP-Compustat linking table
│   └── checkpoint.py        # Resumable extraction state (_checkpoint.json)
├── transform/
│   ├── link_merge.py        # Merge CRSP ↔ Compustat via CCM links
│   ├── ttm_eps.py           # Compute trailing-twelve-month EPS (announcement-date aware)
│   ├── weekly_resample.py   # Resample daily prices to weekly frequency
│   └── gics_assign.py       # Assign point-in-time GICS sector classifications
└── compute/
    ├── sector_pe.py         # Compute aggregate PE = Σ(mktcap) / Σ(earnings)
    └── sector_returns.py    # Compute market-cap weighted weekly returns

main.py                      # Typer CLI entry point; defines extract/transform/compute/run/status commands

data/
├── raw/                      # Output of extract stage (raw WRDS tables as parquet)
│   └── _checkpoint.json      # Tracks extraction progress (resumable on interrupt)
├── processed/                # Output of transform stage (linked, TTM, resampled, GICS-tagged)
└── output/                   # Output of compute stage (final sector PE and returns)
```

### Key Data Files
- **sector_pe_weekly.parquet** — weekly aggregate PE by GICS level; includes `mktcap_coverage_pct` (data quality indicator)
- **sector_returns_weekly.parquet** — market-cap weighted sector returns by GICS level
- **_checkpoint.json** — tracks which CRSP years have been extracted; allows resumable extraction

### Critical Configuration
See `pipeline/config.py` for:
- **GICS_LEVELS** — 4 hierarchy levels (sector, industry_group, industry, sub_industry)
- **VALID_SHRCDS, VALID_EXCHCDS** — filters for common US stock exchanges
- **VALID_LINKTYPES, VALID_LINKPRIMS** — CCM link quality filters
- **MIN_QUARTERS_FOR_TTM = 4** — only compute PE if ≥4 quarters of earnings exist
- **MAX_EPS_STALENESS_DAYS = 730** — don't use earnings older than 2 years
- **REPORT_LAG_DAYS = 45** — fallback lag if report date (rdq) is missing from Compustat

## Key Design Decisions (Affects Code Logic)

### 1. Look-Ahead Bias Prevention
**Most important:** The pipeline uses Compustat's `rdq` (Report Date — earnings announcement date), not the quarter-end date. TTM EPS only "exists" after the announcement. This prevents backtests from using earnings data on the day it's announced as if known on the quarter-end.
- See `transform/ttm_eps.py` for announcement-date filtering logic
- If `rdq` is missing, falls back to quarter-end + `REPORT_LAG_DAYS` (45 days conservative)

### 2. Negative Earners Excluded
Companies with negative TTM earnings are excluded from aggregate PE (standard across Bloomberg, Damodaran, etc.). This prevents loss-making companies from distorting sector PE ratios.
- See `compute/sector_pe.py` — filters for `ttm_eps > 0`
- Track `mktcap_coverage_pct` in output to see what % of sector market cap is positive-earning

### 3. Weekly Frequency (Not Daily or Monthly)
- Weekly: Granular without daily noise
- Week-end = last trading day of ISO week (Friday, or Thursday if Friday is a holiday)
- Sector returns are compounded from daily returns; prices are week-end snapshots

### 4. Point-in-Time GICS
Pipeline uses `comp.co_hgic` (historical GICS table) to capture GICS revisions (e.g., Real Estate separated from Financials in Sep 2016). Falls back to static codes from `comp.company` if `co_hgic` not in subscription.
- See `transform/gics_assign.py`

### 5. Lagged Market Cap for Returns
Sector returns use *previous week-end* market cap for weights to avoid look-ahead bias.

## WRDS Data Sources

| WRDS Table | Rows | Purpose |
|------------|------|---------|
| `crsp.dsf` | ~80-100M | Daily prices, returns, shares outstanding (1963-present) |
| `crsp.msenames` | varies | Share class metadata filters |
| `comp.fundq` | ~3-5M | Quarterly fundamental data (EPS, book value, etc.) |
| `comp.company` | ~50K | Company master (current GICS codes, CIK, etc.) |
| `comp.co_hgic` | varies | Historical GICS (optional; used if available) |
| `crsp.ccmxpf_lnkhist` | ~500K | CRSP permno ↔ Compustat gvkey links |

## Authentication

WRDS credentials are loaded from `.env` file:
```
WRDS_USERNAME=your_username
WRDS_PASSWORD=your_password
```

See `pipeline/auth.py`. If `.env` doesn't exist or credentials are missing, the pipeline prompts interactively on first run.

## Resumable Extraction

CRSP is pulled year-by-year for resume capability:
- State is tracked in `data/raw/_checkpoint.json`
- If extraction is interrupted, run `python main.py extract` again to resume
- To force re-pull all data: `python main.py extract --force`

See `pipeline/extract/checkpoint.py` for state management.

## Data Flow

```
WRDS (crsp.dsf, comp.fundq, ccm links)
    ↓ extract/
    ├── crsp.py          → data/raw/crsp_daily.parquet
    ├── compustat.py     → data/raw/compustat_fundq.parquet
    └── ccm_link.py      → data/raw/ccm_link.parquet

    ↓ transform/
    ├── link_merge.py    → data/processed/linked_universe.parquet
    ├── ttm_eps.py       → data/processed/ttm_eps.parquet
    ├── weekly_resample.py → data/processed/weekly_mktcap.parquet
    └── gics_assign.py   → data/processed/weekly_with_gics.parquet

    ↓ compute/
    ├── sector_pe.py     → data/output/sector_pe_weekly.parquet
    └── sector_returns.py → data/output/sector_returns_weekly.parquet
```

## Intermediate Files

Safe to delete and regenerate (all in `data/processed/`):
- `linked_universe.parquet` — CRSP + Compustat merged via CCM
- `ttm_eps.parquet` — TTM EPS with announcement dates
- `weekly_mktcap.parquet` — Weekly price snapshots and market caps
- `weekly_with_gics.parquet` — Weekly data with GICS codes applied

These are built from `data/raw/` which should be preserved.

## Output Data Schema

### sector_pe_weekly.parquet
- `week_end` — Last trading day of ISO week
- `gics_level` — "sector", "industry_group", "industry", or "sub_industry"
- `gics_code` — GICS code (e.g., "10" for Energy)
- `aggregate_pe` — Sum(market cap) / Sum(earnings) [positive earners only]
- `median_pe` — Median PE of constituents [outlier-robust alternative]
- `n_positive_earners` — Stocks with earnings > 0
- `n_total` — Total stocks in sector
- `mktcap_coverage_pct` — % of sector market cap in positive-earning stocks [quality check]
- `total_mktcap_bn` — Sector market cap in billions USD

### sector_returns_weekly.parquet
- `week_end` — Last trading day of ISO week
- `gics_level`, `gics_code` — Sector hierarchy
- `ret_weekly` — Market-cap weighted total return (including dividends)
- `log_ret_weekly` — Log return
- `cum_ret` — Cumulative return since 1963
- `n_constituents` — Number of stocks with valid returns
- `total_mktcap_bn` — Sector market cap in billions USD

## Common Debugging

**WRDS connection failed:** Verify credentials are case-sensitive; confirm you have CRSP/Compustat access.

**comp.co_hgic not found:** Table may not be in your subscription; pipeline falls back to static GICS automatically.

**Memory errors during transform:** CRSP daily file is 8-12 GB in RAM; close other applications.

**Sector PE is 0 or NaN:** Check `mktcap_coverage_pct` — if <20%, all stocks that week are loss-making. Use `median_pe` as alternative.
