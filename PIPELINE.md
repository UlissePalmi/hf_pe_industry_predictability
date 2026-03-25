# Sector PE & Returns Data Pipeline

Historical US sector Price-to-Earnings (PE) ratios and market-cap weighted returns at all GICS levels, computed from WRDS.

## Quick Start

### 1. Install Dependencies

```bash
pip install -e .
```

### 2. (Optional) Set Up WRDS Credentials in `.env`

For convenience, create a `.env` file with your WRDS credentials:

```bash
# Copy the template
cp .env.example .env

# Edit .env and add your credentials
# .env is git-ignored and will never be committed
WRDS_USERNAME=your_username
WRDS_PASSWORD=your_password
```

If you skip this, the pipeline will prompt you for credentials when you run `extract`.

### 3. Pull Data from WRDS

```bash
python main.py extract
```

If you set up `.env`, it will load credentials automatically. Otherwise, you'll be prompted to enter your WRDS username and password. This step will:
- Pull CRSP daily stock prices (1963-present, ~80-100M rows)
- Pull Compustat quarterly EPS and company info (~3-5M rows)
- Pull CCM linking table (~500K rows)
- Save all raw data to `data/raw/` as parquet files
- Track progress in `data/raw/_checkpoint.json` (resumable if interrupted)

**Typical runtime:** 30-60 minutes depending on WRDS server load.

### 3. Transform Data

```bash
python main.py transform
```

This transforms raw WRDS data into analytical datasets:
- Links CRSP permno <-> Compustat gvkey via CCM
- Computes TTM (trailing twelve-month) EPS
- Resamples daily prices to weekly frequency
- Assigns point-in-time GICS sector classifications
- Outputs to `data/processed/`

**Typical runtime:** 5-10 minutes

### 4. Compute Sector PE and Returns

```bash
python main.py compute
```

Aggregates to sector-level:
- Computes aggregate PE = sum(market cap) / sum(earnings) per sector
- Computes market-cap weighted returns per sector
- All 4 GICS levels: sector, industry group, industry, sub-industry
- Outputs to `data/output/`

**Typical runtime:** 2-3 minutes

### Full Pipeline (One Command)

```bash
python main.py run
```

Runs extract -> transform -> compute end-to-end.

### Check Status

```bash
python main.py status
```

Shows row counts and date ranges of all raw, processed, and output data.

---

## Output Data

### `data/output/sector_pe_weekly.parquet`

Weekly aggregate PE ratios by GICS sector/industry.

Columns:
- `week_end`: Last trading day of ISO week
- `gics_level`: "sector", "industry_group", "industry", or "sub_industry"
- `gics_code`: GICS code (e.g., "10" for Energy)
- `aggregate_pe`: Sum(market cap) / Sum(earnings)
- `median_pe`: Median PE of constituent companies
- `n_positive_earners`: Count of companies with earnings > 0
- `n_total`: Total companies in sector
- `positive_earner_pct`: Percentage with earnings > 0
- `mktcap_coverage_pct`: Percentage of sector market cap included (data quality check)
- `total_mktcap_bn`: Sector market cap in billions USD

**Note:** Aggregate PE only includes companies with positive earnings. Check `mktcap_coverage_pct` — values >70% indicate good coverage.

### `data/output/sector_returns_weekly.parquet`

Market-cap weighted sector returns.

Columns:
- `week_end`: Last trading day of ISO week
- `gics_level`: GICS classification level
- `gics_code`: Sector/industry code
- `ret_weekly`: Market-cap weighted total return (including dividends)
- `log_ret_weekly`: Log return
- `cum_ret`: Cumulative return since 1963
- `n_constituents`: Number of stocks with valid returns
- `total_mktcap_bn`: Sector market cap in billions USD

---

## Key Design Decisions

### Look-Ahead Bias Prevention

The most common mistake in PE backtests is using quarter-end EPS as if it's "known" on the quarter-end date. In reality, earnings are announced 30-60 days later (Report Date, rdq). This pipeline uses rdq so TTM EPS only "exists" after the announcement.

### Negative Earners

Companies with negative TTM earnings are excluded from aggregate PE (standard across Bloomberg, Damodaran, etc.). This prevents loss-making companies from distorting sector PE. Track `mktcap_coverage_pct` to see what % of sector market cap is included.

### Weekly Frequency

- More granular than monthly but less noisy than daily
- Week-end = last trading day of ISO week (Friday, or Thursday if Friday is a holiday)
- Weekly returns are compounded from daily returns

### Point-in-Time GICS

The pipeline uses `comp.co_hgic` (historical GICS) if available in your subscription, capturing GICS revisions (e.g., Real Estate separated from Financials in Sep 2016). Falls back to static codes from `comp.company` otherwise.

### Market Cap Weighting

Sector returns use *lagged* market cap (prior week-end) to avoid look-ahead bias.

---

## Resumable Extraction

If CRSP pull is interrupted:

```bash
python main.py extract
```

Resumes from where it left off via `data/raw/_checkpoint.json`. To force re-pull:

```bash
python main.py extract --force
```

---

## Intermediate Files

Safe to delete and regenerate (stored in `data/processed/`):
- `linked_universe.parquet` — CRSP + Compustat merged
- `ttm_eps.parquet` — TTM EPS with announcement dates
- `weekly_mktcap.parquet` — Weekly price snapshots
- `weekly_with_gics.parquet` — Weekly + GICS codes

---

## Troubleshooting

### WRDS connection failed
- Verify username/password (case-sensitive)
- Confirm you have CRSP/Compustat access in your subscription

### comp.co_hgic not found
- This table may not be in your subscription
- Pipeline falls back to static GICS codes automatically

### Memory errors during transform
- CRSP daily file is large (~8-12 GB in RAM)
- Close other applications or process in smaller chunks

### Sector PE is 0 or NaN
- Check `mktcap_coverage_pct` — if <20%, all companies are loss-making that week
- Use `median_pe` as alternative (outlier-robust)

---

## WRDS Data Sources

| Table | Purpose |
|-------|---------|
| `crsp.dsf` | Daily prices, returns, shares (~80-100M rows) |
| `crsp.msenames` | Share class filters |
| `comp.fundq` | Quarterly EPS (~3-5M rows) |
| `comp.company` | Company info + current GICS (~50K rows) |
| `comp.co_hgic` | Historical GICS (optional) |
| `crsp.ccmxpf_lnkhist` | CRSP-Compustat links (~500K rows) |
