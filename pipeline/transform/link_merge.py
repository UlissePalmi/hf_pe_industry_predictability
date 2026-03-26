"""Merge CRSP and Compustat data via CCM linking table."""
import pandas as pd
from datetime import datetime

from pipeline import config


def run() -> None:
    """
    Merge CRSP daily data with Compustat fundamentals via CCM links.

    Creates a unified dataset linking permno (CRSP) to gvkey (Compustat)
    with valid date ranges for each link.
    """
    print("\nMerging CRSP, Compustat, and CCM links...")

    # Load raw data
    crsp_daily = pd.read_parquet(config.RAW_DIR / "crsp_daily.parquet")
    ccm_links = pd.read_parquet(config.RAW_DIR / "ccm_links.parquet")
    company = pd.read_parquet(config.RAW_DIR / "compustat_company.parquet")

    # Merge CRSP with CCM links via permno
    # For each date, find the valid link
    # linkenddt can be NaT (still-active links) — treat as today or beyond
    today = pd.Timestamp(datetime.now())

    merged = crsp_daily.merge(ccm_links, on="permno", how="left")

    # Filter to rows where the date falls within the link validity window
    # date >= linkdt AND date <= linkenddt (or today if linkenddt is NaT)
    merged["linkenddt_effective"] = merged["linkenddt"].fillna(today)
    valid_links = (
        (merged["date"] >= merged["linkdt"]) &
        (merged["date"] <= merged["linkenddt_effective"])
    )
    merged = merged[valid_links]

    # Drop helper column
    merged = merged.drop(columns=["linkenddt_effective", "linkdt", "linkenddt", "linktype", "linkprim"])

    # Add GICS codes from company table
    merged = merged.merge(
        company[["gvkey", "gsector", "ggroup", "gind", "gsubind"]],
        on="gvkey",
        how="left"
    )

    # Save
    output_file = config.PROCESSED_DIR / "linked_universe.parquet"
    merged.to_parquet(output_file, engine="pyarrow", compression="snappy")

    print(f"✓ Linked universe: {len(merged):,} rows")
    print(f"  Unique permno: {merged['permno'].nunique()}")
    print(f"  Unique gvkey: {merged['gvkey'].nunique()}")
    print(f"  Date range: {merged['date'].min()} to {merged['date'].max()}")
    print(f"  Saved to {output_file.name}")
