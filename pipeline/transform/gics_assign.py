"""Point-in-time GICS sector classification for each stock-week."""
import pandas as pd
import numpy as np

from pipeline import config


def run() -> None:
    """
    Assign GICS codes to each stock-week using point-in-time lookup.

    If comp.co_hgic (historical GICS) is available, use that.
    Otherwise, fall back to comp.company (current/static) codes.
    """
    print("\nAssigning point-in-time GICS codes...")

    weekly_mktcap = pd.read_parquet(config.PROCESSED_DIR / "weekly_mktcap.parquet")
    company = pd.read_parquet(config.RAW_DIR / "compustat_company.parquet")
    ccm_links = pd.read_parquet(config.RAW_DIR / "ccm_links.parquet")

    # Try to load historical GICS
    hgics_file = config.RAW_DIR / "compustat_hgics.parquet"
    has_hgics = hgics_file.exists()

    if has_hgics:
        print("  Using comp.co_hgic for historical GICS assignments...")
        hgics = pd.read_parquet(hgics_file)
        gics_table = hgics[
            ["gvkey", "indfrom", "indthru", "gsector", "ggroup", "gind", "gsubind"]
        ]
        gics_table = gics_table.rename(columns={"indfrom": "effective_date"})

        # Handle NULL indthru (still-active classifications)
        gics_table["indthru"] = gics_table["indthru"].fillna(pd.Timestamp.max)
    else:
        print("  comp.co_hgic not available; using static GICS from comp.company...")
        # Create a static GICS table with all dates valid
        gics_table = company[["gvkey", "gsector", "ggroup", "gind", "gsubind"]].copy()
        gics_table["effective_date"] = pd.Timestamp.min
        gics_table["indthru"] = pd.Timestamp.max

    # Link permno to gvkey
    weekly_mktcap = weekly_mktcap.merge(
        ccm_links[["permno", "gvkey", "linkdt", "linkenddt"]],
        on="permno",
        how="left"
    )

    # Filter to valid link periods
    weekly_mktcap["linkenddt"] = weekly_mktcap["linkenddt"].fillna(pd.Timestamp.now())
    valid_links = (
        (weekly_mktcap["week_end"] >= weekly_mktcap["linkdt"]) &
        (weekly_mktcap["week_end"] <= weekly_mktcap["linkenddt"])
    )
    weekly_mktcap = weekly_mktcap[valid_links].reset_index(drop=True)

    # Now do point-in-time join with GICS
    # For each (permno, gvkey, week_end), find the GICS code where:
    # week_end >= effective_date AND week_end <= indthru
    merged = weekly_mktcap.merge(
        gics_table, on="gvkey", how="left"
    )

    # Filter to valid GICS periods
    valid_gics = (
        (merged["week_end"] >= merged["effective_date"]) &
        (merged["week_end"] <= merged["indthru"])
    )
    merged = merged[valid_gics].reset_index(drop=True)

    # Drop temporary columns
    cols_to_drop = ["linkdt", "linkenddt", "effective_date", "indthru"]
    merged = merged.drop(columns=[c for c in cols_to_drop if c in merged.columns])

    # Ensure GICS codes are integers/strings consistently
    for col in ["gsector", "ggroup", "gind", "gsubind"]:
        if col in merged.columns:
            merged[col] = merged[col].astype(str)

    # Save
    output_file = config.PROCESSED_DIR / "weekly_with_gics.parquet"
    merged.to_parquet(output_file, engine="pyarrow", compression="snappy")

    print(f"✓ Weekly data with GICS: {len(merged):,} rows")
    print(f"  Unique permno: {merged['permno'].nunique()}")
    print(f"  Unique gvkey: {merged['gvkey'].nunique()}")
    print(f"  Saved to {output_file.name}")
