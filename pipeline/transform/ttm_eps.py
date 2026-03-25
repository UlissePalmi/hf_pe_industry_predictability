"""Compute trailing twelve-month (TTM) EPS from quarterly fundamentals."""
import pandas as pd
import numpy as np

from pipeline import config


def run() -> None:
    """
    Compute TTM EPS from Compustat quarterly fundamentals.

    TTM = rolling sum of the last 4 quarters of EPS.
    Known date = the announcement date (rdq) when TTM becomes "public knowledge".

    Critical: Use rdq (report date) as known_date, NOT datadate (quarter end).
    This prevents look-ahead bias.
    """
    print("\nComputing TTM EPS from quarterly fundamentals...")

    fundq = pd.read_parquet(config.RAW_DIR / "compustat_fundq.parquet")

    # Sort by gvkey and datadate
    fundq = fundq.sort_values(["gvkey", "datadate"]).reset_index(drop=True)

    # Prefer diluted EPS (epspxq), fall back to basic (epsfxq)
    fundq["eps"] = fundq["epspxq"].fillna(fundq["epsfxq"])

    # Compute rolling 4-quarter sum for each gvkey
    fundq["ttm_eps"] = fundq.groupby("gvkey")["eps"].rolling(window=4, min_periods=4).sum().reset_index(drop=True)

    # Mark TTM as valid only if all 4 quarters are "recent" (within 15 months)
    # This prevents stale EPS from inflating the TTM
    fundq["quarter_diff"] = fundq.groupby("gvkey")["datadate"].diff().dt.days
    # For a valid TTM, we expect ~90 days between quarters, so 4 quarters ~ 270-360 days
    # Allow up to 450 days (15 months) to be conservative about quarter shifts
    fundq["ttm_valid"] = fundq["quarter_diff"].isna() | (fundq["quarter_diff"] <= 450)

    # Also mark a TTM as valid only after the 4th quarter is reported
    # Use rdq as the "known date" — this is the announcement date
    # For each gvkey, mark the first 3 rows (quarters 1-3) as not yet known
    fundq["is_4th_or_later"] = fundq.groupby("gvkey").cumcount() >= 3
    fundq["ttm_valid"] = fundq["ttm_valid"] & fundq["is_4th_or_later"]

    # The known date is the rdq of the most recent quarter in the TTM window
    # For rows with ttm_valid=True, the known_date is rdq of the current quarter
    # (the 4th quarter just reported)
    fundq["known_date"] = fundq["rdq"]

    # Apply fallback: if rdq is missing, use datadate + report_lag
    missing_rdq = fundq["known_date"].isna()
    fundq.loc[missing_rdq, "known_date"] = (
        fundq.loc[missing_rdq, "datadate"] +
        pd.Timedelta(days=config.REPORT_LAG_DAYS)
    )

    # Select output columns
    ttm_output = fundq[
        [
            "gvkey",
            "datadate",
            "fyearq",
            "fqtr",
            "ttm_eps",
            "known_date",
            "cshoq",
        ]
    ].copy()

    # Only keep rows where TTM is valid
    ttm_output = ttm_output[fundq["ttm_valid"]].reset_index(drop=True)

    # Save
    output_file = config.PROCESSED_DIR / "ttm_eps.parquet"
    ttm_output.to_parquet(output_file, engine="pyarrow", compression="snappy")

    print(f"✓ TTM EPS: {len(ttm_output):,} rows")
    print(f"  Unique gvkey: {ttm_output['gvkey'].nunique()}")
    print(f"  Known date range: {ttm_output['known_date'].min()} to {ttm_output['known_date'].max()}")
    print(f"  Saved to {output_file.name}")
