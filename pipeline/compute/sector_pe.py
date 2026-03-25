"""Compute aggregate sector PE ratios from individual company data."""
import pandas as pd
import numpy as np

from pipeline import config


def run() -> None:
    """
    Compute aggregate sector PE ratios for all GICS levels.

    Sector PE = sum(market_cap) / sum(earnings)
    where earnings = TTM_EPS * shares_outstanding
    and market_cap = price * shares_outstanding

    Filters to companies with positive earnings to avoid distortions.
    """
    print("\nComputing aggregate sector PE ratios...")

    # Load weekly data with GICS codes
    weekly_gics = pd.read_parquet(config.PROCESSED_DIR / "weekly_with_gics.parquet")

    # Load TTM EPS
    ttm_eps = pd.read_parquet(config.PROCESSED_DIR / "ttm_eps.parquet")

    # Merge weekly CRSP with TTM EPS using an as-of join
    # For each (gvkey, week_end), find the most recent TTM EPS where known_date <= week_end
    weekly_gics = weekly_gics.sort_values(["gvkey", "week_end"])
    ttm_eps = ttm_eps.sort_values(["gvkey", "known_date"])

    # Use pandas merge_asof (backward direction: use most recent EPS before or on week_end)
    merged = pd.merge_asof(
        weekly_gics,
        ttm_eps[["gvkey", "known_date", "ttm_eps", "cshoq"]],
        left_on=["gvkey", "week_end"],
        right_on=["gvkey", "known_date"],
        direction="backward",
        tolerance=pd.Timedelta(days=config.MAX_EPS_STALENESS_DAYS)
    )

    # Compute per-company mktcap and earnings
    # mktcap = price * shares (both in CRSP units: price in $, shrout in thousands)
    merged["mktcap"] = merged["prc"] * merged["shrout"] * 1000  # Convert to $

    # earnings = TTM_EPS * shares
    # Note: We have two share counts: shrout (CRSP) and cshoq (Compustat)
    # For consistency, use cshoq (Compustat) if available, else shrout
    merged["shares_for_earnings"] = merged["cshoq"].fillna(merged["shrout"]) * 1000

    merged["earnings"] = merged["ttm_eps"] * merged["shares_for_earnings"]

    # Filter to rows with valid data
    valid_data = (
        (merged["prc"].notna()) &
        (merged["shrout"].notna()) &
        (merged["ttm_eps"].notna()) &
        (merged["earnings"] > 0)  # Only positive earners
    )
    merged = merged[valid_data].reset_index(drop=True)

    # Aggregate by (week_end, gics_level, gics_code)
    results = []

    for level_name, level_info in config.GICS_LEVELS.items():
        gics_col = level_info["col"]

        # Group by week_end and gics_code
        grouped = merged.groupby(["week_end", gics_col]).agg(
            total_mktcap=("mktcap", "sum"),
            total_earnings=("earnings", "sum"),
            median_pe=("prc", lambda x: np.median(x / merged.loc[x.index, "ttm_eps"])),
            n_positive_earners=("earnings", "count"),
        ).reset_index()

        # Get total sector mktcap (including negative earners) for coverage calculation
        sector_mktcap_all = merged.groupby(["week_end", gics_col])["mktcap"].sum()
        grouped = grouped.merge(
            sector_mktcap_all.reset_index().rename(columns={"mktcap": "total_mktcap_all"}),
            on=["week_end", gics_col],
            how="left"
        )

        grouped["aggregate_pe"] = grouped["total_mktcap"] / grouped["total_earnings"]
        grouped["mktcap_coverage_pct"] = (
            grouped["total_mktcap"] / grouped["total_mktcap_all"] * 100
        )
        grouped["total_mktcap_bn"] = grouped["total_mktcap"] / 1e9

        # Get total count of companies in sector
        n_all = merged.groupby(["week_end", gics_col]).size()
        grouped = grouped.merge(
            n_all.reset_index(name="n_total"),
            on=["week_end", gics_col],
            how="left"
        )

        grouped["positive_earner_pct"] = (
            grouped["n_positive_earners"] / grouped["n_total"] * 100
        )

        # Rename gics_col to gics_code and add level name
        grouped = grouped.rename(columns={gics_col: "gics_code"})
        grouped["gics_level"] = level_name

        results.append(grouped)

    # Combine all GICS levels
    output_df = pd.concat(results, ignore_index=True)

    # Select and order columns
    output_df = output_df[
        [
            "week_end",
            "gics_level",
            "gics_code",
            "aggregate_pe",
            "median_pe",
            "n_positive_earners",
            "n_total",
            "positive_earner_pct",
            "mktcap_coverage_pct",
            "total_mktcap_bn",
        ]
    ]

    # Save
    output_file = config.OUTPUT_DIR / "sector_pe_weekly.parquet"
    output_df.to_parquet(output_file, engine="pyarrow", compression="snappy")

    print(f"✓ Sector PE: {len(output_df):,} rows")
    print(f"  GICS levels: {output_df['gics_level'].unique()}")
    print(f"  Week range: {output_df['week_end'].min()} to {output_df['week_end'].max()}")
    print(f"  Saved to {output_file.name}")

    # Print sample
    print("\nSample sector PE (most recent week):")
    latest = output_df[output_df["gics_level"] == "sector"].sort_values("week_end").tail(3)
    for _, row in latest.iterrows():
        print(
            f"  {row['gics_code']}: PE={row['aggregate_pe']:.2f}x, "
            f"coverage={row['mktcap_coverage_pct']:.1f}%"
        )
