"""Compute market-cap weighted sector returns."""
import pandas as pd
import numpy as np

from pipeline import config


def run() -> None:
    """
    Compute market-cap weighted sector returns for all GICS levels.

    Uses lagged market cap weights (prior week-end) to avoid look-ahead bias.
    Handles missing returns (CRSP missing codes) by re-normalizing weights.
    """
    print("\nComputing market-cap weighted sector returns...")

    # Load weekly data with GICS codes
    weekly_gics = pd.read_parquet(config.PROCESSED_DIR / "weekly_with_gics.parquet")

    # Ensure week_end is sorted
    weekly_gics = weekly_gics.sort_values(["gvkey", "week_end"]).reset_index(drop=True)

    # Compute lagged mktcap for weight calculation
    weekly_gics["mktcap"] = weekly_gics["prc"] * weekly_gics["shrout"] * 1000
    weekly_gics["mktcap_lag"] = weekly_gics.groupby("permno")["mktcap"].shift(1)

    # Filter out rows with invalid returns
    # CRSP missing codes: -99, -77, -88, -66
    invalid_codes = {-99.0, -77.0, -88.0, -66.0, np.nan}
    weekly_gics = weekly_gics[~weekly_gics["ret_weekly"].isin(invalid_codes)]

    # Aggregate returns by (week_end, gics_level, gics_code)
    results = []

    for level_name, level_info in config.GICS_LEVELS.items():
        gics_col = level_info["col"]

        grouped = []
        for (week_end, gics_code), group in weekly_gics.groupby(["week_end", gics_col]):
            if len(group) == 0 or group["mktcap_lag"].sum() == 0:
                continue

            # Mktcap-weighted return using lagged weights
            weights = group["mktcap_lag"] / group["mktcap_lag"].sum()
            weighted_ret = (weights * group["ret_weekly"]).sum()

            # Log return
            log_ret = np.log(1 + weighted_ret) if weighted_ret > -1 else np.nan

            # Cumulative return since start (computed later)
            grouped.append(
                {
                    "week_end": week_end,
                    "gics_level": level_name,
                    "gics_code": gics_code,
                    "ret_weekly": weighted_ret,
                    "log_ret_weekly": log_ret,
                    "n_constituents": len(group),
                    "total_mktcap_bn": group["mktcap"].sum() / 1e9,
                }
            )

        if grouped:
            results.append(pd.DataFrame(grouped))

    # Combine all GICS levels
    output_df = pd.concat(results, ignore_index=True)
    output_df = output_df.sort_values(["gics_level", "gics_code", "week_end"])

    # Compute cumulative returns per sector
    output_df["cum_ret"] = (
        output_df.groupby(["gics_level", "gics_code"])["ret_weekly"]
        .apply(lambda x: (1 + x).cumprod() - 1)
        .values
    )

    # Select and order columns
    output_df = output_df[
        [
            "week_end",
            "gics_level",
            "gics_code",
            "ret_weekly",
            "log_ret_weekly",
            "cum_ret",
            "n_constituents",
            "total_mktcap_bn",
        ]
    ]

    # Save
    output_file = config.OUTPUT_DIR / "sector_returns_weekly.parquet"
    output_df.to_parquet(output_file, engine="pyarrow", compression="snappy")

    print(f"✓ Sector returns: {len(output_df):,} rows")
    print(f"  GICS levels: {output_df['gics_level'].unique()}")
    print(f"  Week range: {output_df['week_end'].min()} to {output_df['week_end'].max()}")
    print(f"  Saved to {output_file.name}")

    # Print sample
    print("\nSample sector returns (recent week):")
    latest = output_df[output_df["gics_level"] == "sector"].sort_values("week_end").tail(3)
    for _, row in latest.iterrows():
        print(
            f"  {row['gics_code']}: ret={row['ret_weekly']*100:.2f}%, "
            f"cum_ret={row['cum_ret']*100:.1f}%, "
            f"constituents={row['n_constituents']}"
        )
