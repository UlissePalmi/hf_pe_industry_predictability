"""Resample CRSP daily data to weekly frequency."""
import pandas as pd
import numpy as np

from pipeline import config


def run() -> None:
    """
    Resample CRSP daily data to weekly frequency.

    Computes weekly returns and snapshots price/shares at week-end (last trading day).
    Week is defined as ISO week (Mon-Fri).
    """
    print("\nResampling CRSP daily data to weekly frequency...")

    crsp_daily = pd.read_parquet(config.RAW_DIR / "crsp_daily.parquet")

    # Ensure date is datetime
    crsp_daily["date"] = pd.to_datetime(crsp_daily["date"])

    # Add ISO week identifiers
    crsp_daily["week_end"] = crsp_daily["date"] + pd.to_timedelta(
        (4 - crsp_daily["date"].dt.dayofweek) % 7, unit="D"
    )
    # Adjust: ISO week ends on Friday, but if the day is Friday, week_end is same day
    # If day is Saturday/Sunday, move to next Friday
    # Current logic: 0=Mon (offset 4), 1=Tue (offset 3), ..., 4=Fri (offset 0), 5=Sat (offset 6), 6=Sun (offset 5)

    # Actually, let's use a cleaner approach: groupby ISO week
    crsp_daily["iso_year"] = crsp_daily["date"].dt.isocalendar().year
    crsp_daily["iso_week"] = crsp_daily["date"].dt.isocalendar().week
    crsp_daily["iso_day"] = crsp_daily["date"].dt.dayofweek  # 0=Mon, 6=Sun

    # Week end = Friday of that ISO week
    # For each (iso_year, iso_week), find the last (highest) trading day
    week_group = crsp_daily.groupby(["permno", "iso_year", "iso_week"])

    # Compute weekly return as compound return of daily returns
    # ret_weekly = prod(1 + ret_daily) - 1
    # But we need to handle missing/invalid returns (-99, -77, etc.)
    def compute_weekly_return(daily_rets):
        """Compute weekly return from daily returns, excluding invalid codes."""
        # CRSP missing return codes
        invalid_codes = {-99, -77, -88, -66}
        valid_rets = daily_rets[~daily_rets.isin(invalid_codes)].dropna()

        if len(valid_rets) == 0:
            return np.nan

        # Compound return
        return (1 + valid_rets).prod() - 1

    weekly_data = []

    for (permno, iso_year, iso_week), group in crsp_daily.groupby(
        ["permno", "iso_year", "iso_week"]
    ):
        if len(group) == 0:
            continue

        # Last trading day of week
        week_end_date = group["date"].max()

        # Last row of week (for price and shares snapshot)
        last_row = group.iloc[-1]

        # Weekly return
        weekly_ret = compute_weekly_return(group["ret"].values)

        weekly_data.append(
            {
                "permno": permno,
                "week_end": week_end_date,
                "prc": last_row["prc"],
                "shrout": last_row["shrout"],
                "ret_weekly": weekly_ret,
                "n_trading_days": len(group),
            }
        )

    weekly_df = pd.DataFrame(weekly_data)
    weekly_df = weekly_df.sort_values(["permno", "week_end"]).reset_index(drop=True)

    # Save
    output_file = config.PROCESSED_DIR / "weekly_mktcap.parquet"
    weekly_df.to_parquet(output_file, engine="pyarrow", compression="snappy")

    print(f"✓ Weekly data: {len(weekly_df):,} rows")
    print(f"  Unique permno: {weekly_df['permno'].nunique()}")
    print(f"  Week range: {weekly_df['week_end'].min()} to {weekly_df['week_end'].max()}")
    print(f"  Saved to {output_file.name}")
