"""CLI entry point for the PE industry predictability pipeline."""
import typer
from pathlib import Path

from pipeline import config, auth
from pipeline.extract import checkpoint, crsp, compustat, ccm_link
from pipeline.transform import link_merge, ttm_eps, weekly_resample, gics_assign
from pipeline.compute import sector_pe, sector_returns

app = typer.Typer()


@app.command()
def extract(force: bool = typer.Option(False, "--force", help="Re-pull even if complete")) -> None:
    """
    Pull raw data from WRDS and save to data/raw/.

    Skips tables that have already been pulled (unless --force).
    CRSP is pulled year-by-year for resume capability.
    """
    print("=" * 60)
    print("STEP 1: EXTRACT RAW DATA FROM WRDS")
    print("=" * 60)

    # Get WRDS connection
    conn = auth.get_connection()

    # Initialize checkpoint manager
    ckpt = checkpoint.Checkpoint(config.RAW_DIR)

    # Extract CRSP
    try:
        crsp_ext = crsp.CRSPExtractor(conn, ckpt)
        crsp_ext.run(force=force)
    except Exception as e:
        print(f"✗ CRSP extraction failed: {e}")
        raise

    # Extract Compustat
    try:
        comp_ext = compustat.CompustatExtractor(conn, ckpt)
        comp_ext.run(force=force)
    except Exception as e:
        print(f"✗ Compustat extraction failed: {e}")
        raise

    # Extract CCM links
    try:
        ccm_ext = ccm_link.CCMLinkExtractor(conn, ckpt)
        ccm_ext.run(force=force)
    except Exception as e:
        print(f"✗ CCM link extraction failed: {e}")
        raise

    print("\n✓ Extraction complete")
    ckpt.print_status()


@app.command()
def transform() -> None:
    """
    Transform raw data into processed datasets.

    Performs linking, TTM EPS calculation, weekly resampling, and GICS assignment.
    Requires extracted raw data; does not need WRDS access.
    """
    print("=" * 60)
    print("STEP 2: TRANSFORM RAW → PROCESSED")
    print("=" * 60)

    try:
        link_merge.run()
        ttm_eps.run()
        weekly_resample.run()
        gics_assign.run()
    except Exception as e:
        print(f"✗ Transform failed: {e}")
        raise

    print("\n✓ Transform complete")


@app.command()
def compute() -> None:
    """
    Compute sector-level PE and returns.

    Aggregates from processed data to produce final output:
    - sector_pe_weekly.parquet
    - sector_returns_weekly.parquet
    """
    print("=" * 60)
    print("STEP 3: COMPUTE SECTOR PE AND RETURNS")
    print("=" * 60)

    try:
        sector_pe.run()
        sector_returns.run()
    except Exception as e:
        print(f"✗ Compute failed: {e}")
        raise

    print("\n✓ Compute complete")


@app.command()
def run(force: bool = typer.Option(False, "--force", help="Re-pull WRDS data")) -> None:
    """
    Full pipeline: extract → transform → compute.

    Pulls all data from WRDS, processes it, and computes final sector PE/returns.
    """
    print("\n" + "=" * 60)
    print("FULL PIPELINE: EXTRACT → TRANSFORM → COMPUTE")
    print("=" * 60 + "\n")

    try:
        extract(force=force)
        print("\n")
        transform()
        print("\n")
        compute()
        print("\n" + "=" * 60)
        print("✓ PIPELINE COMPLETE")
        print("=" * 60)
        print("\nOutput files:")
        print(f"  - {config.OUTPUT_DIR / 'sector_pe_weekly.parquet'}")
        print(f"  - {config.OUTPUT_DIR / 'sector_returns_weekly.parquet'}")
    except Exception as e:
        print(f"\n✗ Pipeline failed: {e}")
        raise


@app.command()
def status() -> None:
    """Show pipeline status: what's been pulled, row counts, date ranges."""
    print("=" * 60)
    print("PIPELINE STATUS")
    print("=" * 60)

    ckpt = checkpoint.Checkpoint(config.RAW_DIR)
    print("\nRaw data status:")
    ckpt.print_status()

    print("\nProcessed data:")
    for f in sorted(config.PROCESSED_DIR.glob("*.parquet")):
        import pandas as pd
        df = pd.read_parquet(f)
        date_cols = [c for c in df.columns if "date" in c.lower() or c == "week_end"]
        if date_cols:
            date_range = f"({df[date_cols[0]].min()} to {df[date_cols[0]].max()})"
        else:
            date_range = ""
        print(f"  {f.name}: {len(df):,} rows {date_range}")

    print("\nOutput data:")
    for f in sorted(config.OUTPUT_DIR.glob("*.parquet")):
        import pandas as pd
        df = pd.read_parquet(f)
        date_cols = [c for c in df.columns if "date" in c.lower() or c == "week_end"]
        if date_cols:
            date_range = f"({df[date_cols[0]].min()} to {df[date_cols[0]].max()})"
        else:
            date_range = ""
        print(f"  {f.name}: {len(df):,} rows {date_range}")


if __name__ == "__main__":
    app()
