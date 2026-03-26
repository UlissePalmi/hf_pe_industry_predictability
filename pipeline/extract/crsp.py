"""Extract CRSP daily stock data from WRDS."""
from datetime import datetime
from pathlib import Path

import pandas as pd
from rich.progress import track

from pipeline import config
from pipeline.extract.checkpoint import Checkpoint

try:
    import wrds
except ImportError:
    wrds = None  # type: ignore


class CRSPExtractor:
    """Pull CRSP daily stock file (dsf) from WRDS."""

    def __init__(self, conn: "wrds.Connection", checkpoint: Checkpoint):
        """
        Initialize CRSP extractor.

        Args:
            conn: WRDS Connection object.
            checkpoint: Checkpoint manager.
        """
        self.conn = conn
        self.checkpoint = checkpoint
        self.table_name = "crsp_daily"
        self.raw_dir = config.RAW_DIR

    def run(self, force: bool = False) -> None:
        """
        Pull CRSP daily data year by year.

        Args:
            force: If True, re-pull even if already complete.
        """
        if not force and self.checkpoint.is_complete(self.table_name):
            print(f"✓ {self.table_name} already complete, skipping.")
            return

        start_year = 1963
        end_year = datetime.now().year

        print(f"\nPulling {self.table_name} from {start_year} to {end_year}...")

        for year in track(range(start_year, end_year + 1), description="Years"):
            if not force and not self.checkpoint.needs_year(self.table_name, year):
                continue

            df = self._pull_year(year)
            if df is None or df.empty:
                continue

            # Save annual file
            annual_file = self.raw_dir / f"{self.table_name}_{year}.parquet"
            df.to_parquet(annual_file, engine="pyarrow", compression="snappy")

            # Update checkpoint
            self.checkpoint.mark_year_complete(
                self.table_name, year, row_count=len(df)
            )

        # Concatenate all annual files into one
        self._consolidate_annual_files()

    def _pull_year(self, year: int) -> pd.DataFrame:
        """
        Pull one year of CRSP daily data.

        Args:
            year: Calendar year (e.g., 1963).

        Returns:
            DataFrame with columns: permno, date, prc, shrout, ret, retx, vol
        """
        sql = f"""
        SELECT
            a.permno,
            a.dlycaldt AS date,
            ABS(a.dlyprc) AS prc,
            a.shrout,
            a.dlyret AS ret,
            a.dlyretx AS retx,
            a.dlyvol AS vol
        FROM crsp.dsf_v2 AS a
        INNER JOIN crsp.stksecurityinfohist AS b
            ON a.permno = b.permno
            AND a.dlycaldt BETWEEN b.secinfostartdt AND b.secinfoenddt
        WHERE b.sharetype IN ('NS', 'N/A')
        AND b.securitytype = 'EQTY'
        AND b.securitysubtype = 'COM'
        AND b.usincflg = 'Y'
        AND b.issuertype IN ('ACOR', 'CORP')
        AND a.dlycaldt BETWEEN '{year}-01-01' AND '{year}-12-31'
        AND a.dlyprc IS NOT NULL
        AND a.shrout IS NOT NULL
        ORDER BY a.permno, a.dlycaldt
        """

        try:
            df = self.conn.raw_sql(sql)
            # Convert date to datetime
            df["date"] = pd.to_datetime(df["date"])
            return df
        except Exception as e:
            print(f"  Error pulling {year}: {e}")
            return None

    def _consolidate_annual_files(self) -> None:
        """Concatenate all annual parquet files into one consolidated file."""
        annual_files = sorted(self.raw_dir.glob(f"{self.table_name}_[0-9][0-9][0-9][0-9].parquet"))

        if not annual_files:
            print(f"No annual files found for consolidation.")
            return

        print(f"\nConsolidating {len(annual_files)} annual files...")

        # Read and concatenate
        dfs = []
        for f in track(annual_files, description="Consolidating"):
            dfs.append(pd.read_parquet(f))

        consolidated = pd.concat(dfs, ignore_index=True)
        consolidated_file = self.raw_dir / f"{self.table_name}.parquet"
        consolidated.to_parquet(consolidated_file, engine="pyarrow", compression="snappy")

        # Optionally delete annual files
        for f in annual_files:
            f.unlink()

        print(f"✓ Consolidated to {consolidated_file.name}")
        self.checkpoint.mark_complete(self.table_name, len(consolidated))
