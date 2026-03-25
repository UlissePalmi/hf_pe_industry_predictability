"""Extract Compustat fundamental and GICS data from WRDS."""
import pandas as pd

from pipeline import config
from pipeline.extract.checkpoint import Checkpoint

try:
    import wrds
except ImportError:
    wrds = None  # type: ignore


class CompustatExtractor:
    """Pull Compustat quarterly fundamentals and company data from WRDS."""

    def __init__(self, conn: "wrds.Connection", checkpoint: Checkpoint):
        """
        Initialize Compustat extractor.

        Args:
            conn: WRDS Connection object.
            checkpoint: Checkpoint manager.
        """
        self.conn = conn
        self.checkpoint = checkpoint
        self.raw_dir = config.RAW_DIR

    def run(self, force: bool = False) -> None:
        """
        Pull Compustat data tables.

        Args:
            force: If True, re-pull even if already complete.
        """
        self._pull_fundq(force=force)
        self._pull_company(force=force)
        self._pull_hgics(force=force)

    def _pull_fundq(self, force: bool = False) -> None:
        """Pull quarterly fundamentals (comp.fundq)."""
        table_name = "compustat_fundq"

        if not force and self.checkpoint.is_complete(table_name):
            print(f"✓ {table_name} already complete, skipping.")
            return

        print(f"\nPulling {table_name}...")

        sql = f"""
        SELECT
            f.gvkey,
            f.datadate,
            f.fyearq,
            f.fqtr,
            f.epsfxq,
            f.epspxq,
            f.cshoq,
            f.rdq
        FROM {config.COMPUSTAT_FUNDQ_TABLE} AS f
        INNER JOIN {config.COMPUSTAT_COMPANY_TABLE} AS c
            ON f.gvkey = c.gvkey
        WHERE c.loc = 'USA'
          AND f.indfmt = 'INDL'
          AND f.datafmt = 'STD'
          AND f.popsrc = 'D'
          AND f.consol = 'C'
          AND f.epspxq IS NOT NULL
          AND f.datadate >= '1962-01-01'
        ORDER BY f.gvkey, f.datadate
        """

        try:
            df = self.conn.raw_sql(sql)
            # Convert date columns to datetime
            df["datadate"] = pd.to_datetime(df["datadate"])
            df["rdq"] = pd.to_datetime(df["rdq"])

            output_file = self.raw_dir / f"{table_name}.parquet"
            df.to_parquet(output_file, engine="pyarrow", compression="snappy")

            self.checkpoint.mark_complete(table_name, len(df))
            print(f"✓ Saved {len(df):,} rows to {output_file.name}")

        except Exception as e:
            print(f"Error pulling {table_name}: {e}")
            raise

    def _pull_company(self, force: bool = False) -> None:
        """Pull company info with GICS codes (comp.company)."""
        table_name = "compustat_company"

        if not force and self.checkpoint.is_complete(table_name):
            print(f"✓ {table_name} already complete, skipping.")
            return

        print(f"\nPulling {table_name}...")

        sql = f"""
        SELECT
            gvkey,
            conm,
            gsector,
            ggroup,
            gind,
            gsubind,
            sic,
            naics,
            loc,
            fic
        FROM {config.COMPUSTAT_COMPANY_TABLE}
        WHERE loc = 'USA'
          AND gsector IS NOT NULL
        """

        try:
            df = self.conn.raw_sql(sql)

            output_file = self.raw_dir / f"{table_name}.parquet"
            df.to_parquet(output_file, engine="pyarrow", compression="snappy")

            self.checkpoint.mark_complete(table_name, len(df))
            print(f"✓ Saved {len(df):,} rows to {output_file.name}")

        except Exception as e:
            print(f"Error pulling {table_name}: {e}")
            raise

    def _pull_hgics(self, force: bool = False) -> None:
        """
        Pull historical GICS classifications (comp.co_hgic).

        This table contains the history of GICS code changes per company.
        """
        table_name = "compustat_hgics"

        if not force and self.checkpoint.is_complete(table_name):
            print(f"✓ {table_name} already complete, skipping.")
            return

        print(f"\nPulling {table_name}...")

        sql = f"""
        SELECT
            gvkey,
            indfrom,
            indthru,
            gsector,
            ggroup,
            gind,
            gsubind
        FROM {config.COMPUSTAT_HGICS_TABLE}
        ORDER BY gvkey, indfrom
        """

        try:
            df = self.conn.raw_sql(sql)
            # Convert date columns to datetime
            df["indfrom"] = pd.to_datetime(df["indfrom"])
            df["indthru"] = pd.to_datetime(df["indthru"])

            output_file = self.raw_dir / f"{table_name}.parquet"
            df.to_parquet(output_file, engine="pyarrow", compression="snappy")

            self.checkpoint.mark_complete(table_name, len(df))
            print(f"✓ Saved {len(df):,} rows to {output_file.name}")

        except Exception as e:
            # comp.co_hgic may not be in all subscriptions; don't fail hard
            print(f"⚠ Could not pull {table_name} (may not be in subscription): {e}")
            print(f"  Fallback: will use static GICS codes from comp.company")
