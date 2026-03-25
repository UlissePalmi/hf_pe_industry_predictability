"""Extract CRSP-Compustat linking table from WRDS."""
import pandas as pd

from pipeline import config
from pipeline.extract.checkpoint import Checkpoint

try:
    import wrds
except ImportError:
    wrds = None  # type: ignore


class CCMLinkExtractor:
    """Pull CRSP-Compustat Merged (CCM) linking table from WRDS."""

    def __init__(self, conn: "wrds.Connection", checkpoint: Checkpoint):
        """
        Initialize CCM link extractor.

        Args:
            conn: WRDS Connection object.
            checkpoint: Checkpoint manager.
        """
        self.conn = conn
        self.checkpoint = checkpoint
        self.raw_dir = config.RAW_DIR

    def run(self, force: bool = False) -> None:
        """
        Pull CCM linking table.

        Args:
            force: If True, re-pull even if already complete.
        """
        table_name = "ccm_links"

        if not force and self.checkpoint.is_complete(table_name):
            print(f"✓ {table_name} already complete, skipping.")
            return

        print(f"\nPulling {table_name}...")

        sql = f"""
        SELECT
            lpermno AS permno,
            gvkey,
            linktype,
            linkprim,
            linkdt,
            linkenddt
        FROM {config.CCM_LINK_TABLE}
        WHERE linktype IN ({','.join([f"'{t}'" for t in config.VALID_LINKTYPES])})
          AND linkprim IN ({','.join([f"'{p}'" for p in config.VALID_LINKPRIMS])})
        ORDER BY gvkey, linkdt
        """

        try:
            df = self.conn.raw_sql(sql)

            # Convert date columns to datetime
            df["linkdt"] = pd.to_datetime(df["linkdt"])
            df["linkenddt"] = pd.to_datetime(df["linkenddt"])

            # Handle NULL linkenddt (still-active links)
            # Keep as NaT for now; will use CURRENT_DATE logic in transforms

            output_file = self.raw_dir / f"{table_name}.parquet"
            df.to_parquet(output_file, engine="pyarrow", compression="snappy")

            self.checkpoint.mark_complete(table_name, len(df))
            print(f"✓ Saved {len(df):,} rows to {output_file.name}")

        except Exception as e:
            print(f"Error pulling {table_name}: {e}")
            raise
