"""Checkpoint and resume management for WRDS pulls."""
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional
from dataclasses import dataclass, asdict

from pipeline import config


@dataclass
class TableCheckpoint:
    """Status of a single WRDS table pull."""

    status: str  # "pending", "in_progress", "complete"
    completed_years: list[int]  # For CRSP: years already pulled
    row_count: Optional[int] = None
    last_updated: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dict for JSON serialization."""
        return {
            "status": self.status,
            "completed_years": self.completed_years,
            "row_count": self.row_count,
            "last_updated": self.last_updated,
        }


class Checkpoint:
    """Manages checkpoint metadata for resumable pulls."""
    def __init__(self, raw_dir: Path = config.RAW_DIR):
        """
        Initialize checkpoint manager.

        Args:
            raw_dir: Directory where checkpoint.json and parquet files are stored.
        """
        self.raw_dir = raw_dir
        self.checkpoint_file = raw_dir / "_checkpoint.json"
        self.data = self._load()

    def _load(self) -> Dict[str, Any]:
        """Load checkpoint metadata from disk, or initialize empty."""
        if self.checkpoint_file.exists():
            with open(self.checkpoint_file, "r") as f:
                return json.load(f)
        return {}

    def _save(self) -> None:
        """Write checkpoint metadata to disk."""
        self.raw_dir.mkdir(parents=True, exist_ok=True)
        with open(self.checkpoint_file, "w") as f:
            json.dump(self.data, f, indent=2)

    def get_table(self, table_name: str) -> Optional[TableCheckpoint]:
        """
        Get checkpoint for a table, or None if not started.

        Args:
            table_name: Key like "crsp_daily", "compustat_fundq", etc.

        Returns:
            TableCheckpoint object or None.
        """
        if table_name not in self.data:
            return None
        d = self.data[table_name]
        return TableCheckpoint(
            status=d["status"],
            completed_years=d.get("completed_years", []),
            row_count=d.get("row_count"),
            last_updated=d.get("last_updated"),
        )

    def set_table(self, table_name: str, checkpoint: TableCheckpoint) -> None:
        """
        Update checkpoint for a table.

        Args:
            table_name: Key like "crsp_daily".
            checkpoint: TableCheckpoint object.
        """
        checkpoint.last_updated = datetime.utcnow().isoformat() + "Z"
        self.data[table_name] = checkpoint.to_dict()
        self._save()

    def mark_year_complete(
        self, table_name: str, year: int, row_count: Optional[int] = None
    ) -> None:
        """
        Mark a year as completed for CRSP pulls.

        Args:
            table_name: e.g., "crsp_daily".
            year: Calendar year (e.g., 1963).
            row_count: Optional total rows for this year.
        """
        ckpt = self.get_table(table_name)
        if ckpt is None:
            ckpt = TableCheckpoint(
                status="in_progress", completed_years=[year], row_count=row_count
            )
        else:
            if year not in ckpt.completed_years:
                ckpt.completed_years.append(year)
            ckpt.completed_years.sort()
            if row_count is not None:
                ckpt.row_count = (ckpt.row_count or 0) + row_count
        self.set_table(table_name, ckpt)

    def mark_complete(self, table_name: str, row_count: int) -> None:
        """
        Mark a table pull as fully complete.

        Args:
            table_name: e.g., "compustat_fundq".
            row_count: Total rows in the table.
        """
        ckpt = TableCheckpoint(
            status="complete", completed_years=[], row_count=row_count
        )
        self.set_table(table_name, ckpt)

    def is_complete(self, table_name: str) -> bool:
        """Check if a table has been fully pulled."""
        ckpt = self.get_table(table_name)
        return ckpt is not None and ckpt.status == "complete"

    def needs_year(self, table_name: str, year: int) -> bool:
        """Check if a year still needs to be pulled for CRSP."""
        ckpt = self.get_table(table_name)
        if ckpt is None:
            return True
        return year not in ckpt.completed_years

    def print_status(self) -> None:
        """Print a human-readable status summary."""
        if not self.data:
            print("No data pulled yet.")
            return

        for table_name, meta in self.data.items():
            status = meta["status"]
            row_count = meta.get("row_count", "?")
            updated = meta.get("last_updated", "?")

            if "completed_years" in meta and meta["completed_years"]:
                years = meta["completed_years"]
                year_range = f"{min(years)}-{max(years)}"
                print(
                    f"{table_name}: {status} | {year_range} | "
                    f"{row_count} rows | {updated}"
                )
            else:
                print(f"{table_name}: {status} | {row_count} rows | {updated}")
