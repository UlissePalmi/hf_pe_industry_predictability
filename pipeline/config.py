"""Configuration and constants for the pipeline."""
from pathlib import Path
from datetime import datetime

# Paths
PROJECT_ROOT = Path(__file__).parent.parent
DATA_DIR = PROJECT_ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
PROCESSED_DIR = DATA_DIR / "processed"
OUTPUT_DIR = DATA_DIR / "output"

# Create directories if they don't exist
for d in [RAW_DIR, PROCESSED_DIR, OUTPUT_DIR]:
    d.mkdir(parents=True, exist_ok=True)

# CRSP filters
VALID_SHRCDS = {10, 11}  # Common shares only
VALID_EXCHCDS = {1, 2, 3, 31, 32, 33}  # NYSE, AMEX, NASDAQ and satellites

# CCM linking
VALID_LINKTYPES = {"LU", "LC"}  # LU = unresearched, LC = researched
VALID_LINKPRIMS = {"P", "C"}    # P = primary, C = calendar primary

# PE computation
MIN_QUARTERS_FOR_TTM = 4
MAX_EPS_STALENESS_DAYS = 730  # 2 years

# Earnings announcement lag if rdq is missing (conservative estimate)
REPORT_LAG_DAYS = 45

# Date range for the analysis
START_DATE = "1963-01-01"  # Compustat fundamentals coverage starts ~1962-1963
END_DATE = datetime.now().strftime("%Y-%m-%d")

# GICS level definitions
GICS_LEVELS = {
    "sector": {"col": "gsector", "digits": 2, "count": 11},
    "industry_group": {"col": "ggroup", "digits": 4, "count": 24},
    "industry": {"col": "gind", "digits": 6, "count": 69},
    "sub_industry": {"col": "gsubind", "digits": 8, "count": 158},
}

# Checkpoint metadata file
CHECKPOINT_FILE = RAW_DIR / "_checkpoint.json"

# WRDS table names
CRSP_DAILY_TABLE = "crsp.dsf"
CRSP_NAMES_TABLE = "crsp.msenames"
COMPUSTAT_FUNDQ_TABLE = "comp.fundq"
COMPUSTAT_COMPANY_TABLE = "comp.company"
COMPUSTAT_HGICS_TABLE = "comp.co_hgic"
CCM_LINK_TABLE = "crsp.ccmxpf_lnkhist"
