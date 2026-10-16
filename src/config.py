import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
CSV_DIR = DATA_DIR
SQLITE_DB_PATH = DATA_DIR / "economic_outlook_2027.sqlite"
PDF_REPORT_PATH = Path(os.getenv("PDF_REPORT_PATH", "/Users/adamanwar/Desktop/economic-2027.pdf"))
