import sqlite3
from pathlib import Path
import pandas as pd

DATA_DIR = Path(__file__).resolve().parent.parent / "data"

def test_sqlite_db_exists():
    db_path = DATA_DIR / "economic_outlook_2027.sqlite"
    assert db_path.exists()
    conn = sqlite3.connect(str(db_path))
    tables = [r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()]
    conn.close()
    assert len(tables) >= 12
    assert "table_3_16_state_cpi" in tables

def test_csv_exports():
    csvs = list(DATA_DIR.glob("*.csv"))
    assert len(csvs) >= 12
