"""
DuckDB schema for Healthcare Access Gap Finder.

Creates the counties table and provides connection helpers.

Usage:
    from src.models import get_connection, create_tables
"""

import duckdb
from pathlib import Path

DB_PATH = Path(__file__).parent.parent / "data" / "healthcare.duckdb"

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS counties (
    fips TEXT PRIMARY KEY,
    name TEXT,
    state TEXT,
    population INTEGER,
    median_income REAL,
    poverty_rate REAL,
    pct_white REAL,
    pct_black REAL,
    pct_hispanic REAL,
    uninsured_pct REAL,
    no_checkup_pct REAL,
    depression_pct REAL,
    diabetes_pct REAL,
    obesity_pct REAL,
    mental_health_bad_pct REAL,
    physical_health_bad_pct REAL,
    mammography_pct REAL,
    preventive_men_pct REAL,
    preventive_women_pct REAL,
    hpsa_pc_count INTEGER,
    hpsa_mh_count INTEGER,
    hpsa_dh_count INTEGER,
    hpsa_severity_avg REAL,
    fqhc_count INTEGER,
    fqhc_per_100k REAL,
    latitude REAL,
    longitude REAL,
    need_score REAL,
    access_score REAL,
    gap_score REAL,
    quadrant TEXT
);
"""


def get_connection():
    """Return a DuckDB connection. Creates the DB file if needed."""
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(DB_PATH))


def create_tables(conn=None):
    """Create all tables. Drops existing tables first for a clean seed."""
    close_after = False
    if conn is None:
        conn = get_connection()
        close_after = True

    conn.execute("DROP TABLE IF EXISTS counties")
    conn.execute(SCHEMA_SQL)

    if close_after:
        conn.close()


def db_exists():
    """Check if the DuckDB file exists and has the counties table."""
    if not DB_PATH.exists():
        return False
    try:
        conn = duckdb.connect(str(DB_PATH), read_only=True)
        result = conn.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'counties'"
        ).fetchone()
        conn.close()
        return result[0] > 0
    except Exception:
        return False
