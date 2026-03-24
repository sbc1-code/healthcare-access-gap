"""
Analytics layer for Healthcare Access Gap Finder.
All SQL queries run against DuckDB, return pandas DataFrames.

Usage:
    from src import analytics
    df = analytics.get_all_counties()
"""

import os
import pandas as pd
import duckdb
from pathlib import Path

DB_PATH = Path(__file__).parent.parent / "data" / "healthcare.duckdb"
PROCESSED_PATH = Path(__file__).parent.parent / "data" / "processed" / "county_health_access.json"

# Border county FIPS codes (US-Mexico border), reused from VerdeAzul
BORDER_FIPS = {
    # Texas
    "48141",  # El Paso
    "48243",  # Jeff Davis
    "48377",  # Presidio
    "48043",  # Brewster
    "48371",  # Pecos
    "48443",  # Terrell
    "48465",  # Val Verde
    "48323",  # Maverick
    "48479",  # Webb
    "48427",  # Starr
    "48215",  # Hidalgo
    "48061",  # Cameron
    "48505",  # Zapata
    "48247",  # Jim Hogg
    "48311",  # McMullen
    # New Mexico
    "35013",  # Dona Ana
    "35023",  # Hidalgo
    "35029",  # Luna
    "35035",  # Otero
    # Arizona
    "04003",  # Cochise
    "04019",  # Pima
    "04023",  # Santa Cruz
    "04027",  # Yuma
    # California
    "06025",  # Imperial
    "06073",  # San Diego
}


def _ensure_db():
    """Check DB exists and is current. Re-seed if JSON is newer than DB."""
    if DB_PATH.exists() and PROCESSED_PATH.exists():
        db_mtime = os.path.getmtime(DB_PATH)
        json_mtime = os.path.getmtime(PROCESSED_PATH)
        if json_mtime > db_mtime:
            print("Processed data is newer than database. Re-seeding...")
            DB_PATH.unlink()
        else:
            try:
                conn = duckdb.connect(str(DB_PATH), read_only=True)
                count = conn.execute("SELECT COUNT(*) FROM counties").fetchone()[0]
                conn.close()
                if count > 0:
                    return True
            except Exception:
                pass

    if PROCESSED_PATH.exists():
        print("Auto-seeding from processed data...")
        from src.seed import seed
        seed()
        return DB_PATH.exists()

    print("No database or processed data found.")
    print("Run: python -m src.ingest && python -m src.clean && python -m src.seed")
    return False


def _get_conn():
    """Get a read-only DuckDB connection. Auto-seeds if needed."""
    _ensure_db()
    if not DB_PATH.exists():
        raise FileNotFoundError(
            "No database or processed data found. "
            "Run `python -m src.ingest`, then `python -m src.clean`, then `python -m src.seed`."
        )
    return duckdb.connect(str(DB_PATH), read_only=True)


def _query(sql, params=None):
    """Execute SQL and return a DataFrame."""
    conn = _get_conn()
    try:
        if params:
            result = conn.execute(sql, params).fetchdf()
        else:
            result = conn.execute(sql).fetchdf()
        return result
    finally:
        conn.close()


def get_overview_stats():
    """KPIs: county count, avg need/access/gap, critical gap count."""
    return _query("""
        SELECT
            COUNT(*) AS total_counties,
            ROUND(AVG(need_score), 1) AS avg_need_score,
            ROUND(AVG(access_score), 1) AS avg_access_score,
            ROUND(AVG(gap_score), 1) AS avg_gap_score,
            SUM(CASE WHEN quadrant = 'critical_gap' THEN 1 ELSE 0 END) AS critical_gap_count,
            SUM(CASE WHEN quadrant = 'well_served' THEN 1 ELSE 0 END) AS well_served_count,
            SUM(CASE WHEN quadrant = 'at_risk' THEN 1 ELSE 0 END) AS at_risk_count,
            SUM(CASE WHEN quadrant = 'strained' THEN 1 ELSE 0 END) AS strained_count,
            ROUND(AVG(population), 0) AS avg_population,
            ROUND(AVG(median_income), 0) AS avg_income
        FROM counties
    """)


def get_all_counties():
    """All counties with scores, sorted by gap_score descending."""
    return _query("""
        SELECT
            fips, name, state, population,
            median_income, poverty_rate,
            uninsured_pct, no_checkup_pct,
            depression_pct, diabetes_pct, obesity_pct,
            mental_health_bad_pct, physical_health_bad_pct,
            mammography_pct, preventive_men_pct, preventive_women_pct,
            hpsa_pc_count, hpsa_mh_count, hpsa_dh_count, hpsa_severity_avg,
            fqhc_count, fqhc_per_100k,
            latitude, longitude,
            need_score, access_score, gap_score, quadrant
        FROM counties
        ORDER BY gap_score DESC
    """)


def get_county_detail(fips):
    """Single county full detail."""
    return _query("""
        SELECT *
        FROM counties
        WHERE fips = $1
    """, [fips])


def get_top_gap_counties(limit=10):
    """Counties with the highest gap scores."""
    return _query("""
        SELECT
            fips, name, state, population,
            need_score, access_score, gap_score, quadrant,
            uninsured_pct, hpsa_severity_avg, fqhc_per_100k
        FROM counties
        ORDER BY gap_score DESC
        LIMIT $1
    """, [limit])


def get_quadrant_distribution():
    """Count per quadrant with averages."""
    return _query("""
        SELECT
            quadrant,
            COUNT(*) AS count,
            ROUND(AVG(need_score), 1) AS avg_need,
            ROUND(AVG(access_score), 1) AS avg_access,
            ROUND(AVG(gap_score), 1) AS avg_gap,
            ROUND(AVG(population), 0) AS avg_population,
            ROUND(AVG(median_income), 0) AS avg_income
        FROM counties
        GROUP BY quadrant
        ORDER BY avg_gap DESC
    """)


def get_state_summary():
    """Aggregated stats by state."""
    return _query("""
        SELECT
            state,
            COUNT(*) AS county_count,
            ROUND(AVG(need_score), 1) AS avg_need,
            ROUND(AVG(access_score), 1) AS avg_access,
            ROUND(AVG(gap_score), 1) AS avg_gap,
            SUM(CASE WHEN quadrant = 'critical_gap' THEN 1 ELSE 0 END) AS critical_count,
            ROUND(AVG(uninsured_pct), 1) AS avg_uninsured,
            ROUND(AVG(hpsa_severity_avg), 1) AS avg_hpsa_severity,
            ROUND(SUM(population), 0) AS total_population
        FROM counties
        GROUP BY state
        ORDER BY avg_gap DESC
    """)


def get_scatter_data():
    """Need vs Access for scatter plot, with quadrant coloring."""
    return _query("""
        SELECT
            fips, name, state, population,
            need_score, access_score, gap_score, quadrant
        FROM counties
        WHERE need_score IS NOT NULL AND access_score IS NOT NULL
    """)


def get_county_health_profile(fips):
    """Health metrics for a single county (detail view)."""
    return _query("""
        SELECT
            fips, name, state, population,
            uninsured_pct, no_checkup_pct,
            depression_pct, diabetes_pct, obesity_pct,
            mental_health_bad_pct, physical_health_bad_pct,
            mammography_pct, preventive_men_pct, preventive_women_pct,
            hpsa_pc_count, hpsa_mh_count, hpsa_dh_count, hpsa_severity_avg,
            fqhc_count, fqhc_per_100k,
            median_income, poverty_rate,
            pct_white, pct_black, pct_hispanic,
            need_score, access_score, gap_score, quadrant
        FROM counties
        WHERE fips = $1
    """, [fips])


def get_national_averages():
    """National average benchmark values for all key metrics."""
    return _query("""
        SELECT
            ROUND(AVG(uninsured_pct), 1) AS avg_uninsured,
            ROUND(AVG(no_checkup_pct), 1) AS avg_no_checkup,
            ROUND(AVG(depression_pct), 1) AS avg_depression,
            ROUND(AVG(diabetes_pct), 1) AS avg_diabetes,
            ROUND(AVG(obesity_pct), 1) AS avg_obesity,
            ROUND(AVG(mental_health_bad_pct), 1) AS avg_mental_health_bad,
            ROUND(AVG(physical_health_bad_pct), 1) AS avg_physical_health_bad,
            ROUND(AVG(mammography_pct), 1) AS avg_mammography,
            ROUND(AVG(preventive_men_pct), 1) AS avg_preventive_men,
            ROUND(AVG(preventive_women_pct), 1) AS avg_preventive_women,
            ROUND(AVG(hpsa_severity_avg), 1) AS avg_hpsa_severity,
            ROUND(AVG(fqhc_per_100k), 2) AS avg_fqhc_per_100k,
            ROUND(AVG(median_income), 0) AS avg_income,
            ROUND(AVG(poverty_rate), 1) AS avg_poverty,
            ROUND(AVG(need_score), 1) AS avg_need,
            ROUND(AVG(access_score), 1) AS avg_access,
            ROUND(AVG(gap_score), 1) AS avg_gap
        FROM counties
    """)


def get_border_comparison():
    """Border vs non-border county comparison using FIPS list."""
    border_list = ",".join(f"'{f}'" for f in BORDER_FIPS)
    return _query(f"""
        SELECT
            CASE WHEN fips IN ({border_list}) THEN 'Border' ELSE 'Non-Border' END AS category,
            COUNT(*) AS county_count,
            ROUND(AVG(need_score), 1) AS avg_need,
            ROUND(AVG(access_score), 1) AS avg_access,
            ROUND(AVG(gap_score), 1) AS avg_gap,
            ROUND(AVG(uninsured_pct), 1) AS avg_uninsured,
            ROUND(AVG(hpsa_severity_avg), 1) AS avg_hpsa_severity,
            ROUND(AVG(fqhc_per_100k), 2) AS avg_fqhc_per_100k,
            ROUND(AVG(median_income), 0) AS avg_income,
            ROUND(AVG(poverty_rate), 1) AS avg_poverty,
            ROUND(AVG(diabetes_pct), 1) AS avg_diabetes,
            ROUND(AVG(obesity_pct), 1) AS avg_obesity,
            ROUND(AVG(mental_health_bad_pct), 1) AS avg_mental_health_bad
        FROM counties
        GROUP BY CASE WHEN fips IN ({border_list}) THEN 'Border' ELSE 'Non-Border' END
    """)


def get_counties_by_state(state_abbr):
    """All counties in a given state."""
    return _query("""
        SELECT
            fips, name, state, population,
            need_score, access_score, gap_score, quadrant,
            uninsured_pct, hpsa_severity_avg, fqhc_per_100k
        FROM counties
        WHERE state = $1
        ORDER BY gap_score DESC
    """, [state_abbr])


def get_county_list():
    """Minimal county list for dropdown search."""
    return _query("""
        SELECT fips, name, state
        FROM counties
        ORDER BY state, name
    """)
