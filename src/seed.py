"""
Seed DuckDB from processed county data JSON.

Loads county_health_access.json, inserts into DuckDB,
then computes need_score, access_score, gap_score, and quadrant via SQL.

Usage:
    python -m src.seed
"""

import json
from pathlib import Path
from src.models import get_connection, create_tables

DATA_DIR = Path(__file__).parent.parent / "data"
PROCESSED_PATH = DATA_DIR / "processed" / "county_health_access.json"


def seed():
    """Main seed function."""
    print("=" * 60)
    print("Healthcare Access Gap Finder - Seeding DuckDB")
    print("=" * 60)

    if not PROCESSED_PATH.exists():
        print(f"ERROR: {PROCESSED_PATH} not found.")
        print("Run `python -m src.ingest` then `python -m src.clean` first.")
        return

    with open(PROCESSED_PATH) as f:
        counties = json.load(f)

    print(f"  Loaded {len(counties)} counties from processed JSON")

    conn = get_connection()
    create_tables(conn)

    # Insert all counties
    insert_sql = """
    INSERT INTO counties (
        fips, name, state, population, median_income, poverty_rate,
        pct_white, pct_black, pct_hispanic,
        uninsured_pct, no_checkup_pct, depression_pct, diabetes_pct, obesity_pct,
        mental_health_bad_pct, physical_health_bad_pct,
        mammography_pct, preventive_men_pct, preventive_women_pct,
        hpsa_pc_count, hpsa_mh_count, hpsa_dh_count, hpsa_severity_avg,
        fqhc_count, fqhc_per_100k,
        latitude, longitude
    ) VALUES (
        $1, $2, $3, $4, $5, $6,
        $7, $8, $9,
        $10, $11, $12, $13, $14,
        $15, $16,
        $17, $18, $19,
        $20, $21, $22, $23,
        $24, $25,
        $26, $27
    )
    """

    inserted = 0
    skipped = 0
    for c in counties:
        try:
            conn.execute(insert_sql, [
                c.get("fips"),
                c.get("name"),
                c.get("state"),
                c.get("population"),
                c.get("median_income"),
                c.get("poverty_rate"),
                c.get("pct_white"),
                c.get("pct_black"),
                c.get("pct_hispanic"),
                c.get("uninsured_pct"),
                c.get("no_checkup_pct"),
                c.get("depression_pct"),
                c.get("diabetes_pct"),
                c.get("obesity_pct"),
                c.get("mental_health_bad_pct"),
                c.get("physical_health_bad_pct"),
                c.get("mammography_pct"),
                c.get("preventive_men_pct"),
                c.get("preventive_women_pct"),
                c.get("hpsa_pc_count", 0),
                c.get("hpsa_mh_count", 0),
                c.get("hpsa_dh_count", 0),
                c.get("hpsa_severity_avg", 0.0),
                c.get("fqhc_count", 0),
                c.get("fqhc_per_100k", 0.0),
                c.get("latitude"),
                c.get("longitude"),
            ])
            inserted += 1
        except Exception as e:
            skipped += 1
            if skipped <= 5:
                print(f"  Warning: skipped {c.get('fips')}: {e}")

    print(f"  Inserted: {inserted} counties")
    if skipped > 0:
        print(f"  Skipped: {skipped} counties")

    # Compute need_score via SQL using PERCENT_RANK
    print("  Computing need scores...")
    conn.execute("""
    UPDATE counties SET need_score = scored.need_score
    FROM (
        SELECT fips,
            ROUND(
                (PERCENT_RANK() OVER (ORDER BY COALESCE(uninsured_pct, 0)) * 20) +
                (PERCENT_RANK() OVER (ORDER BY COALESCE(no_checkup_pct, 0)) * 15) +
                (PERCENT_RANK() OVER (ORDER BY COALESCE(mental_health_bad_pct, 0)) * 15) +
                (PERCENT_RANK() OVER (ORDER BY COALESCE(diabetes_pct, 0)) * 15) +
                (PERCENT_RANK() OVER (ORDER BY COALESCE(obesity_pct, 0)) * 10) +
                (PERCENT_RANK() OVER (ORDER BY COALESCE(depression_pct, 0)) * 10) +
                (PERCENT_RANK() OVER (ORDER BY COALESCE(physical_health_bad_pct, 0)) * 10) +
                (PERCENT_RANK() OVER (ORDER BY (100 - COALESCE((COALESCE(preventive_men_pct, 50) + COALESCE(preventive_women_pct, 50)) / 2, 50))) * 5)
            , 1) AS need_score
        FROM counties
    ) AS scored
    WHERE counties.fips = scored.fips
    """)

    # Compute access_score via SQL using PERCENT_RANK
    print("  Computing access scores...")
    conn.execute("""
    UPDATE counties SET access_score = scored.access_score
    FROM (
        SELECT fips,
            ROUND(
                (PERCENT_RANK() OVER (ORDER BY COALESCE(fqhc_per_100k, 0)) * 30) +
                (PERCENT_RANK() OVER (ORDER BY COALESCE(hpsa_severity_avg, 0) DESC) * 40) +
                (PERCENT_RANK() OVER (ORDER BY (100 - COALESCE(uninsured_pct, 0))) * 30)
            , 1) AS access_score
        FROM counties
    ) AS scored
    WHERE counties.fips = scored.fips
    """)

    # Compute gap_score
    print("  Computing gap scores...")
    conn.execute("""
    UPDATE counties SET gap_score = ROUND(need_score - access_score, 1)
    """)

    # Assign quadrants
    print("  Assigning quadrants...")
    conn.execute("""
    UPDATE counties SET quadrant = CASE
        WHEN need_score <= 50 AND access_score >= 50 THEN 'well_served'
        WHEN need_score <= 50 AND access_score < 50 THEN 'at_risk'
        WHEN need_score > 50 AND access_score >= 50 THEN 'strained'
        WHEN need_score > 50 AND access_score < 50 THEN 'critical_gap'
    END
    """)

    # Print summary
    result = conn.execute("""
    SELECT
        quadrant,
        COUNT(*) as count,
        ROUND(AVG(need_score), 1) as avg_need,
        ROUND(AVG(access_score), 1) as avg_access,
        ROUND(AVG(gap_score), 1) as avg_gap
    FROM counties
    GROUP BY quadrant
    ORDER BY avg_gap DESC
    """).fetchall()

    total = conn.execute("SELECT COUNT(*) FROM counties").fetchone()[0]

    print(f"\n{'=' * 60}")
    print("Seed Summary")
    print(f"{'=' * 60}")
    print(f"  Total counties in DB: {total}")
    print(f"\n  Quadrant Distribution:")
    for row in result:
        quad, count, avg_need, avg_access, avg_gap = row
        print(f"    {quad}: {count} counties (avg need={avg_need}, access={avg_access}, gap={avg_gap})")

    # Top gap counties
    top_gap = conn.execute("""
    SELECT name, state, gap_score, need_score, access_score
    FROM counties ORDER BY gap_score DESC LIMIT 5
    """).fetchall()
    print(f"\n  Top 5 Highest Gap Counties:")
    for name, state, gap, need, access in top_gap:
        print(f"    {name}, {state}: gap={gap} (need={need}, access={access})")

    conn.close()
    print(f"\nDatabase saved to: {DATA_DIR / 'healthcare.duckdb'}")
    print("Run `streamlit run dashboard.py` to launch the dashboard.")


if __name__ == "__main__":
    seed()
