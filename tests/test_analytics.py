"""
Tests for analytics functions.

Auto-seeds the database if processed JSON exists but DB does not.
Requires data to have been ingested and cleaned first.
"""

import json
import pytest
import pandas as pd
from pathlib import Path

# Check if we can run tests (need data)
DB_PATH = Path(__file__).parent.parent / "data" / "healthcare.duckdb"
PROCESSED_PATH = Path(__file__).parent.parent / "data" / "processed" / "county_health_access.json"


def _ensure_db():
    """Auto-seed if processed JSON exists but DB does not."""
    if DB_PATH.exists():
        return True
    if PROCESSED_PATH.exists():
        from src.seed import seed
        seed()
        return DB_PATH.exists()
    return False


# Skip all tests if no data available
pytestmark = pytest.mark.skipif(
    not PROCESSED_PATH.exists(),
    reason="No processed data available. Run ingest + clean first."
)


@pytest.fixture(scope="session", autouse=True)
def setup_db():
    """Ensure DB is seeded before tests run."""
    _ensure_db()


class TestOverviewStats:
    def test_returns_dataframe(self):
        from src.analytics import get_overview_stats
        df = get_overview_stats()
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 1

    def test_has_required_columns(self):
        from src.analytics import get_overview_stats
        df = get_overview_stats()
        required = ["total_counties", "avg_need_score", "avg_access_score",
                     "avg_gap_score", "critical_gap_count"]
        for col in required:
            assert col in df.columns, f"Missing column: {col}"

    def test_county_count_positive(self):
        from src.analytics import get_overview_stats
        df = get_overview_stats()
        assert df["total_counties"].iloc[0] > 0


class TestAllCounties:
    def test_returns_dataframe(self):
        from src.analytics import get_all_counties
        df = get_all_counties()
        assert isinstance(df, pd.DataFrame)
        assert len(df) > 0

    def test_has_score_columns(self):
        from src.analytics import get_all_counties
        df = get_all_counties()
        for col in ["need_score", "access_score", "gap_score", "quadrant"]:
            assert col in df.columns

    def test_sorted_by_gap(self):
        from src.analytics import get_all_counties
        df = get_all_counties()
        gaps = df["gap_score"].tolist()
        # Should be sorted descending (with possible None handling)
        valid = [g for g in gaps if g is not None]
        assert valid == sorted(valid, reverse=True)


class TestCountyDetail:
    def test_returns_single_row(self):
        from src.analytics import get_all_counties, get_county_detail
        all_df = get_all_counties()
        if len(all_df) > 0:
            fips = all_df["fips"].iloc[0]
            detail = get_county_detail(fips)
            assert len(detail) == 1
            assert detail["fips"].iloc[0] == fips

    def test_nonexistent_fips(self):
        from src.analytics import get_county_detail
        detail = get_county_detail("99999")
        assert len(detail) == 0


class TestTopGapCounties:
    def test_default_limit(self):
        from src.analytics import get_top_gap_counties
        df = get_top_gap_counties()
        assert len(df) <= 10

    def test_custom_limit(self):
        from src.analytics import get_top_gap_counties
        df = get_top_gap_counties(limit=5)
        assert len(df) <= 5

    def test_sorted_descending(self):
        from src.analytics import get_top_gap_counties
        df = get_top_gap_counties()
        gaps = df["gap_score"].tolist()
        assert gaps == sorted(gaps, reverse=True)


class TestQuadrantDistribution:
    def test_returns_quadrants(self):
        from src.analytics import get_quadrant_distribution
        df = get_quadrant_distribution()
        assert isinstance(df, pd.DataFrame)
        assert len(df) > 0
        assert "quadrant" in df.columns
        assert "count" in df.columns

    def test_valid_quadrant_names(self):
        from src.analytics import get_quadrant_distribution
        df = get_quadrant_distribution()
        valid = {"well_served", "at_risk", "strained", "critical_gap"}
        for q in df["quadrant"].tolist():
            assert q in valid, f"Unknown quadrant: {q}"


class TestStateSummary:
    def test_returns_states(self):
        from src.analytics import get_state_summary
        df = get_state_summary()
        assert isinstance(df, pd.DataFrame)
        assert len(df) > 0
        assert "state" in df.columns


class TestScatterData:
    def test_returns_data(self):
        from src.analytics import get_scatter_data
        df = get_scatter_data()
        assert isinstance(df, pd.DataFrame)
        assert len(df) > 0
        assert "need_score" in df.columns
        assert "access_score" in df.columns


class TestCountyHealthProfile:
    def test_returns_profile(self):
        from src.analytics import get_all_counties, get_county_health_profile
        all_df = get_all_counties()
        if len(all_df) > 0:
            fips = all_df["fips"].iloc[0]
            profile = get_county_health_profile(fips)
            assert len(profile) == 1
            assert "uninsured_pct" in profile.columns


class TestNationalAverages:
    def test_returns_averages(self):
        from src.analytics import get_national_averages
        df = get_national_averages()
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 1
        assert "avg_uninsured" in df.columns


class TestBorderComparison:
    def test_returns_two_groups(self):
        from src.analytics import get_border_comparison
        df = get_border_comparison()
        assert isinstance(df, pd.DataFrame)
        # Should have Border and Non-Border rows
        categories = set(df["category"].tolist())
        assert "Non-Border" in categories


class TestCountyList:
    def test_returns_list(self):
        from src.analytics import get_county_list
        df = get_county_list()
        assert isinstance(df, pd.DataFrame)
        assert len(df) > 0
        assert "fips" in df.columns
        assert "name" in df.columns
