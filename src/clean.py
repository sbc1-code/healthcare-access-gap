"""
Clean and merge all raw data into a single county-level JSON file.

Reads from data/raw/, outputs data/processed/county_health_access.json.

Usage:
    python -m src.clean
"""

import csv
import json
from collections import defaultdict
from pathlib import Path

RAW_DIR = Path(__file__).parent.parent / "data" / "raw"
PROCESSED_DIR = Path(__file__).parent.parent / "data" / "processed"

# State abbreviation to FIPS mapping
STATE_FIPS = {
    "AL": "01", "AK": "02", "AZ": "04", "AR": "05", "CA": "06",
    "CO": "08", "CT": "09", "DE": "10", "DC": "11", "FL": "12",
    "GA": "13", "HI": "15", "ID": "16", "IL": "17", "IN": "18",
    "IA": "19", "KS": "20", "KY": "21", "LA": "22", "ME": "23",
    "MD": "24", "MA": "25", "MI": "26", "MN": "27", "MS": "28",
    "MO": "29", "MT": "30", "NE": "31", "NV": "32", "NH": "33",
    "NJ": "34", "NM": "35", "NY": "36", "NC": "37", "ND": "38",
    "OH": "39", "OK": "40", "OR": "41", "PA": "42", "PR": "72",
    "RI": "44", "SC": "45", "SD": "46", "TN": "47", "TX": "48",
    "UT": "49", "VT": "50", "VA": "51", "WA": "53", "WV": "54",
    "WI": "55", "WY": "56",
}

FIPS_TO_STATE = {v: k for k, v in STATE_FIPS.items()}


def _safe_float(val):
    """Convert to float, return None if invalid."""
    if val is None:
        return None
    try:
        v = float(val)
        return v if v == v else None  # NaN check
    except (ValueError, TypeError):
        return None


def _safe_int(val):
    """Convert to int, return None if invalid."""
    if val is None:
        return None
    try:
        return int(float(val))
    except (ValueError, TypeError):
        return None


def clean_hpsa():
    """
    Process HPSA CSVs. Group by county FIPS.
    Returns dict keyed by 5-digit FIPS with designation counts and severity stats.
    """
    print("Processing HPSA data...")
    hpsa_types = {
        "hpsa_pc": "pc",
        "hpsa_mh": "mh",
        "hpsa_dh": "dh",
    }

    # county_fips -> {pc_count, mh_count, dh_count, severities: []}
    county_data = defaultdict(lambda: {
        "pc_count": 0, "mh_count": 0, "dh_count": 0,
        "severities": [],
    })

    for filename, htype in hpsa_types.items():
        csv_path = RAW_DIR / f"{filename}.csv"
        if not csv_path.exists():
            print(f"  Skipping {filename}.csv (not found)")
            continue

        count = 0
        try:
            with open(csv_path, "r", encoding="utf-8-sig") as f:
                reader = csv.DictReader(f)
                headers = reader.fieldnames or []

                # Find the county FIPS column - try common variations
                fips_col = None
                severity_col = None
                status_col = None

                # Identify columns by scanning headers
                county_name_col = None
                state_fips_col = None
                county_fips_col = None

                for h in headers:
                    hl = h.lower().strip()
                    if "common state county fips" in hl or ("state" in hl and "county" in hl and "fips" in hl):
                        fips_col = h
                    elif "county fips" in hl and "state" not in hl and fips_col is None:
                        county_fips_col = h
                    if "hpsa score" in hl:
                        severity_col = h
                    if "hpsa status" in hl:
                        status_col = h
                    if "common county name" in hl or ("county" in hl and "name" in hl and "state" not in hl):
                        county_name_col = h
                    if "common state fips" in hl and "county" not in hl:
                        state_fips_col = h

                # If no combined FIPS column, try building from state + county
                if not fips_col and state_fips_col and county_fips_col:
                    fips_col = "__combined__"
                elif not fips_col and county_fips_col:
                    fips_col = county_fips_col

                if not fips_col:
                    # Last resort: any column with "fips" in name
                    for h in headers:
                        if "fips" in h.lower():
                            fips_col = h
                            break

                if not fips_col:
                    print(f"  Warning: No FIPS column found in {filename}.csv")
                    print(f"    Available columns: {headers[:10]}...")
                    continue

                skipped_no_county = 0

                for row in reader:
                    # Only include designated HPSAs (not withdrawn/proposed)
                    if status_col:
                        status = (row.get(status_col) or "").strip().lower()
                        if status and status not in ("designated", ""):
                            continue

                    # Filter: skip rows where Common County Name is null/empty
                    # This excludes population-based HPSAs without clean county mapping
                    if county_name_col:
                        county_name = (row.get(county_name_col) or "").strip()
                        if not county_name:
                            skipped_no_county += 1
                            continue

                    # Get FIPS code
                    if fips_col == "__combined__":
                        st = (row.get(state_fips_col) or "").strip()
                        ct = (row.get(county_fips_col) or "").strip()
                        if not st or not ct:
                            continue
                        try:
                            fips_raw = str(int(float(st))).zfill(2) + str(int(float(ct))).zfill(3)
                        except (ValueError, TypeError):
                            continue
                    else:
                        fips_raw = (row.get(fips_col) or "").strip()

                    if not fips_raw:
                        continue

                    # Normalize to 5-digit FIPS
                    try:
                        fips = str(int(float(fips_raw))).zfill(5)
                    except (ValueError, TypeError):
                        continue

                    if len(fips) != 5:
                        continue

                    county_data[fips][f"{htype}_count"] += 1

                    if severity_col:
                        sev = _safe_float(row.get(severity_col))
                        if sev is not None:
                            county_data[fips]["severities"].append(sev)

                    count += 1

        except Exception as e:
            print(f"  Error reading {filename}.csv: {e}")
            continue

        if skipped_no_county > 0:
            print(f"  {filename}: skipped {skipped_no_county} rows without county name")
        print(f"  {filename}: {count} designations processed")

    # Compute severity stats per county
    result = {}
    for fips, data in county_data.items():
        sevs = data["severities"]
        result[fips] = {
            "hpsa_pc_count": data["pc_count"],
            "hpsa_mh_count": data["mh_count"],
            "hpsa_dh_count": data["dh_count"],
            "hpsa_severity_avg": round(sum(sevs) / len(sevs), 1) if sevs else 0.0,
            "hpsa_severity_max": max(sevs) if sevs else 0.0,
        }

    print(f"  HPSA data for {len(result)} counties")
    return result


def clean_cdc_places():
    """
    Process CDC PLACES data from raw JSON.
    Returns dict keyed by 5-digit FIPS with health measures.
    """
    print("Processing CDC PLACES data...")
    places_path = RAW_DIR / "cdc_places.json"
    if not places_path.exists():
        print("  Skipping CDC PLACES (cdc_places.json not found)")
        return {}

    with open(places_path) as f:
        records = json.load(f)

    result = {}
    for rec in records:
        fips = rec.get("fips", "")
        if not fips or len(fips) != 5:
            continue

        entry = {
            "name": rec.get("name", ""),
            "state": rec.get("state", ""),
            "population": rec.get("population"),
            "latitude": rec.get("latitude"),
            "longitude": rec.get("longitude"),
            "uninsured_pct": _safe_float(rec.get("ACCESS2")),
            "no_checkup_pct": _safe_float(rec.get("CHECKUP")),
            "depression_pct": _safe_float(rec.get("DEPRESSION")),
            "diabetes_pct": _safe_float(rec.get("DIABETES")),
            "obesity_pct": _safe_float(rec.get("OBESITY")),
            "copd_pct": _safe_float(rec.get("COPD")),
            "mental_health_bad_pct": _safe_float(rec.get("MHLTH")),
            "physical_health_bad_pct": _safe_float(rec.get("PHLTH")),
            "mammography_pct": _safe_float(rec.get("MAMMOUSE")),
            "preventive_women_pct": _safe_float(rec.get("COREW")),
            "preventive_men_pct": _safe_float(rec.get("COREM")),
        }
        result[fips] = entry

    print(f"  CDC PLACES data for {len(result)} counties")
    return result


def clean_fqhc():
    """
    Process FQHC locations CSV. Aggregate to county level.
    Uses ZIP code to approximate county by extracting state FIPS from state abbreviation
    and using the site's ZIP prefix as a rough county proxy.
    Falls back to state-level aggregation if county mapping is unreliable.

    Returns dict keyed by 5-digit FIPS with fqhc_count.
    """
    print("Processing FQHC data...")
    fqhc_path = RAW_DIR / "fqhc_sites.csv"
    if not fqhc_path.exists():
        print("  Skipping FQHC (fqhc_sites.csv not found)")
        return {}

    # Build a ZIP-to-county lookup from the Census crosswalk if available
    # Otherwise, we aggregate by state and distribute proportionally
    zip_county_path = RAW_DIR / "zip_county_crosswalk.csv"

    zip_to_county = {}
    if zip_county_path.exists():
        try:
            with open(zip_county_path, "r", encoding="utf-8-sig") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    z = (row.get("ZIP") or row.get("zip") or "").strip().zfill(5)
                    c = (row.get("COUNTY") or row.get("county") or "").strip().zfill(5)
                    if z and c and len(c) == 5:
                        zip_to_county[z] = c
            print(f"  Loaded ZIP-to-county crosswalk: {len(zip_to_county)} entries")
        except Exception as e:
            print(f"  Warning: could not load ZIP crosswalk: {e}")

    # Read FQHC sites
    county_counts = defaultdict(int)
    unmapped = 0
    total_sites = 0

    try:
        with open(fqhc_path, "r", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            headers = reader.fieldnames or []

            # Find relevant columns
            zip_col = None
            state_col = None
            county_col = None
            lat_col = None
            lon_col = None

            for h in headers:
                hl = h.lower().strip()
                if "zip" in hl and "code" in hl and zip_col is None:
                    zip_col = h
                elif hl in ("zip", "site zip", "site zip code") and zip_col is None:
                    zip_col = h
                if "state" in hl and "abbr" in hl:
                    state_col = h
                elif hl in ("state", "site state abbreviation", "site state"):
                    if state_col is None:
                        state_col = h
                if "county" in hl and ("fips" in hl or "code" in hl):
                    county_col = h
                if "latitude" in hl or hl == "lat":
                    lat_col = h
                if "longitude" in hl or hl == "lon":
                    lon_col = h

            # Try finding zip and state more broadly
            if not zip_col:
                for h in headers:
                    if "zip" in h.lower():
                        zip_col = h
                        break
            if not state_col:
                for h in headers:
                    if "state" in h.lower() and "name" not in h.lower():
                        state_col = h
                        break

            for row in reader:
                total_sites += 1

                # Try direct county FIPS first
                if county_col:
                    county_fips = (row.get(county_col) or "").strip()
                    if county_fips:
                        try:
                            county_fips = str(int(float(county_fips))).zfill(5)
                            if len(county_fips) == 5:
                                county_counts[county_fips] += 1
                                continue
                        except (ValueError, TypeError):
                            pass

                # Try ZIP-to-county lookup
                if zip_col:
                    zipcode = (row.get(zip_col) or "").strip()
                    if zipcode:
                        zipcode = zipcode.split("-")[0].zfill(5)  # handle ZIP+4
                        if zipcode in zip_to_county:
                            county_counts[zip_to_county[zipcode]] += 1
                            continue

                # Fallback: use state FIPS + approximate from ZIP
                if state_col and zip_col:
                    state_abbr = (row.get(state_col) or "").strip().upper()
                    zipcode = (row.get(zip_col) or "").strip().split("-")[0].zfill(5)
                    state_fips_code = STATE_FIPS.get(state_abbr)
                    if state_fips_code and zipcode:
                        # Use state FIPS + "000" as a catch-all for unmapped
                        approx_fips = state_fips_code + "000"
                        county_counts[approx_fips] += 1
                        unmapped += 1
                        continue

                unmapped += 1

    except Exception as e:
        print(f"  Error reading FQHC CSV: {e}")
        return {}

    # Remove the "000" catch-all entries (state-level unmapped)
    cleaned = {}
    state_unmapped = 0
    for fips, count in county_counts.items():
        if fips.endswith("000"):
            state_unmapped += count
        else:
            cleaned[fips] = {"fqhc_count": count}

    print(f"  FQHC sites processed: {total_sites}")
    print(f"  Mapped to counties: {sum(c['fqhc_count'] for c in cleaned.values())}")
    print(f"  State-level only (unmapped to county): {state_unmapped}")
    print(f"  Counties with FQHCs: {len(cleaned)}")
    return cleaned


def clean_census():
    """
    Process Census ACS data from raw JSON.
    Returns dict keyed by 5-digit FIPS with demographic/economic measures.
    """
    print("Processing Census ACS data...")
    census_path = RAW_DIR / "census_acs.json"
    if not census_path.exists():
        print("  Skipping Census ACS (census_acs.json not found)")
        return {}

    with open(census_path) as f:
        raw = json.load(f)

    result = {}
    for fips, rec in raw.items():
        population = _safe_int(rec.get("population"))
        median_income = _safe_int(rec.get("median_income"))

        # Poverty rate
        pov_universe = _safe_int(rec.get("poverty_universe"))
        pov_below = _safe_int(rec.get("poverty_below"))
        poverty_rate = None
        if pov_universe and pov_universe > 0 and pov_below is not None:
            poverty_rate = round(pov_below / pov_universe * 100, 1)

        # Uninsured rate from B27001
        # Sum up all the "no insurance" age groups for both male and female
        no_ins_keys = [
            "male_19_25_no_ins", "male_26_34_no_ins", "male_35_44_no_ins",
            "male_45_54_no_ins", "male_55_64_no_ins", "male_65_74_no_ins",
            "male_75_plus_no_ins",
            "female_19_25_no_ins", "female_26_34_no_ins", "female_35_44_no_ins",
            "female_45_54_no_ins", "female_55_64_no_ins", "female_65_74_no_ins",
            "female_75_plus_no_ins",
        ]
        ins_universe = _safe_int(rec.get("insurance_universe"))
        no_ins_total = 0
        no_ins_valid = False
        for k in no_ins_keys:
            v = _safe_int(rec.get(k))
            if v is not None:
                no_ins_total += v
                no_ins_valid = True

        census_uninsured_pct = None
        if no_ins_valid and ins_universe and ins_universe > 0:
            census_uninsured_pct = round(no_ins_total / ins_universe * 100, 1)

        # Race breakdown
        race_total = _safe_int(rec.get("race_total"))
        race_white = _safe_int(rec.get("race_white"))
        race_black = _safe_int(rec.get("race_black"))
        hispanic = _safe_int(rec.get("hispanic"))

        pct_white = None
        pct_black = None
        pct_hispanic = None
        if race_total and race_total > 0:
            if race_white is not None:
                pct_white = round(race_white / race_total * 100, 1)
            if race_black is not None:
                pct_black = round(race_black / race_total * 100, 1)
        if population and population > 0 and hispanic is not None:
            pct_hispanic = round(hispanic / population * 100, 1)

        result[fips] = {
            "population": population,
            "median_income": median_income,
            "poverty_rate": poverty_rate,
            "census_uninsured_pct": census_uninsured_pct,
            "pct_white": pct_white,
            "pct_black": pct_black,
            "pct_hispanic": pct_hispanic,
        }

    print(f"  Census data for {len(result)} counties")
    return result


def merge_all(places, hpsa, fqhc, census):
    """
    Merge all data sources on 5-digit county FIPS.
    CDC PLACES is the backbone (provides county list, names, coords).
    """
    print("Merging all data sources...")
    counties = []

    for fips, cdc in places.items():
        if not cdc.get("name") or not cdc.get("state"):
            continue
        # Skip territories (FIPS starting with 72 = Puerto Rico, etc.)
        state_fips = fips[:2]
        if state_fips in ("72", "78", "66", "69", "60"):
            continue

        # Start with CDC data
        county = {
            "fips": fips,
            "name": cdc.get("name", ""),
            "state": cdc.get("state", ""),
            "latitude": cdc.get("latitude"),
            "longitude": cdc.get("longitude"),
            # CDC health measures
            "uninsured_pct": cdc.get("uninsured_pct"),
            "no_checkup_pct": cdc.get("no_checkup_pct"),
            "depression_pct": cdc.get("depression_pct"),
            "diabetes_pct": cdc.get("diabetes_pct"),
            "obesity_pct": cdc.get("obesity_pct"),
            "mental_health_bad_pct": cdc.get("mental_health_bad_pct"),
            "physical_health_bad_pct": cdc.get("physical_health_bad_pct"),
            "mammography_pct": cdc.get("mammography_pct"),
            "preventive_women_pct": cdc.get("preventive_women_pct"),
            "preventive_men_pct": cdc.get("preventive_men_pct"),
        }

        # Add HPSA data
        hpsa_data = hpsa.get(fips, {})
        county["hpsa_pc_count"] = hpsa_data.get("hpsa_pc_count", 0)
        county["hpsa_mh_count"] = hpsa_data.get("hpsa_mh_count", 0)
        county["hpsa_dh_count"] = hpsa_data.get("hpsa_dh_count", 0)
        county["hpsa_severity_avg"] = hpsa_data.get("hpsa_severity_avg", 0.0)

        # Add FQHC data
        fqhc_data = fqhc.get(fips, {})
        county["fqhc_count"] = fqhc_data.get("fqhc_count", 0)

        # Add Census data
        census_data = census.get(fips, {})
        county["population"] = census_data.get("population") or cdc.get("population")
        county["median_income"] = census_data.get("median_income")
        county["poverty_rate"] = census_data.get("poverty_rate")
        county["pct_white"] = census_data.get("pct_white")
        county["pct_black"] = census_data.get("pct_black")
        county["pct_hispanic"] = census_data.get("pct_hispanic")

        # Use Census uninsured if CDC value is missing
        if county["uninsured_pct"] is None:
            county["uninsured_pct"] = census_data.get("census_uninsured_pct")

        # Compute FQHC per 100K
        pop = county.get("population")
        fqhc_ct = county.get("fqhc_count", 0)
        if pop and pop > 0 and fqhc_ct > 0:
            county["fqhc_per_100k"] = round(fqhc_ct / (pop / 100000), 2)
        else:
            county["fqhc_per_100k"] = 0.0

        counties.append(county)

    # Sort by population descending
    counties.sort(key=lambda x: (x.get("population") or 0), reverse=True)

    print(f"  Merged counties: {len(counties)}")
    return counties


def save_processed(counties):
    """Save processed data to JSON."""
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    out_path = PROCESSED_DIR / "county_health_access.json"
    with open(out_path, "w") as f:
        json.dump(counties, f, indent=2)
    print(f"  Saved to {out_path.resolve()}")
    return out_path


def clean():
    """Run full cleaning pipeline."""
    print("=" * 60)
    print("Healthcare Access Gap Finder - Data Cleaning")
    print("=" * 60)

    hpsa = clean_hpsa()
    places = clean_cdc_places()
    fqhc = clean_fqhc()
    census = clean_census()

    if not places:
        print("\nERROR: No CDC PLACES data found. Run `python -m src.ingest` first.")
        return

    counties = merge_all(places, hpsa, fqhc, census)
    save_processed(counties)

    # Print summary stats
    with_hpsa = sum(1 for c in counties if c.get("hpsa_pc_count", 0) > 0)
    with_fqhc = sum(1 for c in counties if c.get("fqhc_count", 0) > 0)
    with_census = sum(1 for c in counties if c.get("median_income") is not None)

    print(f"\n{'=' * 60}")
    print("Cleaning Summary")
    print(f"{'=' * 60}")
    print(f"  Total counties: {len(counties)}")
    print(f"  Counties with HPSA data: {with_hpsa}")
    print(f"  Counties with FQHC data: {with_fqhc}")
    print(f"  Counties with Census data: {with_census}")
    print(f"\nRun `python -m src.seed` to load into DuckDB.")


if __name__ == "__main__":
    clean()
