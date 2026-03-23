"""
Download all raw data for Healthcare Access Gap Finder.

Sources:
- HRSA HPSAs: Primary Care, Mental Health, Dental shortage area CSVs
- CDC PLACES: County-level health measures via Socrata JSON API
- HRSA FQHC: Community health center locations CSV
- Census ACS: Population, income, poverty, insurance, race

Usage:
    python -m src.ingest
"""

import csv
import io
import json
import os
import sys
import time
from pathlib import Path
from urllib.request import urlopen, Request
from urllib.error import URLError

RAW_DIR = Path(__file__).parent.parent / "data" / "raw"

HPSA_URLS = {
    "hpsa_pc": "https://data.hrsa.gov/DataDownload/DD_Files/BCD_HPSA_FCT_DET_PC.csv",
    "hpsa_mh": "https://data.hrsa.gov/DataDownload/DD_Files/BCD_HPSA_FCT_DET_MH.csv",
    "hpsa_dh": "https://data.hrsa.gov/DataDownload/DD_Files/BCD_HPSA_FCT_DET_DH.csv",
}

FQHC_URL = "https://data.hrsa.gov/DataDownload/DD_Files/Health_Center_Service_Delivery_and_LookAlike_Sites.csv"

# CDC PLACES measures we need
PLACES_MEASURES = [
    "ACCESS2",
    "CHECKUP",
    "DEPRESSION",
    "DIABETES",
    "OBESITY",
    "COPD",
    "MHLTH",
    "PHLTH",
    "MAMMOUSE",
    "COREW",
    "COREM",
]

# Census ACS variables
CENSUS_VARS = {
    "B01003_001E": "population",
    "B19013_001E": "median_income",
    "B17001_001E": "poverty_universe",
    "B17001_002E": "poverty_below",
    "B27001_001E": "insurance_universe",
    "B27001_005E": "male_19_25_no_ins",
    "B27001_008E": "male_26_34_no_ins",
    "B27001_011E": "male_35_44_no_ins",
    "B27001_014E": "male_45_54_no_ins",
    "B27001_017E": "male_55_64_no_ins",
    "B27001_020E": "male_65_74_no_ins",
    "B27001_023E": "male_75_plus_no_ins",
    "B27001_033E": "female_19_25_no_ins",
    "B27001_036E": "female_26_34_no_ins",
    "B27001_039E": "female_35_44_no_ins",
    "B27001_042E": "female_45_54_no_ins",
    "B27001_045E": "female_55_64_no_ins",
    "B27001_048E": "female_65_74_no_ins",
    "B27001_051E": "female_75_plus_no_ins",
    "B02001_001E": "race_total",
    "B02001_002E": "race_white",
    "B02001_003E": "race_black",
    "B03001_003E": "hispanic",
}


def fetch_json(url, label=""):
    """Fetch JSON from URL with retries."""
    for attempt in range(3):
        try:
            req = Request(url, headers={"User-Agent": "HealthcareAccessGap/1.0"})
            with urlopen(req, timeout=120) as resp:
                return json.loads(resp.read().decode())
        except Exception as e:
            print(f"  Retry {attempt + 1}/3 for {label}: {e}")
            time.sleep(2)
    print(f"  FAILED: {label}")
    return None


def download_csv(url, filename, label=""):
    """Download a CSV file to data/raw/."""
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    out_path = RAW_DIR / filename

    print(f"Downloading {label}...")
    for attempt in range(3):
        try:
            req = Request(url, headers={"User-Agent": "HealthcareAccessGap/1.0"})
            with urlopen(req, timeout=300) as resp:
                data = resp.read()
            with open(out_path, "wb") as f:
                f.write(data)
            size_mb = len(data) / (1024 * 1024)
            print(f"  Saved {filename} ({size_mb:.1f} MB)")
            return out_path
        except Exception as e:
            print(f"  Retry {attempt + 1}/3 for {label}: {e}")
            time.sleep(3)

    print(f"  FAILED to download {label}. Skipping.")
    return None


def fetch_hpsa_csvs():
    """Download all three HPSA CSVs."""
    results = {}
    for key, url in HPSA_URLS.items():
        label = key.upper().replace("_", " ")
        path = download_csv(url, f"{key}.csv", label)
        if path:
            # Count rows
            try:
                with open(path, "r", encoding="utf-8-sig") as f:
                    reader = csv.reader(f)
                    header = next(reader)
                    count = sum(1 for _ in reader)
                print(f"  {label}: {count} rows, {len(header)} columns")
                results[key] = path
            except Exception as e:
                print(f"  Warning: could not count rows for {label}: {e}")
                results[key] = path
    return results


def fetch_cdc_places():
    """Fetch CDC PLACES county-level data via Socrata JSON API."""
    print("Fetching CDC PLACES county data...")
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    all_data = {}

    for measure_id in PLACES_MEASURES:
        url = (
            f"https://data.cdc.gov/resource/swc5-untb.json"
            f"?measureid={measure_id}&datavaluetypeid=CrdPrv&$limit=50000"
            f"&$select=locationid,locationname,stateabbr,data_value,totalpopulation,geolocation"
        )
        data = fetch_json(url, f"PLACES {measure_id}")
        if not data:
            continue

        count = 0
        for row in data:
            fips = row.get("locationid", "")
            if not fips or not row.get("data_value"):
                continue

            if fips not in all_data:
                geo = row.get("geolocation", {})
                coords = geo.get("coordinates", [None, None]) if isinstance(geo, dict) else [None, None]
                all_data[fips] = {
                    "fips": fips,
                    "name": row.get("locationname", ""),
                    "state": row.get("stateabbr", ""),
                    "population": int(row.get("totalpopulation", 0) or 0),
                    "longitude": coords[0] if coords else None,
                    "latitude": coords[1] if coords else None,
                }
            all_data[fips][measure_id] = float(row["data_value"])
            count += 1

        print(f"  {measure_id}: {count} counties")
        time.sleep(0.5)

    # Save to raw
    out_path = RAW_DIR / "cdc_places.json"
    with open(out_path, "w") as f:
        json.dump(list(all_data.values()), f, indent=2)

    print(f"  Total counties with CDC data: {len(all_data)}")
    return all_data


def fetch_fqhc_csv():
    """Download FQHC locations CSV."""
    path = download_csv(FQHC_URL, "fqhc_sites.csv", "HRSA FQHC Sites")
    if path:
        try:
            with open(path, "r", encoding="utf-8-sig") as f:
                reader = csv.reader(f)
                header = next(reader)
                count = sum(1 for _ in reader)
            print(f"  FQHC sites: {count} rows, {len(header)} columns")
        except Exception as e:
            print(f"  Warning: could not count FQHC rows: {e}")
    return path


def fetch_census_acs(api_key=None):
    """Fetch Census ACS 5-year county data."""
    if not api_key:
        print("Skipping Census ACS (no API key). Set CENSUS_API_KEY env var.")
        return {}

    print("Fetching Census ACS data...")
    var_list = ",".join(["NAME"] + list(CENSUS_VARS.keys()))
    url = (
        f"https://api.census.gov/data/2023/acs/acs5"
        f"?get={var_list}"
        f"&for=county:*&key={api_key}"
    )

    try:
        req = Request(url, headers={"User-Agent": "HealthcareAccessGap/1.0"})
        with urlopen(req, timeout=120) as resp:
            data = json.loads(resp.read().decode())

        headers = data[0]
        census_data = {}

        for row in data[1:]:
            state_fips = row[headers.index("state")]
            county_fips = row[headers.index("county")]
            fips = state_fips + county_fips

            entry = {"fips": fips, "name": row[headers.index("NAME")]}

            for var_code, field_name in CENSUS_VARS.items():
                val = row[headers.index(var_code)]
                if val and val not in ("-666666666", "-999999999", "null", "None"):
                    try:
                        entry[field_name] = int(val)
                    except ValueError:
                        try:
                            entry[field_name] = float(val)
                        except ValueError:
                            pass

            census_data[fips] = entry

        # Save raw
        RAW_DIR.mkdir(parents=True, exist_ok=True)
        out_path = RAW_DIR / "census_acs.json"
        with open(out_path, "w") as f:
            json.dump(census_data, f, indent=2)

        print(f"  Census data for {len(census_data)} counties")
        return census_data

    except Exception as e:
        print(f"  FAILED to fetch Census data: {e}")
        return {}


def ingest():
    """Run full ingestion pipeline."""
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    api_key = os.environ.get("CENSUS_API_KEY")

    print("=" * 60)
    print("Healthcare Access Gap Finder - Data Ingestion")
    print("=" * 60)

    # 1. HPSA CSVs
    hpsa_paths = fetch_hpsa_csvs()

    # 2. CDC PLACES
    places = fetch_cdc_places()

    # 3. FQHC
    fqhc_path = fetch_fqhc_csv()

    # 4. Census
    census = fetch_census_acs(api_key)

    # Summary
    print("\n" + "=" * 60)
    print("Ingestion Summary")
    print("=" * 60)
    print(f"  HPSA CSVs downloaded: {len(hpsa_paths)}/3")
    print(f"  CDC PLACES counties: {len(places)}")
    print(f"  FQHC CSV: {'Yes' if fqhc_path else 'No'}")
    print(f"  Census ACS counties: {len(census)}")
    print(f"\nRaw data saved to: {RAW_DIR.resolve()}")
    print("Run `python -m src.clean` to process the data.")


if __name__ == "__main__":
    ingest()
