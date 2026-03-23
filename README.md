# Healthcare Access Gap Finder

A county-level analysis tool that maps the gap between health needs and healthcare access infrastructure across every US county. Most health dashboards show outcomes (diabetes rates, life expectancy) without connecting them to the access infrastructure (or lack of it) driving those outcomes. This tool bridges that gap by combining shortage area designations, community health center locations, insurance coverage, and chronic disease prevalence into a single framework that identifies where intervention will have the most impact.

## What It Does

- Scores every US county on health **need** (disease burden, uninsured rate, preventive care gaps) and **access** (provider shortages, FQHCs, insurance coverage)
- Computes a **gap score** that reveals where needs outpace available resources
- Classifies counties into four quadrants: Well Served, At Risk, Strained, Critical Gap
- Provides county-level detail with rule-based intervention recommendations

## Why

Public health dashboards typically show what is happening (prevalence rates, mortality) but not why it is happening or where to intervene. By layering HRSA shortage data and FQHC locations on top of CDC health measures, this tool surfaces the counties where the structural gap between need and access is widest, making it actionable for health systems, policymakers, and community organizations.

## Stack

- **Python** - ETL pipeline
- **DuckDB** - Analytical database (percentile-rank scoring via SQL)
- **Streamlit + Plotly** - Interactive dashboard with choropleth mapping

## Data Sources

| Source | What It Provides | Link |
|--------|-----------------|------|
| HRSA HPSAs | Health Professional Shortage Area designations and severity scores | [data.hrsa.gov](https://data.hrsa.gov/topics/health-workforce/shortage-areas) |
| CDC PLACES | County-level chronic disease, prevention, and health behavior measures | [data.cdc.gov](https://data.cdc.gov/500-Cities-Places/PLACES-County-Data-GIS-Friendly-Format-2024-releas/swc5-untb) |
| HRSA FQHCs | Federally Qualified Health Center locations with coordinates | [data.hrsa.gov](https://data.hrsa.gov/topics/health-centers) |
| Census ACS | Population, median income, poverty rate, insurance coverage, race | [census.gov](https://data.census.gov/) |

## Run Locally

```bash
# Clone and setup
git clone https://github.com/sbc1-code/healthcare-access-gap.git
cd healthcare-access-gap
make setup
source .venv/bin/activate

# Set Census API key (free, get one at https://api.census.gov/data/key_signup.html)
export CENSUS_API_KEY=your_key_here

# Run ETL pipeline
make ingest      # Download all raw data
make clean-data  # Process and merge to county level
make seed        # Load into DuckDB and compute scores

# Launch dashboard
make dashboard
```

## Known Limitations

- FQHC-to-county mapping uses ZIP codes when direct county FIPS is unavailable, which can misassign clinics near county boundaries
- HPSA designations cover geographic areas that do not always align with county boundaries
- CDC PLACES uses model-based small area estimates, not direct measurements
- Access Score does not account for telehealth, private provider density, or hospital proximity
- Census ACS estimates carry sampling margins of error

## Future Expansion

- **CDC SVI** - Social Vulnerability Index for housing, transportation, language barriers
- **FCC Broadband** - Internet access data for telehealth readiness assessment
- **NPI Registry** - Provider density by specialty at the county level
- **AHRF** - Area Health Resources File for hospital beds and physicians per capita

## Author

**Sebastian Becerra** - [LinkedIn](https://www.linkedin.com/in/sebastianbecerra1/)
