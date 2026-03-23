"""Healthcare Access Gap Finder - Streamlit Dashboard"""

import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import json
from urllib.request import urlopen, Request
from src import analytics

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

st.set_page_config(
    page_title="Healthcare Access Gap Finder",
    page_icon="",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ---------------------------------------------------------------------------
# Theme constants
# ---------------------------------------------------------------------------

OLIVE = "#6B7A2F"
TERRACOTTA = "#B85042"
GREEN = "#4A7C59"
GOLD = "#D4A843"
ORANGE = "#D47A43"
BG = "#F5F4F0"
SURFACE = "#FFFFFF"
TEXT = "#1A1F1C"
MUTED = "#6B6B6B"
BORDER = "#E0DDD5"

QUADRANT_COLORS = {
    "well_served": GREEN,
    "at_risk": GOLD,
    "strained": ORANGE,
    "critical_gap": TERRACOTTA,
}

QUADRANT_LABELS = {
    "well_served": "Well Served",
    "at_risk": "At Risk",
    "strained": "Strained",
    "critical_gap": "Critical Gap",
}

QUADRANT_DESCRIPTIONS = {
    "well_served": "Low health need, good access. These counties have adequate provider coverage and lower disease burden.",
    "at_risk": "Low current need but limited access infrastructure. Could deteriorate without investment.",
    "strained": "High health need but some access in place. Resources are stretched thin.",
    "critical_gap": "High health need combined with poor access. Priority targets for intervention.",
}

PLOTLY_CONFIG = {"displayModeBar": False}

# ---------------------------------------------------------------------------
# CSS
# ---------------------------------------------------------------------------

st.markdown(f"""
<style>
    #MainMenu, footer, header {{visibility: hidden;}}

    .metric-card {{
        background: {SURFACE};
        border: 1px solid {BORDER};
        border-radius: 12px;
        padding: 20px;
        text-align: center;
    }}
    .metric-card .value {{
        font-size: 2rem;
        font-weight: 700;
        color: {TEXT};
        margin: 4px 0;
    }}
    .metric-card .label {{
        font-size: 0.85rem;
        color: {MUTED};
        text-transform: uppercase;
        letter-spacing: 0.05em;
    }}
    .quadrant-card {{
        background: {SURFACE};
        border: 1px solid {BORDER};
        border-radius: 12px;
        padding: 16px;
        margin-bottom: 8px;
    }}
    .quadrant-card .q-title {{
        font-weight: 600;
        font-size: 1rem;
        margin-bottom: 4px;
    }}
    .quadrant-card .q-count {{
        font-size: 1.6rem;
        font-weight: 700;
    }}
    .quadrant-card .q-desc {{
        font-size: 0.82rem;
        color: {MUTED};
        margin-top: 6px;
    }}
    .rec-card {{
        background: {SURFACE};
        border-left: 4px solid {OLIVE};
        border-radius: 0 8px 8px 0;
        padding: 12px 16px;
        margin-bottom: 10px;
    }}
    .rec-card .rec-title {{
        font-weight: 600;
        font-size: 0.95rem;
    }}
    .rec-card .rec-desc {{
        font-size: 0.85rem;
        color: {MUTED};
        margin-top: 4px;
    }}
    div[data-testid="stMetricValue"] {{
        font-size: 1.6rem;
    }}
</style>
""", unsafe_allow_html=True)


# ---------------------------------------------------------------------------
# Data loading (cached)
# ---------------------------------------------------------------------------

@st.cache_resource
def load_geojson():
    """Load county GeoJSON for choropleth mapping."""
    url = "https://raw.githubusercontent.com/plotly/datasets/master/geojson-counties-fips.json"
    req = Request(url, headers={"User-Agent": "HealthcareAccessGap/1.0"})
    with urlopen(req, timeout=60) as resp:
        return json.loads(resp.read().decode())


@st.cache_data(ttl=3600)
def load_overview():
    return analytics.get_overview_stats()


@st.cache_data(ttl=3600)
def load_all_counties():
    return analytics.get_all_counties()


@st.cache_data(ttl=3600)
def load_scatter():
    return analytics.get_scatter_data()


@st.cache_data(ttl=3600)
def load_quadrants():
    return analytics.get_quadrant_distribution()


@st.cache_data(ttl=3600)
def load_top_gap():
    return analytics.get_top_gap_counties(limit=10)


@st.cache_data(ttl=3600)
def load_national_avgs():
    return analytics.get_national_averages()


@st.cache_data(ttl=3600)
def load_border():
    return analytics.get_border_comparison()


@st.cache_data(ttl=3600)
def load_county_list():
    return analytics.get_county_list()


@st.cache_data(ttl=3600)
def load_state_summary():
    return analytics.get_state_summary()


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------

def metric_card(label, value, color=TEXT):
    """Render a styled metric card."""
    st.markdown(f"""
    <div class="metric-card">
        <div class="label">{label}</div>
        <div class="value" style="color: {color};">{value}</div>
    </div>
    """, unsafe_allow_html=True)


def quadrant_card(quadrant_key, count):
    """Render a quadrant summary card."""
    color = QUADRANT_COLORS.get(quadrant_key, MUTED)
    label = QUADRANT_LABELS.get(quadrant_key, quadrant_key)
    desc = QUADRANT_DESCRIPTIONS.get(quadrant_key, "")
    st.markdown(f"""
    <div class="quadrant-card">
        <div class="q-title" style="color: {color};">{label}</div>
        <div class="q-count" style="color: {color};">{count}</div>
        <div class="q-desc">{desc}</div>
    </div>
    """, unsafe_allow_html=True)


def recommendation_card(title, description):
    """Render an intervention recommendation card."""
    st.markdown(f"""
    <div class="rec-card">
        <div class="rec-title">{title}</div>
        <div class="rec-desc">{description}</div>
    </div>
    """, unsafe_allow_html=True)


def get_recommendations(county_row):
    """Generate rule-based intervention recommendations for a county."""
    recs = []
    uninsured = county_row.get("uninsured_pct") or 0
    fqhc = county_row.get("fqhc_per_100k") or 0
    mental_bad = county_row.get("mental_health_bad_pct") or 0
    hpsa_mh = county_row.get("hpsa_mh_count") or 0
    checkup = county_row.get("no_checkup_pct") or 0
    diabetes = county_row.get("diabetes_pct") or 0
    obesity = county_row.get("obesity_pct") or 0
    hpsa_sev = county_row.get("hpsa_severity_avg") or 0
    prev_men = county_row.get("preventive_men_pct") or 50
    prev_women = county_row.get("preventive_women_pct") or 50

    if uninsured > 15 and fqhc < 5:
        recs.append((
            "Expand community health center capacity or mobile health clinics",
            f"This county has {uninsured:.1f}% uninsured and only {fqhc:.1f} FQHCs per 100K. "
            "Increasing clinic capacity can provide primary care to uninsured residents."
        ))

    if mental_bad > 16 and hpsa_mh > 0:
        recs.append((
            "Invest in telehealth mental health services",
            f"Mental health distress rate is {mental_bad:.1f}% with {hpsa_mh} mental health "
            "shortage designations. Telehealth can bridge the provider gap."
        ))

    if checkup > 35 or (prev_men + prev_women) / 2 < 30:
        recs.append((
            "Community health worker outreach for preventive screenings",
            f"Only {100 - checkup:.1f}% had a recent checkup. Community health workers "
            "can connect residents with preventive care and reduce downstream costs."
        ))

    if diabetes > 12 and obesity > 35:
        recs.append((
            "Nutrition education and subsidized healthy food access",
            f"Diabetes prevalence is {diabetes:.1f}% and obesity is {obesity:.1f}%. "
            "Nutrition programs and food access initiatives can address root causes."
        ))

    if hpsa_sev > 15:
        recs.append((
            "Federal NHSC loan repayment to recruit providers",
            f"Average HPSA severity score is {hpsa_sev:.1f} (out of 25). "
            "National Health Service Corps loan repayment can attract providers to shortage areas."
        ))

    if not recs:
        recs.append((
            "Maintain current access infrastructure",
            "This county is performing relatively well. Continue monitoring key metrics "
            "and invest in preventive care to maintain gains."
        ))

    return recs


# ---------------------------------------------------------------------------
# Tab 1: Overview
# ---------------------------------------------------------------------------

def render_overview():
    st.markdown("## Healthcare Access Gap Finder")
    st.markdown(
        "Where are Americans not getting the care they need? "
        "This tool maps the gap between health needs and access infrastructure "
        "across every US county."
    )

    overview = load_overview()
    if overview.empty:
        st.error("No data available. Run the ETL pipeline first.")
        return

    row = overview.iloc[0]

    # KPI cards
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        metric_card("Counties Analyzed", f"{int(row['total_counties']):,}")
    with c2:
        metric_card("Avg Need Score", f"{row['avg_need_score']:.1f}", TERRACOTTA)
    with c3:
        metric_card("Avg Access Score", f"{row['avg_access_score']:.1f}", GREEN)
    with c4:
        metric_card("Critical Gap Counties", f"{int(row['critical_gap_count']):,}", TERRACOTTA)

    st.markdown("---")

    # Choropleth map
    st.markdown("### National Gap Map")
    st.caption("Counties colored by gap score. Green = low gap (good). Terracotta = high gap (needs attention).")

    all_counties = load_all_counties()
    try:
        geojson = load_geojson()

        fig = px.choropleth(
            all_counties,
            geojson=geojson,
            locations="fips",
            color="gap_score",
            color_continuous_scale=[
                [0, GREEN],
                [0.5, "#F5E6A3"],
                [1, TERRACOTTA],
            ],
            scope="usa",
            hover_name="name",
            hover_data={
                "fips": False,
                "state": True,
                "gap_score": ":.1f",
                "need_score": ":.1f",
                "access_score": ":.1f",
                "population": ":,.0f",
            },
            labels={
                "gap_score": "Gap Score",
                "need_score": "Need Score",
                "access_score": "Access Score",
                "population": "Population",
            },
        )
        fig.update_layout(
            margin=dict(l=0, r=0, t=0, b=0),
            geo=dict(
                bgcolor=BG,
                lakecolor=BG,
                landcolor="#E8E6E0",
            ),
            coloraxis_colorbar=dict(
                title="Gap Score",
                thickness=15,
                len=0.6,
            ),
            height=500,
        )
        st.plotly_chart(fig, use_container_width=True, config=PLOTLY_CONFIG)
    except Exception as e:
        st.warning(f"Could not load county map: {e}")
        st.info("The map requires an internet connection to download county boundaries.")

    st.markdown("---")

    # Quadrant summary cards
    st.markdown("### Quadrant Overview")
    quad_data = load_quadrants()
    quad_dict = {}
    if not quad_data.empty:
        for _, qr in quad_data.iterrows():
            quad_dict[qr["quadrant"]] = int(qr["count"])

    q1, q2, q3, q4 = st.columns(4)
    with q1:
        quadrant_card("well_served", quad_dict.get("well_served", 0))
    with q2:
        quadrant_card("at_risk", quad_dict.get("at_risk", 0))
    with q3:
        quadrant_card("strained", quad_dict.get("strained", 0))
    with q4:
        quadrant_card("critical_gap", quad_dict.get("critical_gap", 0))

    st.markdown("---")

    # Top 10 gap counties
    st.markdown("### Top 10 Highest Gap Counties")
    top_gap = load_top_gap()
    if not top_gap.empty:
        display_df = top_gap[["name", "state", "gap_score", "need_score", "access_score",
                              "quadrant", "uninsured_pct", "hpsa_severity_avg"]].copy()
        display_df.columns = ["County", "State", "Gap Score", "Need Score", "Access Score",
                              "Quadrant", "Uninsured %", "HPSA Severity"]
        st.dataframe(display_df, use_container_width=True, hide_index=True)


# ---------------------------------------------------------------------------
# Tab 2: Explore
# ---------------------------------------------------------------------------

def render_explore():
    st.markdown("## Explore Counties")
    st.markdown("Need Score (x-axis) vs Access Score (y-axis). Counties in the bottom-right are the highest priority.")

    scatter_df = load_scatter()
    if scatter_df.empty:
        st.warning("No data available.")
        return

    # Filters
    fc1, fc2 = st.columns(2)
    with fc1:
        states = sorted(scatter_df["state"].unique().tolist())
        selected_states = st.multiselect("Filter by State", states, default=[])
    with fc2:
        pop_min, pop_max = int(scatter_df["population"].min() or 0), int(scatter_df["population"].max() or 1000000)
        pop_range = st.slider(
            "Population Range",
            min_value=pop_min,
            max_value=min(pop_max, 2000000),
            value=(pop_min, min(pop_max, 2000000)),
            step=10000,
        )

    # Apply filters
    filtered = scatter_df.copy()
    if selected_states:
        filtered = filtered[filtered["state"].isin(selected_states)]
    filtered = filtered[
        (filtered["population"] >= pop_range[0]) &
        (filtered["population"] <= pop_range[1])
    ]

    # Add quadrant labels for legend
    filtered["Quadrant"] = filtered["quadrant"].map(QUADRANT_LABELS)

    # Scatter plot
    fig = px.scatter(
        filtered,
        x="need_score",
        y="access_score",
        color="Quadrant",
        color_discrete_map={v: QUADRANT_COLORS[k] for k, v in QUADRANT_LABELS.items()},
        size="population",
        size_max=20,
        hover_name="name",
        hover_data={
            "state": True,
            "need_score": ":.1f",
            "access_score": ":.1f",
            "gap_score": ":.1f",
            "population": ":,.0f",
            "Quadrant": False,
        },
        labels={
            "need_score": "Need Score (higher = worse health needs)",
            "access_score": "Access Score (higher = better access)",
            "gap_score": "Gap Score",
            "population": "Population",
        },
        opacity=0.7,
    )

    # Add quadrant divider lines
    fig.add_hline(y=50, line_dash="dash", line_color=MUTED, opacity=0.5)
    fig.add_vline(x=50, line_dash="dash", line_color=MUTED, opacity=0.5)

    # Quadrant labels
    fig.add_annotation(x=25, y=95, text="Well Served", showarrow=False,
                       font=dict(color=GREEN, size=12), opacity=0.7)
    fig.add_annotation(x=75, y=95, text="Strained", showarrow=False,
                       font=dict(color=ORANGE, size=12), opacity=0.7)
    fig.add_annotation(x=25, y=5, text="At Risk", showarrow=False,
                       font=dict(color=GOLD, size=12), opacity=0.7)
    fig.add_annotation(x=75, y=5, text="Critical Gap", showarrow=False,
                       font=dict(color=TERRACOTTA, size=12), opacity=0.7)

    fig.update_layout(
        height=600,
        margin=dict(l=40, r=20, t=20, b=40),
        plot_bgcolor=SURFACE,
        paper_bgcolor=BG,
        xaxis=dict(gridcolor=BORDER, range=[0, 100]),
        yaxis=dict(gridcolor=BORDER, range=[0, 100]),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=-0.15,
            xanchor="center",
            x=0.5,
        ),
    )
    st.plotly_chart(fig, use_container_width=True, config=PLOTLY_CONFIG)

    # Searchable dataframe
    st.markdown("### County Data")
    st.caption(f"Showing {len(filtered):,} counties")
    display_cols = ["fips", "name", "state", "population", "need_score",
                    "access_score", "gap_score", "quadrant"]
    display_df = filtered[display_cols].copy()
    display_df.columns = ["FIPS", "County", "State", "Population", "Need Score",
                          "Access Score", "Gap Score", "Quadrant"]
    st.dataframe(display_df, use_container_width=True, hide_index=True)


# ---------------------------------------------------------------------------
# Tab 3: Your County
# ---------------------------------------------------------------------------

def render_your_county():
    st.markdown("## Your County")
    st.markdown("Select a county to see its detailed health access profile.")

    county_list = load_county_list()
    if county_list.empty:
        st.warning("No data available.")
        return

    # Build search options
    options = {}
    for _, r in county_list.iterrows():
        label = f"{r['name']}, {r['state']}"
        options[label] = r["fips"]

    selected_label = st.selectbox(
        "Search for a county",
        options=[""] + sorted(options.keys()),
        index=0,
        placeholder="Type a county name...",
    )

    if not selected_label:
        st.info("Select a county above to see its profile.")
        return

    fips = options[selected_label]
    profile = analytics.get_county_health_profile(fips)
    if profile.empty:
        st.error("County data not found.")
        return

    c = profile.iloc[0].to_dict()
    natl = load_national_avgs()
    na = natl.iloc[0].to_dict() if not natl.empty else {}

    # County info card
    st.markdown(f"### {c.get('name', '')}, {c.get('state', '')}")
    ic1, ic2, ic3, ic4 = st.columns(4)
    with ic1:
        pop = c.get("population")
        st.metric("Population", f"{int(pop):,}" if pop else "N/A")
    with ic2:
        inc = c.get("median_income")
        st.metric("Median Income", f"${int(inc):,}" if inc else "N/A")
    with ic3:
        pov = c.get("poverty_rate")
        st.metric("Poverty Rate", f"{pov:.1f}%" if pov else "N/A")
    with ic4:
        quad = c.get("quadrant", "")
        st.metric("Quadrant", QUADRANT_LABELS.get(quad, quad))

    st.markdown("---")

    # Score cards
    sc1, sc2, sc3 = st.columns(3)
    with sc1:
        need = c.get("need_score", 0)
        avg_need = na.get("avg_need", 50)
        delta = round(need - avg_need, 1) if need and avg_need else None
        st.metric("Need Score", f"{need:.1f}" if need else "N/A",
                  delta=f"{delta:+.1f} vs avg" if delta is not None else None,
                  delta_color="inverse")
    with sc2:
        access = c.get("access_score", 0)
        avg_access = na.get("avg_access", 50)
        delta = round(access - avg_access, 1) if access and avg_access else None
        st.metric("Access Score", f"{access:.1f}" if access else "N/A",
                  delta=f"{delta:+.1f} vs avg" if delta is not None else None,
                  delta_color="normal")
    with sc3:
        gap = c.get("gap_score", 0)
        avg_gap = na.get("avg_gap", 0)
        delta = round(gap - avg_gap, 1) if gap is not None and avg_gap is not None else None
        st.metric("Gap Score", f"{gap:.1f}" if gap is not None else "N/A",
                  delta=f"{delta:+.1f} vs avg" if delta is not None else None,
                  delta_color="inverse")

    st.markdown("---")

    # Health profile bars
    st.markdown("#### Health Needs Profile")
    st.caption("County values compared to national averages (dashed line)")

    health_metrics = [
        ("Uninsured %", c.get("uninsured_pct"), na.get("avg_uninsured")),
        ("No Checkup %", c.get("no_checkup_pct"), na.get("avg_no_checkup")),
        ("Diabetes %", c.get("diabetes_pct"), na.get("avg_diabetes")),
        ("Obesity %", c.get("obesity_pct"), na.get("avg_obesity")),
        ("Mental Health Bad %", c.get("mental_health_bad_pct"), na.get("avg_mental_health_bad")),
        ("Depression %", c.get("depression_pct"), na.get("avg_depression")),
        ("Physical Health Bad %", c.get("physical_health_bad_pct"), na.get("avg_physical_health_bad")),
    ]

    h_labels = []
    h_values = []
    h_avgs = []
    for label, val, avg in health_metrics:
        if val is not None:
            h_labels.append(label)
            h_values.append(val)
            h_avgs.append(avg)

    if h_labels:
        fig_health = go.Figure()
        fig_health.add_trace(go.Bar(
            y=h_labels,
            x=h_values,
            orientation="h",
            marker_color=TERRACOTTA,
            opacity=0.8,
            name="County",
        ))
        # National average markers
        fig_health.add_trace(go.Scatter(
            y=h_labels,
            x=h_avgs,
            mode="markers",
            marker=dict(symbol="line-ns", size=20, line=dict(width=2, color=TEXT)),
            name="National Avg",
        ))
        fig_health.update_layout(
            height=max(250, len(h_labels) * 40),
            margin=dict(l=0, r=20, t=10, b=10),
            plot_bgcolor=SURFACE,
            paper_bgcolor=BG,
            xaxis=dict(gridcolor=BORDER, title="Percentage"),
            showlegend=True,
            legend=dict(orientation="h", yanchor="bottom", y=-0.3, xanchor="center", x=0.5),
        )
        st.plotly_chart(fig_health, use_container_width=True, config=PLOTLY_CONFIG)

    st.markdown("---")

    # Access profile bars
    st.markdown("#### Access Profile")

    access_metrics = [
        ("FQHCs per 100K", c.get("fqhc_per_100k"), na.get("avg_fqhc_per_100k")),
        ("Insurance Coverage %", 100 - (c.get("uninsured_pct") or 0), 100 - (na.get("avg_uninsured") or 0)),
    ]

    # HPSA severity inverted: lower is better for access
    hpsa_sev = c.get("hpsa_severity_avg")
    avg_hpsa = na.get("avg_hpsa_severity")
    if hpsa_sev is not None:
        # Show as "Access Score" where lower severity = better
        access_metrics.append(
            ("HPSA Severity (lower is better)", hpsa_sev, avg_hpsa)
        )

    a_labels = []
    a_values = []
    a_avgs = []
    for label, val, avg in access_metrics:
        if val is not None:
            a_labels.append(label)
            a_values.append(val)
            a_avgs.append(avg)

    if a_labels:
        fig_access = go.Figure()
        fig_access.add_trace(go.Bar(
            y=a_labels,
            x=a_values,
            orientation="h",
            marker_color=GREEN,
            opacity=0.8,
            name="County",
        ))
        fig_access.add_trace(go.Scatter(
            y=a_labels,
            x=a_avgs,
            mode="markers",
            marker=dict(symbol="line-ns", size=20, line=dict(width=2, color=TEXT)),
            name="National Avg",
        ))
        fig_access.update_layout(
            height=max(200, len(a_labels) * 50),
            margin=dict(l=0, r=20, t=10, b=10),
            plot_bgcolor=SURFACE,
            paper_bgcolor=BG,
            xaxis=dict(gridcolor=BORDER, title="Value"),
            showlegend=True,
            legend=dict(orientation="h", yanchor="bottom", y=-0.4, xanchor="center", x=0.5),
        )
        st.plotly_chart(fig_access, use_container_width=True, config=PLOTLY_CONFIG)

    st.markdown("---")

    # Intervention recommendations
    st.markdown("#### Recommended Interventions")
    recs = get_recommendations(c)
    for title, desc in recs:
        recommendation_card(title, desc)

    # Demographics
    st.markdown("---")
    st.markdown("#### Demographics")
    dc1, dc2, dc3 = st.columns(3)
    with dc1:
        pw = c.get("pct_white")
        st.metric("% White", f"{pw:.1f}%" if pw else "N/A")
    with dc2:
        pb = c.get("pct_black")
        st.metric("% Black", f"{pb:.1f}%" if pb else "N/A")
    with dc3:
        ph = c.get("pct_hispanic")
        st.metric("% Hispanic", f"{ph:.1f}%" if ph else "N/A")


# ---------------------------------------------------------------------------
# Tab 4: Under the Hood
# ---------------------------------------------------------------------------

def render_under_the_hood():
    st.markdown("## Under the Hood")
    st.markdown("Methodology, data sources, and analysis details.")

    # Methodology
    st.markdown("### Scoring Methodology")
    st.markdown("""
    Each county receives two composite scores based on relative ranking (percentile) across all US counties:

    **Need Score (0-100, higher = greater health burden)**
    - Uninsured rate: 20% weight
    - No recent checkup: 15%
    - Mental health distress: 15%
    - Diabetes prevalence: 15%
    - Obesity prevalence: 10%
    - Depression prevalence: 10%
    - Physical health distress: 10%
    - Lack of preventive care: 5%

    **Access Score (0-100, higher = better infrastructure)**
    - HPSA severity (inverted): 40% weight
    - FQHCs per 100K population: 30%
    - Insurance coverage: 30%

    **Gap Score** = Need Score minus Access Score. Positive values indicate needs outpacing available resources.
    """)

    st.markdown("---")

    # Data sources
    st.markdown("### Data Sources")
    st.markdown("""
    | Source | Description | Link |
    |--------|-------------|------|
    | HRSA HPSAs | Health Professional Shortage Areas (Primary Care, Mental Health, Dental) | [data.hrsa.gov](https://data.hrsa.gov/topics/health-workforce/shortage-areas) |
    | CDC PLACES | County-level chronic disease and prevention measures | [data.cdc.gov](https://data.cdc.gov/500-Cities-Places/PLACES-County-Data-GIS-Friendly-Format-2024-releas/swc5-untb) |
    | HRSA FQHCs | Federally Qualified Health Center locations | [data.hrsa.gov](https://data.hrsa.gov/topics/health-centers) |
    | Census ACS | American Community Survey 5-year estimates (population, income, poverty, insurance, race) | [census.gov](https://data.census.gov/) |
    """)

    st.markdown("---")

    # Quadrant definitions
    st.markdown("### Quadrant Definitions")
    quad_data = []
    for key in ["well_served", "at_risk", "strained", "critical_gap"]:
        quad_data.append({
            "Quadrant": QUADRANT_LABELS[key],
            "Need Score": "<= 50" if key in ("well_served", "at_risk") else "> 50",
            "Access Score": ">= 50" if key in ("well_served", "strained") else "< 50",
            "Description": QUADRANT_DESCRIPTIONS[key],
        })
    st.dataframe(pd.DataFrame(quad_data), use_container_width=True, hide_index=True)

    st.markdown("---")

    # Border comparison
    st.markdown("### Border vs Non-Border Counties")
    st.caption("US-Mexico border counties compared to all other counties.")
    border_df = load_border()
    if not border_df.empty:
        display_cols = ["category", "county_count", "avg_need", "avg_access", "avg_gap",
                        "avg_uninsured", "avg_hpsa_severity", "avg_fqhc_per_100k",
                        "avg_income", "avg_poverty"]
        display_names = ["Category", "Counties", "Avg Need", "Avg Access", "Avg Gap",
                         "Avg Uninsured %", "Avg HPSA Severity", "Avg FQHCs/100K",
                         "Avg Income", "Avg Poverty %"]
        border_display = border_df[display_cols].copy()
        border_display.columns = display_names
        st.dataframe(border_display, use_container_width=True, hide_index=True)

        # Bar chart comparison
        metrics_to_compare = ["avg_need", "avg_access", "avg_gap"]
        labels_map = {"avg_need": "Need Score", "avg_access": "Access Score", "avg_gap": "Gap Score"}

        fig_border = go.Figure()
        for _, row in border_df.iterrows():
            cat = row["category"]
            color = TERRACOTTA if cat == "Border" else OLIVE
            fig_border.add_trace(go.Bar(
                x=[labels_map[m] for m in metrics_to_compare],
                y=[row[m] for m in metrics_to_compare],
                name=cat,
                marker_color=color,
                opacity=0.85,
            ))
        fig_border.update_layout(
            barmode="group",
            height=350,
            margin=dict(l=40, r=20, t=20, b=40),
            plot_bgcolor=SURFACE,
            paper_bgcolor=BG,
            yaxis=dict(gridcolor=BORDER, title="Score"),
            legend=dict(orientation="h", yanchor="bottom", y=-0.2, xanchor="center", x=0.5),
        )
        st.plotly_chart(fig_border, use_container_width=True, config=PLOTLY_CONFIG)

    st.markdown("---")

    # State summary
    st.markdown("### State Summary")
    state_df = load_state_summary()
    if not state_df.empty:
        display_cols = ["state", "county_count", "avg_need", "avg_access", "avg_gap",
                        "critical_count", "avg_uninsured"]
        display_names = ["State", "Counties", "Avg Need", "Avg Access", "Avg Gap",
                         "Critical Gap", "Avg Uninsured %"]
        state_display = state_df[display_cols].copy()
        state_display.columns = display_names
        st.dataframe(state_display, use_container_width=True, hide_index=True)

    st.markdown("---")

    # Known limitations
    st.markdown("### Known Limitations")
    st.markdown("""
    - FQHC-to-county mapping uses ZIP codes when direct county FIPS is unavailable, which can introduce errors for ZIP codes that cross county boundaries.
    - HPSA designations cover geographic areas that may not align perfectly with county boundaries. County-level aggregation is approximate.
    - CDC PLACES data uses model-based estimates, not direct measurements.
    - Census ACS data is based on survey sampling with associated margins of error.
    - Access Score does not account for telehealth availability, private provider density, or hospital proximity.
    - Need Score does not include social determinants like housing, transportation, or food access.
    """)

    st.markdown("---")

    # Future expansion
    st.markdown("### Future Expansion")
    st.markdown("""
    Planned data integrations:
    - **CDC SVI (Social Vulnerability Index):** Housing, transportation, language barriers
    - **FCC Broadband Data:** Internet access for telehealth readiness
    - **NPI Registry:** Provider density by specialty at the county level
    - **AHRF (Area Health Resources File):** Hospital beds, physicians per capita
    """)

    st.markdown("---")
    st.caption("Built by Sebastian Becerra | Data from HRSA, CDC, Census Bureau")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

tab1, tab2, tab3, tab4 = st.tabs(["Overview", "Explore", "Your County", "Under the Hood"])

with tab1:
    render_overview()
with tab2:
    render_explore()
with tab3:
    render_your_county()
with tab4:
    render_under_the_hood()
