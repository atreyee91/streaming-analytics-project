"""
Streaming Analytics Dashboard

Multi-page Streamlit dashboard that reads gold-layer Delta tables
(as Parquet) and visualises sales, engagement, and traffic metrics
with Plotly charts.

Run:  cd dashboards && ../venv/bin/streamlit run streamlit_app.py
"""

import os
import glob
from datetime import datetime

import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
GOLD_BASE = os.path.join(PROJECT_ROOT, "data", "gold")

SALES_PATH = os.path.join(GOLD_BASE, "sales_metrics")
ENGAGEMENT_PATH = os.path.join(GOLD_BASE, "user_engagement")
TRAFFIC_PATH = os.path.join(GOLD_BASE, "traffic_analysis")

st.set_page_config(
    page_title="Streaming Analytics Dashboard",
    page_icon="\u2b50",
    layout="wide",
)

# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def read_delta_as_pandas(path: str) -> pd.DataFrame:
    """Read a Delta table directory as pandas by globbing parquet files."""
    parquet_files = glob.glob(os.path.join(path, "**", "*.parquet"), recursive=True)
    if not parquet_files:
        return pd.DataFrame()
    dfs = [pd.read_parquet(f) for f in parquet_files]
    return pd.concat(dfs, ignore_index=True)


@st.cache_data(ttl=300)
def load_sales() -> pd.DataFrame:
    return read_delta_as_pandas(SALES_PATH)


@st.cache_data(ttl=300)
def load_engagement() -> pd.DataFrame:
    return read_delta_as_pandas(ENGAGEMENT_PATH)


@st.cache_data(ttl=300)
def load_traffic() -> pd.DataFrame:
    return read_delta_as_pandas(TRAFFIC_PATH)


# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------

def sidebar():
    st.sidebar.title("Navigation")
    page = st.sidebar.radio(
        "Go to",
        ["Overview", "Sales Analytics", "User Engagement", "Traffic Analysis"],
    )
    st.sidebar.markdown("---")
    st.sidebar.caption(f"Last refreshed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    if st.sidebar.button("Refresh Data"):
        st.cache_data.clear()
        st.rerun()
    return page


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def check_data_ready() -> bool:
    """Return True if gold tables exist, else show setup instructions."""
    for path, name in [(SALES_PATH, "sales_metrics"),
                       (ENGAGEMENT_PATH, "user_engagement"),
                       (TRAFFIC_PATH, "traffic_analysis")]:
        delta_log = os.path.join(path, "_delta_log")
        if not os.path.isdir(delta_log):
            st.error(f"Gold table **{name}** not found at `{path}`")
            st.info(
                "Run the gold aggregation pipeline first:\n\n"
                "```bash\n"
                "cd src && JAVA_HOME=/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home "
                "../venv/bin/python gold_aggregation.py\n"
                "```"
            )
            return False
    return True


def fmt_currency(val) -> str:
    return f"${val:,.2f}"


# ---------------------------------------------------------------------------
# Pages
# ---------------------------------------------------------------------------

def page_overview():
    st.title("Overview")
    sales = load_sales()
    engagement = load_engagement()

    if sales.empty or engagement.empty:
        st.warning("No data available.")
        return

    total_revenue = sales["total_revenue"].sum()
    total_orders = sales["total_orders"].sum()
    total_sessions = engagement["total_sessions"].sum()
    avg_conversion = engagement["conversion_rate_view_to_cart"].mean()

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total Revenue", fmt_currency(total_revenue))
    c2.metric("Total Orders", f"{int(total_orders):,}")
    c3.metric("Total Sessions", f"{int(total_sessions):,}")
    c4.metric("View-to-Cart Rate", f"{avg_conversion:.1f}%")

    st.markdown("---")
    st.subheader("Sales Summary")
    st.dataframe(
        sales.sort_values("total_revenue", ascending=False).reset_index(drop=True),
        use_container_width=True,
    )

    st.subheader("Engagement Summary")
    st.dataframe(engagement.reset_index(drop=True), use_container_width=True)


def page_sales():
    st.title("Sales Analytics")
    sales = load_sales()
    if sales.empty:
        st.warning("No sales data available.")
        return

    # Revenue by category — horizontal bar
    st.subheader("Revenue by Category")
    cat_revenue = (
        sales.groupby("category", as_index=False)["total_revenue"]
        .sum()
        .sort_values("total_revenue")
    )
    fig_bar = px.bar(
        cat_revenue, x="total_revenue", y="category",
        orientation="h",
        labels={"total_revenue": "Total Revenue ($)", "category": "Category"},
        text_auto="$.2f",
    )
    fig_bar.update_layout(yaxis=dict(categoryorder="total ascending"))
    st.plotly_chart(fig_bar, use_container_width=True)

    # Order metrics table
    st.subheader("Order Metrics")
    display_df = sales.copy()
    display_df["total_revenue"] = display_df["total_revenue"].apply(fmt_currency)
    display_df["avg_order_value"] = display_df["avg_order_value"].apply(fmt_currency)
    st.dataframe(display_df.reset_index(drop=True), use_container_width=True)

    # Revenue share donut
    st.subheader("Revenue Share")
    cat_share = sales.groupby("category", as_index=False)["total_revenue"].sum()
    fig_donut = px.pie(
        cat_share, values="total_revenue", names="category",
        hole=0.45,
    )
    fig_donut.update_traces(textinfo="percent+label")
    st.plotly_chart(fig_donut, use_container_width=True)


def page_engagement():
    st.title("User Engagement")
    engagement = load_engagement()
    if engagement.empty:
        st.warning("No engagement data available.")
        return

    # Session metrics cards
    row = engagement.iloc[0]
    c1, c2, c3 = st.columns(3)
    c1.metric("Total Sessions", f"{int(row['total_sessions']):,}")
    c2.metric("Total Users", f"{int(row['total_users']):,}")
    c3.metric("Avg Events / Session", f"{row['avg_events_per_session']:.1f}")

    st.markdown("---")

    # Conversion funnel
    st.subheader("Conversion Funnel")
    funnel_data = pd.DataFrame({
        "Stage": ["Page Views", "Add to Cart", "Purchases"],
        "Count": [
            int(row["total_page_views"]),
            int(row["total_add_to_carts"]),
            int(row["total_purchases"]),
        ],
    })
    fig_funnel = go.Figure(go.Funnel(
        y=funnel_data["Stage"],
        x=funnel_data["Count"],
        textinfo="value+percent initial",
    ))
    fig_funnel.update_layout(funnelmode="stack")
    st.plotly_chart(fig_funnel, use_container_width=True)

    # Conversion rate gauges
    st.subheader("Conversion Rates")
    g1, g2 = st.columns(2)
    with g1:
        fig_g1 = go.Figure(go.Indicator(
            mode="gauge+number",
            value=row["conversion_rate_view_to_cart"],
            title={"text": "View \u2192 Cart (%)"},
            gauge={"axis": {"range": [0, 100]}, "bar": {"color": "#636EFA"}},
        ))
        fig_g1.update_layout(height=300)
        st.plotly_chart(fig_g1, use_container_width=True)
    with g2:
        fig_g2 = go.Figure(go.Indicator(
            mode="gauge+number",
            value=row["conversion_rate_cart_to_purchase"],
            title={"text": "Cart \u2192 Purchase (%)"},
            gauge={"axis": {"range": [0, 100]}, "bar": {"color": "#EF553B"}},
        ))
        fig_g2.update_layout(height=300)
        st.plotly_chart(fig_g2, use_container_width=True)

    # Event type bar chart
    st.subheader("Event Counts")
    event_data = pd.DataFrame({
        "Event Type": ["Page Views", "Add to Cart", "Purchases"],
        "Count": [
            int(row["total_page_views"]),
            int(row["total_add_to_carts"]),
            int(row["total_purchases"]),
        ],
    })
    fig_events = px.bar(
        event_data, x="Event Type", y="Count",
        text_auto=True, color="Event Type",
    )
    st.plotly_chart(fig_events, use_container_width=True)


def page_traffic():
    st.title("Traffic Analysis")
    traffic = load_traffic()
    if traffic.empty:
        st.warning("No traffic data available.")
        return

    # Device type distribution — pie
    st.subheader("Device Type Distribution")
    device_counts = traffic.groupby("device_type", as_index=False)["event_count"].sum()
    fig_pie = px.pie(device_counts, values="event_count", names="device_type")
    fig_pie.update_traces(textinfo="percent+label")
    st.plotly_chart(fig_pie, use_container_width=True)

    # Hourly traffic pattern — line by device
    st.subheader("Hourly Traffic Pattern")
    hourly = (
        traffic.groupby(["event_hour", "device_type"], as_index=False)["event_count"]
        .sum()
        .sort_values("event_hour")
    )
    fig_line = px.line(
        hourly, x="event_hour", y="event_count", color="device_type",
        labels={"event_hour": "Hour of Day", "event_count": "Events"},
        markers=True,
    )
    st.plotly_chart(fig_line, use_container_width=True)

    # Bot vs legitimate — stacked bar
    st.subheader("Bot vs Legitimate Traffic")
    bot_data = (
        traffic.groupby("device_type", as_index=False)[["bot_events", "legitimate_events"]]
        .sum()
    )
    bot_melted = bot_data.melt(
        id_vars="device_type",
        value_vars=["bot_events", "legitimate_events"],
        var_name="Traffic Type",
        value_name="Events",
    )
    fig_stacked = px.bar(
        bot_melted, x="device_type", y="Events", color="Traffic Type",
        barmode="stack",
        labels={"device_type": "Device Type"},
    )
    st.plotly_chart(fig_stacked, use_container_width=True)

    # Traffic metrics table
    st.subheader("Traffic Metrics")
    st.dataframe(traffic.reset_index(drop=True), use_container_width=True)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    if not check_data_ready():
        return

    page = sidebar()

    if page == "Overview":
        page_overview()
    elif page == "Sales Analytics":
        page_sales()
    elif page == "User Engagement":
        page_engagement()
    elif page == "Traffic Analysis":
        page_traffic()


if __name__ == "__main__":
    main()
