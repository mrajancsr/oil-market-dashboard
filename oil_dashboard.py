import streamlit as st
import pandas as pd
import plotly.express as px
from datetime import datetime


# --- Load Data ---
@st.cache_data
def load_data(file_name: str = "oil_historical_market_data.csv"):
    df = pd.read_csv(file_name, parse_dates=["Date"])
    df.set_index("Date", inplace=True)
    return df


# --- Helper Functions ---
def format_large_number(value):
    """Format large numbers into K (thousands) or M (millions) with units."""
    if value >= 1_000_000:
        return f"{value / 1_000_000:.2f} M BBL"
    elif value >= 1_000:
        return f"{value / 1_000:.0f} K BBL"
    else:
        return f"{value:.0f} BBL"


@st.cache_data
def convert_df_to_csv(df):
    return df.to_csv(index=True).encode("utf-8")


# --- Dashboard Layout ---
def create_dashboard():
    df = load_data()

    st.title("🛢️ Kempstar Crude Oil Fundamental Price Monitoring Dashboard")
    st.caption("Data sourced from Yahoo Finance & EIA | Developed by Rajan Subramanian")

    # --- Top Section: Key Metrics ---
    st.header("Market Overview")
    col1, col2, col3 = st.columns(3)
    col1.metric("WTI Price", f"${df['WTI'].iloc[-1]:.2f}")
    col2.metric("Brent Price", f"${df['Brent'].iloc[-1]:.2f}")
    spread = df["WTI-Brent Spread"].iloc[-1]
    col3.metric("WTI-Brent Spread", f"{spread:+.2f}")

    col1.metric(
        "Crude Inventory",
        format_large_number(df["Crude_Oil_Inventory"].iloc[-1]),
    )
    col2.metric(
        "Weekly Inventory Change",
        format_large_number(df["Weekly_Inventory_Change"].iloc[-1]),
    )
    col3.metric("OVX (Oil Volatility Index)", f"{df['OVX (Oil VIX)'].iloc[-1]:.2f}")

    st.markdown("---")

    # --- Date Range Filter ---
    st.subheader("Select Date Range for Analysis")
    date_range = st.slider(
        "Select Date Range",
        min_value=df.index.min().date(),
        max_value=df.index.max().date(),
        value=(df.index.min().date(), df.index.max().date()),
    )

    filtered_df = df.loc[date_range[0] : date_range[1]]

    st.markdown("---")

    # --- Price Trend Chart ---
    st.subheader("Crude Oil Price Trends")
    selected_prices = st.multiselect(
        "Select Price Series", ["WTI", "Brent"], default=["WTI", "Brent"]
    )

    if selected_prices:
        fig_prices = px.line(
            filtered_df,
            x=filtered_df.index,
            y=selected_prices,
            labels={"value": "Price (USD)", "variable": "Oil Type"},
            title="WTI & Brent Prices Over Time",
        )
        st.plotly_chart(fig_prices)

    # --- Volatility Chart ---
    st.subheader("Oil Volatility Index (OVX)")
    fig_ovx = px.line(
        filtered_df,
        x=filtered_df.index,
        y="OVX (Oil VIX)",
        labels={"OVX (Oil VIX)": "OVX (Volatility Index)"},
        title="Oil Market Volatility (OVX) Over Time",
    )
    st.plotly_chart(fig_ovx)

    # --- Inventory Change Chart ---
    st.subheader("Weekly Crude Oil Inventory Change")
    fig_inventory = px.line(
        filtered_df,
        x=filtered_df.index,
        y="Weekly_Inventory_Change",
        labels={"Weekly_Inventory_Change": "Inventory Change (BBL)"},
        title="Weekly Inventory Change Over Time",
    )
    st.plotly_chart(fig_inventory)

    # --- Inventory vs Price Scatter Plot ---
    st.subheader("Inventory Change vs WTI Weekly Price Change")
    if "WTI_Weekly_Price_Change" not in df.columns:
        df["WTI_Weekly_Price_Change"] = df["WTI"].pct_change(5) * 100

    filtered_df = df.loc[date_range[0] : date_range[1]]
    fig_scatter = px.scatter(
        filtered_df,
        x="Weekly_Inventory_Change",
        y="WTI_Weekly_Price_Change",
        title="Inventory Change vs WTI Weekly Price Change",
        labels={
            "Weekly_Inventory_Change": "Weekly Inventory Change (BBL)",
            "WTI_Weekly_Price_Change": "WTI Weekly Price Change (%)",
        },
        trendline="ols",
    )
    st.plotly_chart(fig_scatter)

    # --- Observations & Insights ---
    st.subheader("📝 Observations & Insights")
    st.markdown(
        """
    ### Price Trends
    - WTI and Brent prices started 2024 at approximately \\$70 and \\$75 per barrel.
    - Prices rallied to \\$85-\\$90 per barrel by May, driven by strong demand recovery, OPEC+ discipline, and geopolitical tensions.
    - Prices declined steadily into September, weighed by economic slowdown fears and rising US production.
    - A brief rally in October highlighted market sensitivity to seasonal demand and geopolitical shifts.
    - As of December, prices have reverted to around \\$70-\\$75 per barrel, indicating a broadly range-bound year.

    ### Inventory Insights
    - Early inventory draws in Q1 and Q2 supported rising prices.
    - Inventory builds during summer coincided with declining prices, reflecting higher production and weaker refinery demand.
    - Overall, inventory changes alone showed weak correlation with weekly price changes, emphasizing the importance of macroeconomic and geopolitical factors.

    ### Volatility Dynamics
    - OVX declined from 40 to around 25 by mid-year, reflecting reduced uncertainty.
    - Volatility spiked in September and October as prices rallied, driven by repositioning and renewed geopolitical risks.
    - By year-end, OVX settled near 25-30, indicating a balanced market outlook with moderate event risk.

    ### Key Takeaways
    - Prices were range-bound in 2024 with supply/demand factors balanced.
    - Inventory changes influenced short-term prices but were not the primary driver.
    - Combining inventory data with volatility, macro indicators, and positioning data is essential for robust price modeling.
    """
    )

    st.markdown("---")

    # --- Data Download Option ---
    st.subheader("📥 Download Filtered Data")
    csv = convert_df_to_csv(filtered_df)
    st.download_button(
        label="Download Filtered Data as CSV",
        data=csv,
        file_name="filtered_oil_data.csv",
        mime="text/csv",
    )

    # --- Data Preview ---
    st.subheader("🔍 Preview Latest Data")
    st.dataframe(filtered_df.tail(10))


if __name__ == "__main__":
    create_dashboard()
