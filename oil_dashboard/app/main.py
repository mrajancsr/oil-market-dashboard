import os

import numpy as np
import pandas as pd
import plotly.express as px
import streamlit as st


# --- Load Data ---
@st.cache_data
def load_data(file_name: str = "oil_market_data.csv"):
    current_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.abspath(os.path.join(current_dir, "../../"))
    data_file = os.path.join(project_root, "data", file_name)
    df = pd.read_csv(data_file, parse_dates=["Date"])
    df.set_index("Date", inplace=True)
    return df


# --- Helper Functions ---
def format_large_number(value):
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

    # --- Key Metrics Section ---
    st.header("Market Overview")
    col1, col2, col3 = st.columns(3)
    col1.metric("WTI Price", f"${df['WTI'].iloc[-1]:.2f}")
    col2.metric("Brent Price", f"${df['Brent'].iloc[-1]:.2f}")
    spread = df["WTI-Brent Spread"].iloc[-1]
    col3.metric("WTI-Brent Spread", f"{spread:+.2f}")

    col1.metric(
        "Crude Inventory", format_large_number(df["Crude Oil Inventory"].iloc[-1])
    )
    col2.metric(
        "Weekly Inventory Change",
        format_large_number(df["Weekly Inventory Change"].iloc[-1]),
    )
    col3.metric("OVX (Oil Volatility Index)", f"{df['OVX'].iloc[-1]:.2f}")

    st.markdown("---")

    # --- Date Range Selector ---
    st.subheader("Select Date Range for Analysis")
    date_range = st.slider(
        "Select Date Range",
        min_value=df.index.min().date(),
        max_value=df.index.max().date(),
        value=(df.index.min().date(), df.index.max().date()),
    )

    filtered_df = df.loc[date_range[0] : date_range[1]]

    st.markdown("---")

    # --- Price and Volatility Charts (Full Width) ---
    st.subheader("Price & Volatility Trends")

    st.plotly_chart(
        px.line(
            filtered_df,
            x=filtered_df.index,
            y=["WTI", "Brent"],
            labels={"value": "Price (USD)", "variable": "Oil Type"},
            title="WTI & Brent Prices Over Time",
        ),
        use_container_width=True,
    )

    st.plotly_chart(
        px.line(
            filtered_df,
            x=filtered_df.index,
            y="OVX",
            labels={"OVX": "OVX (Volatility Index)"},
            title="Oil Volatility Index (OVX) Over Time",
        ),
        use_container_width=True,
    )

    st.markdown("---")

    # --- Inventory Chart ---
    st.subheader("Weekly Crude Oil Inventory Change")
    st.plotly_chart(
        px.line(
            filtered_df,
            x=filtered_df.index,
            y="Weekly Inventory Change",
            labels={"Weekly Inventory Change": "Inventory Change (BBL)"},
            title="Weekly Inventory Change Over Time",
        ),
        use_container_width=True,
    )

    # --- Inventory Change vs WTI Weekly Price Change (Scatter Plot) ---
    if "WTI Weekly Price Change" not in df.columns:
        df["WTI Weekly Price Change"] = df["WTI"].pct_change(5) * 100

    filtered_df = df.loc[date_range[0] : date_range[1]]
    st.subheader("Inventory Change vs WTI Weekly Price Change")
    st.plotly_chart(
        px.scatter(
            filtered_df,
            x="Weekly Inventory Change",
            y="WTI Weekly Price Change",
            title="Inventory Change vs WTI Weekly Price Change",
            labels={
                "Weekly Inventory Change": "Inventory Change (BBL)",
                "WTI Weekly Price Change": "Weekly Price Change (%)",
            },
            trendline="ols",
        ),
        use_container_width=True,
    )

    st.markdown("---")

    # --- Technical Indicators Section ---
    st.subheader("Technical Indicators")

    with st.expander("Show Technical Indicators (MA, Bollinger Bands, RSI, MACD)"):
        # Moving Averages
        st.write("### WTI Price with 50-Day & 200-Day Moving Averages")
        st.plotly_chart(
            px.line(
                filtered_df,
                x=filtered_df.index,
                y=["WTI", "WTI_MA50", "WTI_MA200"],
                labels={"value": "Price (USD)", "variable": "Indicator"},
                title="WTI with Moving Averages",
            ),
            use_container_width=True,
        )

        # Bollinger Bands
        st.write("### Bollinger Bands")
        st.plotly_chart(
            px.line(
                filtered_df,
                x=filtered_df.index,
                y=["WTI", "WTI_BB_Upper", "WTI_BB_Lower", "WTI"],
                labels={"value": "Price (USD)", "variable": "Indicator"},
                title="WTI with Bollinger Bands",
            ),
            use_container_width=True,
        )

        # RSI
        st.write("### Relative Strength Index (RSI)")
        st.plotly_chart(
            px.line(
                filtered_df, x=filtered_df.index, y="WTI_RSI", title="WTI RSI (14-day)"
            ),
            use_container_width=True,
        )

        # MACD
        st.write("### MACD Indicator")
        st.plotly_chart(
            px.line(
                filtered_df,
                x=filtered_df.index,
                y=["WTI_MACD", "WTI_MACD_Signal"],
                labels={"value": "Value", "variable": "Indicator"},
                title="WTI MACD (12-26-9)",
            ),
            use_container_width=True,
        )

    st.markdown("---")

    # --- Observations & Insights ---
    st.subheader("📝 Observations & Insights")
    st.markdown(
        """
        ### Price Trends
        - WTI and Brent prices fluctuated significantly due to macroeconomic and geopolitical factors.
        - Inventory draws and builds influenced short-term pricing, but broader trends tied to global demand and OPEC actions.

        ### Inventory Insights
        - Inventory builds tend to cap price rallies, especially when refinery throughput drops.
        - Large inventory draws often coincide with rising OVX, indicating higher uncertainty.

        ### Volatility Dynamics
        - OVX spikes are often linked to geopolitical risk events or sudden shifts in OPEC policy.

        ### Technical Takeaways
        - RSI over 70 often preceded pullbacks.
        - MACD crossovers caught some trends but struggled during sideways periods.
        """
    )

    st.markdown("---")

    # --- Data Download Options ---
    st.subheader("📥 Download Data")

    csv_filtered = convert_df_to_csv(filtered_df)
    st.download_button(
        "Download Filtered Data as CSV",
        csv_filtered,
        "filtered_oil_data.csv",
        "text/csv",
    )

    csv_full = convert_df_to_csv(df)
    st.download_button(
        "Download Full Data as CSV", csv_full, "full_oil_data.csv", "text/csv"
    )

    # --- Data Preview ---
    st.subheader("🔍 Preview Latest Data")
    st.dataframe(filtered_df.tail(10))


if __name__ == "__main__":
    create_dashboard()
