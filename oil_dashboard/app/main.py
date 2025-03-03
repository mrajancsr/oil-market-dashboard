import os

import numpy as np
import pandas as pd
import plotly.graph_objects as go
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

    # --- Price and Moving Averages Chart ---
    st.subheader("WTI & Brent Price Trends with Moving Averages")

    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=filtered_df.index,
            y=filtered_df["WTI"],
            mode="lines",
            name="WTI",
            line=dict(color="blue"),
        )
    )
    fig.add_trace(
        go.Scatter(
            x=filtered_df.index,
            y=filtered_df["Brent"],
            mode="lines",
            name="Brent",
            line=dict(color="green"),
        )
    )
    fig.add_trace(
        go.Scatter(
            x=filtered_df.index,
            y=filtered_df["WTI_MA50"],
            mode="lines",
            name="WTI 50-Day MA",
            line=dict(color="red", dash="solid"),
        )
    )
    fig.add_trace(
        go.Scatter(
            x=filtered_df.index,
            y=filtered_df["WTI_MA200"],
            mode="lines",
            name="WTI 200-Day MA",
            line=dict(color="black", dash="dash"),
        )
    )
    fig.update_layout(
        title="WTI & Brent Prices with Moving Averages",
        xaxis_title="Date",
        yaxis_title="Price (USD)",
        template="plotly_white",
    )

    st.plotly_chart(fig, use_container_width=True)

    # --- OVX Chart ---
    st.subheader("Oil Volatility Index (OVX)")
    fig_ovx = go.Figure()
    fig_ovx.add_trace(
        go.Scatter(
            x=filtered_df.index,
            y=filtered_df["OVX"],
            mode="lines",
            name="OVX",
            line=dict(color="purple"),
        )
    )
    fig_ovx.update_layout(
        title="Oil Volatility Index (OVX)",
        xaxis_title="Date",
        yaxis_title="Volatility Index",
        template="plotly_white",
    )

    st.plotly_chart(fig_ovx, use_container_width=True)

    st.markdown("---")

    # --- Inventory Change Chart ---
    st.subheader("Weekly Crude Oil Inventory Change")
    fig_inv = go.Figure()
    fig_inv.add_trace(
        go.Scatter(
            x=filtered_df.index,
            y=filtered_df["Weekly Inventory Change"],
            mode="lines",
            name="Weekly Inventory Change",
            line=dict(color="teal"),
        )
    )
    fig_inv.update_layout(
        title="Weekly Inventory Change",
        xaxis_title="Date",
        yaxis_title="Change (BBL)",
        template="plotly_white",
    )

    st.plotly_chart(fig_inv, use_container_width=True)

    # --- Inventory Change vs WTI Weekly Price Change Scatter Plot ---
    st.subheader("Inventory Change vs WTI Weekly Price Change")

    fig_scatter = go.Figure(
        data=go.Scatter(
            x=filtered_df["Weekly Inventory Change"],
            y=filtered_df["WTI Weekly Price Change"],
            mode="markers",
            marker=dict(size=8, opacity=0.7),
            name="Data Points",
        )
    )

    fig_scatter.update_layout(
        title="Inventory Change vs WTI Weekly Price Change",
        xaxis_title="Inventory Change (BBL)",
        yaxis_title="Weekly Price Change (%)",
        template="plotly_white",
    )

    st.plotly_chart(fig_scatter, use_container_width=True)

    st.markdown("---")

    # --- Technical Indicators Section ---
    st.subheader("Technical Indicators")

    with st.expander("Show Technical Indicators (MA, Bollinger Bands, RSI, MACD)"):
        # Bollinger Bands
        st.write("### Bollinger Bands")
        fig_bb = go.Figure()
        fig_bb.add_trace(
            go.Scatter(
                x=filtered_df.index,
                y=filtered_df["WTI"],
                mode="lines",
                name="WTI",
                line=dict(color="blue"),
            )
        )
        fig_bb.add_trace(
            go.Scatter(
                x=filtered_df.index,
                y=filtered_df["WTI_BB_Upper"],
                mode="lines",
                name="Upper Band",
                line=dict(color="gray"),
            )
        )
        fig_bb.add_trace(
            go.Scatter(
                x=filtered_df.index,
                y=filtered_df["WTI_BB_Lower"],
                mode="lines",
                name="Lower Band",
                line=dict(color="gray"),
            )
        )
        fig_bb.update_layout(title="WTI with Bollinger Bands", template="plotly_white")
        st.plotly_chart(fig_bb, use_container_width=True)

        # RSI
        st.write("### Relative Strength Index (RSI)")
        fig_rsi = go.Figure()
        fig_rsi.add_trace(
            go.Scatter(
                x=filtered_df.index,
                y=filtered_df["WTI_RSI"],
                mode="lines",
                name="RSI",
                line=dict(color="orange"),
            )
        )
        fig_rsi.update_layout(
            title="WTI RSI (14-day)",
            yaxis=dict(range=[0, 100]),
            template="plotly_white",
        )
        st.plotly_chart(fig_rsi, use_container_width=True)

        # MACD
        st.write("### MACD Indicator")
        fig_macd = go.Figure()
        fig_macd.add_trace(
            go.Scatter(
                x=filtered_df.index,
                y=filtered_df["WTI_MACD"],
                mode="lines",
                name="MACD",
                line=dict(color="green"),
            )
        )
        fig_macd.add_trace(
            go.Scatter(
                x=filtered_df.index,
                y=filtered_df["WTI_MACD_Signal"],
                mode="lines",
                name="Signal Line",
                line=dict(color="red"),
            )
        )
        fig_macd.update_layout(title="MACD & Signal Line", template="plotly_white")
        st.plotly_chart(fig_macd, use_container_width=True)

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
