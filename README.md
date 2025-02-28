# Oil Market Dashboard

## Overview
This repository contains an interactive **Crude Oil Fundamental Price Monitoring Dashboard** built using **Python** and **Streamlit**. The dashboard integrates multiple data sources to track and visualize key fundamental indicators affecting the crude oil market, including:

- **WTI and Brent crude oil prices** (sourced from Yahoo Finance)
- **Crude oil inventory levels and weekly inventory changes** (sourced from EIA API v2)
- **Oil Volatility Index (OVX)** to track implied volatility in crude oil markets

## Key Features
- 📊 **Interactive Time-Series Charts** for WTI, Brent, and OVX.
- 📉 **WTI-Brent Spread Monitoring** to track price differentials between key benchmarks.
- 🛢️ **Inventory Analysis** including current levels and weekly changes.
- 🔗 **Correlation Analysis** between weekly price changes and inventory changes.
- 🔎 **Observations & Insights Section** to highlight key trends and market dynamics.
- 📅 **Custom Date Range Selection** for flexible exploration.

---

## Data Sources
- **Yahoo Finance** (WTI, Brent, OVX prices)
- **U.S. Energy Information Administration (EIA)** (Crude oil inventory data)

---

## Insights
### Price Trends - 2024
- **WTI and Brent started the year at $70 and $75 per barrel, respectively.**
- Prices **rallied through May**, peaking at **$85 (WTI) and $90 (Brent)**, driven by:
    - Stronger-than-expected global demand recovery.
    - **OPEC+ production discipline**, limiting supply.
    - Geopolitical supply concerns (Middle East and Russia).
- Prices **declined steadily from June to September**, reflecting:
    - Concerns over **slowing economic growth (China, global tightening cycles)**.
    - **Rising US crude production**, particularly from shale.
    - Higher-than-expected inventory builds.
- A brief **price rebound occurred in October** ahead of winter demand.
- As of **year-end**, prices stabilized around **$70 and $75**, reflecting **range-bound market conditions**.

---

### Crude Oil Inventory Dynamics
- **Inventory levels fluctuated modestly**, with **summer builds aligning with price declines**.
- This highlights how **inventory builds signal weaker short-term demand** or stronger-than-expected supply.
- The correlation between **weekly inventory changes and price changes was weak**, underscoring:
    - Price discovery depends on **macro sentiment, geopolitics, and global demand forecasts**.
- However, **unexpected inventory shocks** (especially during peak demand periods) still triggered sharper price responses.

---

### Oil Volatility Index (OVX) Behavior
- **OVX started the year elevated at 40**, reflecting macro and geopolitical uncertainties.
- Volatility **declined into mid-year** (~25 in July) as supply/demand balances stabilized.
- **OVX spiked sharply in September-October** alongside price rebounds, indicating:
    - Speculative positioning.
    - Repricing of geopolitical risks.
- Into year-end, **OVX settled back into historical ranges (~25)**, reflecting a more balanced risk outlook.

---

## Motivation
This project was developed as part of my **quantitative commodities research** initiative to enhance my technical and market analysis capabilities. The goal was to:

- Combine **real-world commodity data** with **interactive data visualization**.
- Build a tool that can support **quantitative research and trading insights generation**.
- Strengthen my **Python, Streamlit, and data engineering skills** while deepening my understanding of **crude oil market dynamics**.

---

## Future Enhancements
- 📡 **Add live data streaming capabilities** to turn this into a real-time market monitoring tool.
- 📊 **Expand to natural gas, refined products, and broader commodity coverage**.
- 🧠 **Incorporate predictive modeling techniques (ARIMA, XGBoost, LSTM)** to forecast prices and inventory levels.
- 🌐 **Integrate global macroeconomic indicators** to better contextualize inventory and price movements.

---

## Setup
### Install Requirements
```bash
pip install streamlit pandas yfinance plotly requests
