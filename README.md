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

## Data Sources
- **Yahoo Finance** (WTI, Brent, OVX prices)
- **U.S. Energy Information Administration (EIA)** (Crude oil inventory data)

## Setup
### Install Requirements
```bash
pip install streamlit pandas yfinance plotly requests
