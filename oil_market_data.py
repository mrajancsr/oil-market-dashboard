"""
This script collects historical oil market data, including
- WTI and Brent Crude Oil Prices
- Oil Price Volatility (OVX)
- Crude_Oil_Inventory"Crude_Oil_Inventory" Levels from the EIA API

The data is processed and saved for further analysis
"""

import yfinance as yf
import pandas as pd
import requests
from dataclasses import dataclass
import os
import numpy as np


@dataclass
class OilMarketData:
    start_date: str = "2023-01-01"
    end_date: str = "2023-01-10"

    def get_oil_prices(self):
        """Fetches historical WTI and Brent Oil Prices from Yahoo Finance"""
        wti = yf.download(
            "CL=F", start=self.start_date, end=self.end_date, progress=True
        ).droplevel(level=1, axis=1)["Close"]

        brent = yf.download(
            "BZ=F", start=self.start_date, end=self.end_date, progress=True
        ).droplevel(level=1, axis=1)["Close"]
        oil_prices = pd.DataFrame({"WTI": wti, "Brent": brent})
        oil_prices["WTI-Brent Spread"] = oil_prices["WTI"] - oil_prices["Brent"]
        return oil_prices

    def get_oil_volatility(self):
        """Fetch historical Oil Volatility Index (OVX) from Yahoo Finance"""
        ovx = yf.download(
            "^OVX", start=self.start_date, end=self.end_date, progress=True
        ).droplevel(level=1, axis=1)["Close"]
        return pd.DataFrame({"OVX (Oil VIX)": ovx})

    def get_eia_inventory(self, api_key: str):
        """Fetch historical Crude_Oil_Inventory"Crude_Oil_Inventory" data from the EIA API"""

        url = "https://api.eia.gov/v2/petroleum/stoc/wstk/data/"

        params = {
            "api_key": api_key,
            "frequency": "weekly",
            "data[0]": ["value"],  # This is key — asks for actual values
            "facets[product][]": ["EPC0"],  # Crude Oil
            "facets[duoarea][]": ["NUS"],  # Total U.S.
            "facets[process][]": ["SAE"],  # Ending Stocks
            "start": self.start_date,
            "end": self.end_date,
            "sort[0][column]": "period",
            "sort[0][direction]": "desc",
            "offset": 0,
            "length": 100,
        }

        res = requests.get(url, params=params)
        if res.status_code != 200:
            raise Exception(f"Failed to fetch data: {res.status_code}, {res.text}")

        data = res.json()

        if "response" not in data or "data" not in data["response"]:
            raise Exception("Unexpected response format")

        records = data["response"]["data"]

        if not records:
            raise Exception("No inventory data returned.")

        inventory_data = pd.DataFrame.from_records(records)
        inventory_data["period"] = pd.to_datetime(inventory_data["period"])
        inventory_data.set_index("period", inplace=True)

        # Clean up columns
        inventory_data = inventory_data[["value"]]
        inventory_data["value"] = inventory_data["value"].astype(int)
        inventory_data.rename(columns={"value": "Crude_Oil_Inventory"}, inplace=True)

        return inventory_data

    def get_oil_data(self, api_key: str):
        oil_prices = self.get_oil_prices()
        oil_vol = self.get_oil_volatility()
        oil_inventory = self.get_eia_inventory(api_key)

        all_data = self.preprocess_oil_data(oil_prices, oil_inventory, oil_vol)
        return all_data

    def preprocess_oil_data(
        self,
        oil_prices: pd.DataFrame,
        oil_inventory: pd.DataFrame,
        oil_volatility: pd.DataFrame,
    ) -> pd.DataFrame:
        for df in (oil_prices, oil_inventory, oil_volatility):
            df.index = pd.to_datetime(df.index)

        # log returns
        oil_prices["WTI_Log_Return"] = np.log(1 + oil_prices["WTI"].pct_change())
        oil_prices["Brent_Log_Return"] = np.log(1 + oil_prices["Brent"].pct_change())

        # moving averages
        oil_prices["WTI-7D_MA"] = oil_prices["WTI"].rolling(7).mean()
        oil_prices["Brent-7D_MA"] = oil_prices["Brent"].rolling(7).mean()
        oil_volatility["OVX_7D_MA"] = oil_volatility["OVX (Oil VIX)"].rolling(7).mean()

        # inventory weekly change and pct change
        oil_inventory["Weekly_Inventory_Change"] = oil_inventory[
            "Crude_Oil_Inventory"
        ].diff()
        oil_inventory["Weekly_Percent_Change"] = oil_inventory[
            "Crude_Oil_Inventory"
        ].pct_change()

        # Resample inventory to daily
        inventory_daily = oil_inventory.resample("D").ffill()

        all_data = oil_prices.join(
            [oil_volatility, inventory_daily], how="outer"
        ).ffill()

        # feature engineering
        all_data["WTI_Rolling_Volatility"] = all_data["WTI_Log_Return"].rolling(7).std()
        all_data["WTI_Momentum"] = all_data["WTI"].pct_change(5)

        # inventory anomalies
        all_data["Inventory_Zscore"] = (
            all_data["Crude_Oil_Inventory"] - all_data["Crude_Oil_Inventory"].mean()
        ) / all_data["Crude_Oil_Inventory"].std()

        return all_data


def main():
    START_DATE = "2024-01-01"
    END_DATE = "2024-12-31"
    API_KEY = os.getenv("EIA_API_KEY")
    oil_data = OilMarketData(START_DATE, END_DATE)
    data = oil_data.get_oil_data(API_KEY)
    data.reset_index(inplace=True)
    data.rename(columns={"index": "Date"}, inplace=True)
    data.to_csv("oil_historical_market_data.csv", index=False)


if __name__ == "__main__":
    main()
