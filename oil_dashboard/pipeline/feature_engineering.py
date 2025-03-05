from typing import Dict

import numpy as np
import pandas as pd

from oil_dashboard.pipeline.technical_indicators import (
    add_technical_indicators,
)


def add_price_based_features(oil_data: pd.DataFrame) -> pd.DataFrame:
    oil_data["WTI Log Return"] = np.log(1 + oil_data["WTI"].pct_change())
    oil_data["Brent Log Return"] = np.log(1 + oil_data["Brent"].pct_change())
    oil_data["WTI-Brent Spread"] = oil_data["WTI"] - oil_data["Brent"]
    oil_data["WTI Weekly Price Change"] = oil_data["WTI"].pct_change(5) * 100

    oil_data["WTI-7D MA"] = oil_data["WTI"].rolling(7).mean()
    oil_data["Brent-7D MA"] = oil_data["Brent"].rolling(7).mean()
    oil_data["OVX 7D MA"] = oil_data["OVX"].rolling(7).mean()
    oil_data["WTI Rolling Volatility"] = (
        oil_data["WTI Log Return"].rolling(7).std()
    )
    oil_data["WTI Momentum"] = oil_data["WTI"].pct_change(5)

    return oil_data


def add_inventory_based_features(inventory_df: pd.DataFrame) -> pd.DataFrame:
    # weekly inventory change and z-scores
    inventory_df["Weekly Inventory Change"] = inventory_df[
        "Crude Oil Inventory"
    ].diff()
    inventory_df["Weekly Percent Change"] = inventory_df[
        "Crude Oil Inventory"
    ].pct_change()
    inventory_daily = inventory_df.resample("D").ffill()
    inventory_daily["Inventory Zscore"] = (
        inventory_daily["Crude Oil Inventory"]
        - inventory_daily["Crude Oil Inventory"].mean()
    ) / inventory_daily["Crude Oil Inventory"].std()

    return inventory_daily


def generate_features(
    data_frames: Dict[str, pd.DataFrame],
) -> pd.DataFrame:
    """Generate derived features for the dashboard including:
    - Log returns, moving averages, and volatility for prices
    - Weekly inventory changes and inventory anomalies
    - Combines price and inventory data into a single master DataFrame

    Parameters
    ----------
    data_frames : Dict[str, pd.DataFrame]
        Dictionary containing data from multiple sources
        (e.g., YAHOO_FINANCE, EIA, BAKER_HUGHES)

    Returns
    -------
    pd.DataFrame
        a master dataframe with combined data and derived features

    Raises
    ------
    ValueError
        if YAHOO_FINANCE OR EIA is not in data_frames
    ValueError
        if the oil price data or inventory data is empty
    """
    if "YAHOO_FINANCE" not in data_frames or "EIA" not in data_frames:
        raise ValueError(
            "Missing required data sources for feature engineering"
        )

    oil_data = data_frames["YAHOO_FINANCE"]
    oil_inventory = data_frames["EIA"]

    if oil_data.empty or oil_inventory.empty:
        raise ValueError("Data Not Available for feature engineering")

    oil_data.index = pd.to_datetime(oil_data.index)
    oil_inventory.index = pd.to_datetime(oil_inventory.index)

    oil_data = add_price_based_features(oil_data)

    inventory_daily = add_inventory_based_features(oil_inventory)

    # merge to master DataFrame
    master_df = oil_data.join(inventory_daily, how="outer").ffill()

    # Add technical indicators via helper function
    master_df = add_technical_indicators(master_df)

    return master_df
