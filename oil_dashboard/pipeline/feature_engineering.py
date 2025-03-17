from typing import Dict, List

import numpy as np
import pandas as pd

from oil_dashboard.config.data_source_config import DataSourceType
from oil_dashboard.pipeline.technical_indicators import (
    add_technical_indicators,
)


def add_price_based_features(
    oil_data: pd.DataFrame, columns: List[str]
) -> pd.DataFrame:
    for col in columns:
        oil_data[f"{col}_log_return"] = np.log(1 + oil_data[col].pct_change())
        oil_data[f"{col}_momentum"] = oil_data[col].pct_change(5)
        oil_data[f"{col}_rolling_volatility"] = (
            oil_data[f"{col}_log_return"].rolling(7).std()
        )
        oil_data[f"{col}_weekly_price_change"] = (
            oil_data[col].pct_change(5) * 100
        )

    # Compute WTI-Brent Spread
    oil_data["wti-brent spread"] = oil_data["wti"] - oil_data["brent"]

    oil_data["wti_7D_MA"] = oil_data["wti"].rolling(7).mean()
    oil_data["brent_7D_MA"] = oil_data["brent"].rolling(7).mean()
    oil_data["ovx_7D_MA"] = oil_data["ovx"].rolling(7).mean()

    return oil_data


def add_inventory_based_features(
    inventory_df: pd.DataFrame, col: str = "crude_oil_inventory"
) -> pd.DataFrame:
    # weekly inventory change and z-scores
    inventory_df["weekly_inventory_change"] = inventory_df[col].diff()
    inventory_df["weekly_percent_change"] = inventory_df[col].pct_change()
    inventory_daily = inventory_df.resample("D").ffill()
    inventory_daily["inventory_zscore"] = (
        inventory_daily[col] - inventory_daily[col].mean()
    ) / inventory_daily[col].std()

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

    oil_data = data_frames[DataSourceType.YAHOO_FINANCE.name].copy()
    oil_inventory = data_frames[DataSourceType.EIA.name].copy()

    if oil_data.empty or oil_inventory.empty:
        raise ValueError("Data Not Available for feature engineering")

    oil_data.index = pd.to_datetime(oil_data.index)
    oil_inventory.index = pd.to_datetime(oil_inventory.index)

    # Rename Close Price Columns Before Computing Price Based Features
    rename_map = {
        "close_wti": "wti",
        "close_brent": "brent",
        "close_ovx": "ovx",
        "close_dxy": "dxy",
    }

    oil_data.rename(columns=rename_map, inplace=True)

    oil_data = add_price_based_features(oil_data, columns=["wti", "brent"])

    inventory_daily = add_inventory_based_features(oil_inventory)

    # merge to master DataFrame
    master_df = oil_data.join(inventory_daily, how="outer").ffill()

    # Add technical indicators via helper function
    master_df = add_technical_indicators(master_df, columns=["wti", "brent"])

    # Restore columns back to their original names
    reverse_map = {value: key for key, value in rename_map.items()}

    master_df.rename(columns=reverse_map, inplace=True)

    return master_df
