"""This module reshapes and modifies data for PostgreSQL storage.

Functions:
- reshape_price_data_for_db: Converts wide-format price data
                             into a database-friendly format.
- reshape_inventory_data_for_db: Formats raw EIA inventory data
                                 to match the database schema.
"""

from typing import Set

import pandas as pd

from oil_dashboard.config.constants import (
    BAKER_HUGHES_COLUMNS_US,
    INVENTORY_FEATURES,
)
from oil_dashboard.config.sql_tables import (
    COMMODITY_FEATURES_TABLE_COLUMNS,
    PRICE_DATA_TABLE_COLUMNS,
    TECHNICAL_INDICATORS,
    TECHNICAL_INDICATORS_TABLE_COLUMNS,
)


def reshape_price_data_for_db(price_data: pd.DataFrame) -> pd.DataFrame:
    """Converts wide-format price data into a long format for PostgreSQL

    The function transforms OHLCV (Open, High, Low, Close, Volume) price data
    into a structured format by:
    - Converting a wide DataFrame into a long format
    - Extracting ticker symbols and OHLCV types from column names
    - Pivoting the data back to a structured form
    - Ensuring column names match the PostgreSQL schema
    - Handling missing values and type conversions

    Parameters
    ----------
    price_data : pd.DataFrame
        Wide-format DataFrame where columns represent OHLCV
        for different tickers.

    Returns
    -------
    pd.DataFrame
        Transformed DataFrame with columns:
        [date, symbol, open, high, low, close, volume].
    """
    price_data = price_data.reset_index()  # Ensure Date is a column

    # Melt DataFrame into long format (Date, Ticker, Column, Value)
    df_long = price_data.melt(
        id_vars=["date"], var_name="column", value_name="value"
    )

    # Extract Ticker and OHLCV type from column names
    df_long[["type", "ticker"]] = df_long["column"].str.split("_", expand=True)

    # Pivot table to get correct structure: (Date, Ticker, OHLCV)
    df_final = df_long.pivot(
        index=["date", "ticker"], columns="type", values="value"
    ).reset_index()

    # Rename columns to match PostgreSQL schema
    df_final.rename(columns={"ticker": "symbol"}, inplace=True)

    # Ensure all column names are lowercase
    df_final.columns = df_final.columns.str.lower()

    # Reorder columns to match PostgreSQL schema
    df_final = df_final[PRICE_DATA_TABLE_COLUMNS]

    # Drop rows where **all columns except date & symbol are NaN**
    df_final.dropna(
        subset=PRICE_DATA_TABLE_COLUMNS[2:],
        how="all",
        inplace=True,
    )

    # Convert `volume` to `int` **only if all values are whole numbers**
    if (df_final["volume"] % 1 == 0).all():
        df_final["volume"] = df_final["volume"].astype(int)

    return df_final


def reshape_inventory_data_for_db(
    inventory_data: pd.DataFrame,
) -> pd.DataFrame:
    """Formats raw EIA inventory data to match PostgreSQL schema

     This function prepares crude oil inventory data from EIA by:
    - Renaming columns to match the database schema
    - Adding a required 'product' column to distinguish the type of inventory
    - Ensuring the column order is correct for database insertion

    Parameters
    ----------
    inventory_data : pd.DataFrame
        DataFrame containing raw crude oil inventory data from EIA.

    Returns
    -------
    pd.DataFrame
        Transformed DataFrame with columns: [date, product, inventory].
    """
    df = inventory_data.copy()

    # Reset index so `period` becomes a column
    df = df.reset_index()

    # Rename columns to match PostgreSQL schema
    df.rename(
        columns={"period": "date", "crude_oil_inventory": "inventory"},
        inplace=True,
    )

    # Add `product` column (required by schema)
    df["product"] = "crude_oil"

    # Ensure correct column order
    df = df[["date", "product", "inventory"]]

    return df


def reshape_baker_hughes_to_db(rig_data: pd.DataFrame) -> pd.DataFrame:
    """Formats raw Baker Hughes rig count data to match PostgreSQL schema.

    Parameters
    ----------
    rig_data : pd.DataFrame
        Raw Baker Hughes rig count data.

    Returns
    -------
    pd.DataFrame
        Cleaned and structured DataFrame ready for database insertion.
    """
    if rig_data.empty:
        raise ValueError("Received empty rig count data")

    # ✅ Extract the first row (U.S. data) and format as DataFrame
    us_rig_data = rig_data.iloc[0]

    data = {
        "date": pd.to_datetime(
            us_rig_data[BAKER_HUGHES_COLUMNS_US["date"]], format="%d %b %Y"
        ),
        "total_rigs": int(us_rig_data[BAKER_HUGHES_COLUMNS_US["count"]]),
        "weekly_change": int(
            us_rig_data[BAKER_HUGHES_COLUMNS_US["weekly_change"]]
        ),
        "yoy_change": int(
            us_rig_data[BAKER_HUGHES_COLUMNS_US["yearly_change"]]
        ),
        "oil_rigs": None,  # place holder until better data is found
        "gas_rigs": None,
        "misc_rigs": None,
    }

    return pd.DataFrame([data])


def prepare_technical_indicators_for_db(
    features_df: pd.DataFrame,
) -> pd.DataFrame:
    """Prepares technical indicators for database insertion.

    This function:
    - Extracts technical indicators for WTI & Brent dynamically
    - Converts data into long format ('date', 'symbol', 'indicator_name', 'value')
    - Pivots back to wide format (`date`, `symbol`, `ma50`, `rsi`, etc.)
    - Ensures missing values are stored as `NULL` for PostgreSQL compatibility

    Parameters
    ----------
    features_df : pd.DataFrame
        DataFrame containing computed technical indicators

    Returns
    -------
    pd.DataFrame
        Wide-Format DataFrmae with (`date`, `symbol`, `ma50`, `rsi`, `macd`, etc.)
    """

    # Dynamically extract WTI & Brent indicators
    wti_columns = (
        features_df.keys()
        .intersection({f"wti_{col}" for col in TECHNICAL_INDICATORS})
        .to_list()
    )
    brent_columns = (
        features_df.keys()
        .intersection({f"brent_{col}" for col in TECHNICAL_INDICATORS})
        .to_list()
    )

    # Select only the relevant columns
    technical_indicator_columns = ["date"] + wti_columns + brent_columns

    # Convert technical indicators to long format
    technical_indicators_long = features_df.melt(
        id_vars=["date"],  # Keep `date`
        value_vars=technical_indicator_columns[1:],  # Exclude `date`
        var_name="symbol_feature",
        value_name="value",
    )

    # Extract `symbol` and `indicator_name` separately
    technical_indicators_long[["symbol", "indicator_name"]] = (
        technical_indicators_long["symbol_feature"].str.split(
            "_", n=1, expand=True
        )
    )

    # Pivot to wide format matching database schema
    technical_indicators_wide = (
        technical_indicators_long.pivot(
            index=["date", "symbol"], columns="indicator_name", values="value"
        )
        .reset_index()
        .rename_axis(None, axis=1)
    )

    # Ensure column names match SQL schema
    technical_indicators_wide = technical_indicators_wide[
        TECHNICAL_INDICATORS_TABLE_COLUMNS
    ]

    # Fill missing values with NULL (SQL default behavior)
    technical_indicators_wide = technical_indicators_wide.where(
        pd.notna(technical_indicators_wide), None
    )

    return technical_indicators_wide


def prepare_features_for_db(
    features_df: pd.DataFrame, price_columns: Set[str]
) -> pd.DataFrame:
    """Prepare feature data for insertion into the PostgreSQL database

    Converts price-based, technical indactors and inventory features into long format

    Parameters
    ----------
    features_df : pd.DataFrame
        DataFrame containing comptuted features, including
        price-based, inventory and technical indicators.
    price_columns : Set[str]
        Set of raw price-related columns
        (e.g. "Open_WTI", "Close_Brent") to exclude from
        feature extraction.

    Returns
    -------
    pd.DataFrame
        Long-format DataFrame ready for database storage
    """
    # Extract only feature columns (exclusing price data)
    feature_columns = features_df.keys().difference(price_columns)

    # Extract technical indicators & price-based features in long format
    features_long = features_df.melt(
        id_vars=["date"],
        value_vars=[
            col for col in feature_columns if col not in INVENTORY_FEATURES
        ],
        var_name="symbol_feature",
        value_name="feature_value",
    )

    # Extract symbol name (e.g., WTI, Brent) and feature name dynamically for non-inventory features
    features_long[["symbol", "feature_name"]] = features_long[
        "symbol_feature"
    ].str.split("_", n=1, expand=True)

    features_long.loc[
        features_long["symbol_feature"] == "wti-brent spread", "feature_name"
    ] = "spread"

    features_long.loc[
        features_long["symbol_feature"] == "wti-brent spread", "symbol"
    ] = "wti-brent"

    # rearrange the columns in features table
    features_long = features_long[COMMODITY_FEATURES_TABLE_COLUMNS[:4]]

    # Handle potential NaNs before insertion
    features_long = features_long.where(pd.notna(features_long), None)

    # Extract inventory features separately
    inventory_long = features_df.melt(
        id_vars=["date"],
        value_vars=list(INVENTORY_FEATURES),
        var_name="feature_name",
        value_name="feature_value",
    )

    # Assign `"inventory"` as the symbol
    inventory_long["symbol"] = "inventory"

    # Merge both DataFrames to ensure all features are included
    final_features = pd.concat(
        [features_long, inventory_long], ignore_index=True
    )
    return final_features
