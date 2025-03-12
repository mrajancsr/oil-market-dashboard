"""This module reshapes and modifies data for PostgreSQL storage.

Functions:
- reshape_price_data_for_db: Converts wide-format price data
                             into a database-friendly format.
- reshape_inventory_data_for_db: Formats raw EIA inventory data
                                 to match the database schema.
"""

import pandas as pd


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
        id_vars=["Date"], var_name="Column", value_name="Value"
    )

    # Extract Ticker and OHLCV type from column names
    df_long[["Type", "Ticker"]] = df_long["Column"].str.split("_", expand=True)

    # Pivot table to get correct structure: (Date, Ticker, OHLCV)
    df_final = df_long.pivot(
        index=["Date", "Ticker"], columns="Type", values="Value"
    ).reset_index()

    # Rename columns to match PostgreSQL schema
    df_final.rename(columns={"Ticker": "symbol", "Date": "date"}, inplace=True)

    # Ensure all column names are lowercase
    df_final.columns = df_final.columns.str.lower()

    # Reorder columns to match PostgreSQL schema
    df_final = df_final[
        ["date", "symbol", "open", "high", "low", "close", "volume"]
    ]

    # Drop rows where **all columns except date & symbol are NaN**
    df_final.dropna(
        subset=["open", "high", "low", "close", "volume"],
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
        columns={"period": "date", "Crude Oil Inventory": "inventory"},
        inplace=True,
    )

    # Add `product` column (required by schema)
    df["product"] = "Crude Oil"

    # Ensure correct column order
    df = df[["date", "product", "inventory"]]

    return df
