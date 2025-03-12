import asyncio
import os
from datetime import date
from typing import Dict

import pandas as pd
from neptunedb import AsyncDBHandler
from neptunedb.db_config import DBConfig

from oil_dashboard.config.data_source_config import DataSourceType
from oil_dashboard.pipeline.feature_engineering import generate_features
from oil_dashboard.pipeline.oil_pipeline import OilPipeLine


def reshape_for_db(df):
    """Converts the wide-format DataFrame into a long format suitable for PostgreSQL."""  # noqa
    df = df.reset_index()  # Ensure Date is a column

    # Melt DataFrame into long format (Date, Ticker, Column, Value)
    df_long = df.melt(id_vars=["Date"], var_name="Column", value_name="Value")

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


async def save_to_db(data_frames: Dict[str, pd.DataFrame]) -> None:
    """Saves the dataframe to postgresql

    Parameters
    ----------
    master_df : pd.DataFrame
        _description_
    """
    config = DBConfig.from_env()

    async with AsyncDBHandler(config) as handler:
        if DataSourceType.YAHOO_FINANCE.name in data_frames:
            yahoo_df = reshape_for_db(
                data_frames[DataSourceType.YAHOO_FINANCE.name]
            )
            await handler.push(
                "commodity.price_data",
                ["date", "symbol", "open", "high", "low", "close", "volume"],
                yahoo_df.itertuples(index=False),
            )

        # Save inventory data (EIA)
        if DataSourceType.EIA.name in data_frames:
            await handler.push(
                "commodity.inventory_data",
                [
                    "date",
                    "product",
                    "inventory",
                ],
                data_frames[DataSourceType.EIA.name].itertuples(index=False),
            )

        if DataSourceType.BAKER_HUGHES.name in data_frames:
            await handler.push(
                "commodity.rig_count_data",
                [
                    "date",
                    "total_rigs",
                    "oil_rigs",
                    "gas_rigs",
                    "misc_rigs",
                    "weekly_change",
                    "yoy_change",
                ],
                data_frames[DataSourceType.BAKER_HUGHES.name].itertuples(
                    index=False
                ),
            )
        # Generate Features **before** storing in PostgreSQL
        print("Generating Features...")
        features_df = generate_features(data_frames)
        features_df.reset_index(inplace=True)
        features_df.rename(columns={"index": "Date"}, inplace=True)

        # Reshape for database storage (long format)
        features_long = features_df.melt(
            id_vars=["Date", "symbol"],
            var_name="feature_name",
            value_name="feature_value",
        )

        # Store computed features in PostgreSQL
        await handler.push(
            "commodity.features",
            ["Date", "symbol", "feature_name", "feature_value"],
            features_long.itertuples(index=False),
        )


def main():
    api_key = os.getenv("EIA_API_KEY")
    if not api_key:
        raise ValueError("EIA_API_KEY environment variable is required")

    pipeline = OilPipeLine(
        start_date=date(2025, 1, 1), end_date=date.today(), api_key=api_key
    )

    os.makedirs("data", exist_ok=True)

    print("Fetching data sources...")
    data_frames: Dict[str, pd.DataFrame] = pipeline.fetch_all_data()

    print("Saving to PostGreSQL Database")
    asyncio.run(save_to_db(data_frames))


if __name__ == "__main__":
    main()
