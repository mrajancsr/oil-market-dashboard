import asyncio
import os
from datetime import date
from typing import Dict, List

import pandas as pd
import uvloop
from neptunedb import AsyncDBHandler
from neptunedb.db_config import DBConfig
from neptunedb.dbhandler import async_logger

from oil_dashboard.config.data_source_config import DataSourceType
from oil_dashboard.config.sql_tables import (
    COMMODITY_FEATURES_TABLE_COLUMNS,
    PRICE_DATA_TABLE_COLUMNS,
    RIG_COUNT_DATA_TABLE_COLUMNS,
    TECHNICAL_INDICATORS_TABLE_COLUMNS,
    SQLTableType,
)
from oil_dashboard.pipeline.feature_engineering import generate_features
from oil_dashboard.pipeline.oil_pipeline import OilPipeLine
from oil_dashboard.utils.data_transformations import (
    prepare_features_for_db,
    prepare_technical_indicators_for_db,
    reshape_baker_hughes_to_db,
    reshape_inventory_data_for_db,
    reshape_price_data_for_db,
)

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())


async def push_with_logging(
    handler: AsyncDBHandler,
    table_name: str,
    schema_name: str,
    columns: List[str],
    data: pd.DataFrame,
) -> None:
    """
    Inserts a Pandas DataFrame into a PostgreSQL table asynchronously
    and logs the number of rows inserted.

    This function:
    - Converts the given DataFrame into a tuple format for bulk insertion.
    - Asynchronously inserts data into the specified table.
    - Uses aiologger for non-blocking logging to avoid slowing down execution

    Parameters
    ----------
    handler : AsyncDBHandler
        The database handler responsible for executing the insert operation.
    table_name : str
        The name of the table where the data will be inserted.
    schema_name : str
        The schema to which the table belongs.
    columns : List[str]
        A list of column names corresponding to the table.
    data : pd.DataFrame
        The DataFrame containing the data to be inserted.

    Returns
    -------
    None
        This function performs an asynchronous database insert operation
        and logs the result, but does not return any values.
    """

    num_rows = len(data)  # Get row count before conversion

    # Convert DataFrame to tuples & push to database
    await handler.push(
        table_name, schema_name, columns, data.itertuples(index=False)
    )

    # Non-blocking logging with aiologger
    await async_logger.info(
        f"Inserted {num_rows} rows into {schema_name}.{table_name}"
    )


async def save_to_db(data_frames: Dict[str, pd.DataFrame]) -> None:
    """
    Processes, transforms, and asynchronously inserts various data sources
    into PostgreSQL.

    This function:
    - Generates features from raw market data.
    - Extracts and stores technical indicators.
    - Reshapes and stores price, inventory, and rig count data.
    - Uses `asyncio.gather()` to insert all data in parallel.
    - Uses `aiologger` for fully non-blocking logging.

    Parameters
    ----------
    data_frames : Dict[str, pd.DataFrame]
        A dictionary containing raw data from multiple sources
        (Yahoo Finance, EIA, Baker Hughes).
    """
    # Check if there's data to process
    if not data_frames:
        await async_logger.warning(
            "No data found. Skipping database insertion."
        )
        return

    # Validate Required Data Sources Exist
    missing_sources = [
        source.name
        for source in [
            DataSourceType.YAHOO_FINANCE,
            DataSourceType.EIA,
            DataSourceType.BAKER_HUGHES,
        ]
        if source.name not in data_frames
    ]

    if missing_sources:
        await async_logger.error(
            f"Missing required data sources: {', '.join(missing_sources)}. \
                Skipping database insertion."
        )
        return

    config = DBConfig.from_env()

    async with AsyncDBHandler(config) as handler:
        # Generate Features **before** storing in PostgreSQL
        await async_logger.info("Generating Features...")
        features_df = generate_features(data_frames).reset_index()
        features_df.rename(columns={"index": "date"}, inplace=True)

        # Extract Technical Indicators (MA, Bollinger, RSI, MACD)
        technical_indicators_df = prepare_technical_indicators_for_db(
            features_df
        )

        # Extract Price Columns for Features
        PRICE_COLUMNS = data_frames[DataSourceType.YAHOO_FINANCE.name].columns
        features_long = prepare_features_for_db(features_df, PRICE_COLUMNS)
        # Reshape & Insert Price Data
        data_frames[DataSourceType.YAHOO_FINANCE.name] = (
            reshape_price_data_for_db(
                data_frames[DataSourceType.YAHOO_FINANCE.name]
            )
        )

        # Reshape & Insert Inventory Data
        data_frames[DataSourceType.EIA.name] = reshape_inventory_data_for_db(
            data_frames[DataSourceType.EIA.name]
        )

        # Reshape & Insert Rig Count Data Before Insert
        data_frames[DataSourceType.BAKER_HUGHES.name] = (
            reshape_baker_hughes_to_db(
                data_frames[DataSourceType.BAKER_HUGHES.name]
            )
        )

        # Store all tasks in a list before executing
        tasks = []

        # Insert Technical Indicators if data exists
        if not technical_indicators_df.empty:
            await async_logger.info(
                f"Preparing to insert {len(technical_indicators_df)} rows into commodity.technical_indicators"
            )
            tasks.append(
                push_with_logging(
                    handler,
                    SQLTableType.TECHNICAL_INDICATORS.value,
                    SQLTableType.SCHEMA_NAME.value,
                    TECHNICAL_INDICATORS_TABLE_COLUMNS,
                    technical_indicators_df,
                )
            )

        # Insert Features if data exists
        if not features_long.empty:
            await async_logger.info(
                f"Preparing to insert {len(features_long)} rows into commodity.features"
            )
            tasks.append(
                push_with_logging(
                    handler,
                    SQLTableType.FEATURES.value,
                    SQLTableType.SCHEMA_NAME.value,
                    COMMODITY_FEATURES_TABLE_COLUMNS,
                    features_long,
                )
            )

        # Insert Price Data if available
        if not data_frames[DataSourceType.YAHOO_FINANCE.name].empty:
            await async_logger.info(
                f"Preparing to insert {len(data_frames[DataSourceType.YAHOO_FINANCE.name])} rows into commodity.price_data"
            )
            tasks.append(
                push_with_logging(
                    handler,
                    SQLTableType.PRICE_DATA.value,
                    SQLTableType.SCHEMA_NAME.value,
                    PRICE_DATA_TABLE_COLUMNS,
                    data_frames[DataSourceType.YAHOO_FINANCE.name],
                )
            )

        # Insert Inventory Data if available
        if not data_frames[DataSourceType.EIA.name].empty:
            await async_logger.info(
                f"Preparing to insert {len(data_frames[DataSourceType.EIA.name])} rows into commodity.inventory_data"
            )
            tasks.append(
                push_with_logging(
                    handler,
                    SQLTableType.INVENTORY_DATA.value,
                    SQLTableType.SCHEMA_NAME.value,
                    ["date", "product", "inventory"],
                    data_frames[DataSourceType.EIA.name],
                )
            )

        # Insert Rig Count Data if available
        if not data_frames[DataSourceType.BAKER_HUGHES.name].empty:
            await async_logger.info(
                f"Preparing to insert {len(data_frames[DataSourceType.BAKER_HUGHES.name])} rows into commodity.rig_count_data"
            )
            tasks.append(
                push_with_logging(
                    handler,
                    SQLTableType.RIG_COUNT_DATA.value,
                    SQLTableType.SCHEMA_NAME.value,
                    RIG_COUNT_DATA_TABLE_COLUMNS,
                    data_frames[DataSourceType.BAKER_HUGHES.name],
                )
            )

        # Run all inserts concurrently
        if tasks:
            await asyncio.gather(*tasks)
            await async_logger.info("All data inserted successfully.")
        else:
            await async_logger.warning(
                "No valid data to insert. Skipping database write."
            )


async def main():
    api_key = os.getenv("EIA_API_KEY")
    if not api_key:
        raise ValueError("EIA_API_KEY environment variable is required")

    pipeline = OilPipeLine(
        start_date=date(2024, 8, 1), end_date=date.today(), api_key=api_key
    )

    os.makedirs("data", exist_ok=True)
    try:
        print("Fetching data sources...")
        data_frames: Dict[str, pd.DataFrame] = pipeline.fetch_all_data()

        print("Saving to PostGreSQL Database")
        await save_to_db(data_frames)

    finally:
        await async_logger.shutdown()

        print("Finished gathering data...")


if __name__ == "__main__":
    asyncio.run(main())
