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
from oil_dashboard.utils.data_transformations import (
    reshape_baker_hughes_to_db,
    reshape_inventory_data_for_db,
    reshape_price_data_for_db,
)


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
            yahoo_df = reshape_price_data_for_db(
                data_frames[DataSourceType.YAHOO_FINANCE.name]
            )
            await handler.push(
                "commodity.price_data",
                ["date", "symbol", "open", "high", "low", "close", "volume"],
                yahoo_df.itertuples(index=False),
            )

        # Save inventory data (EIA)
        if DataSourceType.EIA.name in data_frames:
            eia_df = reshape_inventory_data_for_db(
                data_frames[DataSourceType.EIA.name]
            )
            await handler.push(
                "commodity.inventory_data",
                [
                    "date",
                    "product",
                    "inventory",
                ],
                eia_df.itertuples(index=False),
            )

        if DataSourceType.BAKER_HUGHES.name in data_frames:
            rig_count_df = reshape_baker_hughes_to_db(
                data_frames[DataSourceType.BAKER_HUGHES.name]
            )
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
                rig_count_df.itertuples(index=False),
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
