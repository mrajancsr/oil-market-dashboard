import asyncio
import logging
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

logging.basicConfig(
    level=logging.INFO,  # ✅ Change to DEBUG for more details
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


async def save_to_db(data_frames: Dict[str, pd.DataFrame]) -> None:
    """Saves the dataframe to postgresql

    Parameters
    ----------
    master_df : pd.DataFrame
        _description_
    """
    config = DBConfig.from_env()

    async with AsyncDBHandler(config) as handler:
        # Generate Features **before** storing in PostgreSQL
        print("Generating Features...")
        features_df = generate_features(data_frames).reset_index()
        features_df.rename(columns={"index": "date"}, inplace=True)

        # Extract Technical Indicators (MA, Bollinger, RSI, MACD)
        technical_indicator_columns = [
            "ma50",
            "ma200",
            "bb_upper",
            "bb_lower",
            "rsi",
            "macd",
            "macd_signal",
        ]
        technical_indicators_df = features_df[
            ["date", "symbol"]
            + [
                col
                for col in technical_indicator_columns
                if col in features_df.columns
            ]
        ].fillna(0)

        # Extract Features (WTI Log Return, Spread, etc.) (long format)
        feature_columns = list(
            set(features_df.columns)
            - set(["open", "high", "low", "close", "volume"])
        )
        features_long = features_df.melt(
            id_vars=["date", "symbol"],
            value_vars=feature_columns,
            var_name="feature_name",
            value_name="feature_value",
        )

        # Store Indicators
        await handler.push(
            "technical_indicators",
            "commodity",
            ["date", "symbol"] + technical_indicator_columns,
            technical_indicators_df.itertuples(index=False),
        )
        logger.info(
            f"Inserted {len(technical_indicators_df)} rows into commodity.technical_indicators"
        )

        # Store computed features in PostgreSQL
        await handler.push(
            "features",
            "commodity",
            ["date", "symbol", "feature_name", "feature_value"],
            features_long.itertuples(index=False),
        )
        logger.info(
            f"Inserted {len(features_long)} rows into commodity.features"
        )
        # Reshape and Insert Price Data
        if DataSourceType.YAHOO_FINANCE.name in data_frames:
            data_frames[DataSourceType.YAHOO_FINANCE.name] = (
                reshape_price_data_for_db(
                    data_frames[DataSourceType.YAHOO_FINANCE.name]
                )
            )

            await handler.push(
                "price_data",
                "commodity",
                ["date", "symbol", "open", "high", "low", "close", "volume"],
                data_frames[DataSourceType.YAHOO_FINANCE.name].itertuples(
                    index=False
                ),
            )
            logger.info(
                f"Inserted {len(data_frames[DataSourceType.YAHOO_FINANCE.name])} rows into commodity.price_data"
            )

        # Reshape & Insert Inventory Data
        if DataSourceType.EIA.name in data_frames:
            data_frames[DataSourceType.EIA.name] = (
                reshape_inventory_data_for_db(
                    data_frames[DataSourceType.EIA.name]
                )
            )
            await handler.push(
                "inventory_data",
                "commodity",
                [
                    "date",
                    "product",
                    "inventory",
                ],
                data_frames[DataSourceType.EIA.name].itertuples(index=False),
            )
            logger.info(
                f"Inserted {len(data_frames[DataSourceType.EIA.name])} rows into commodity.inventory_data"
            )

        # Reshape & Insert Rig Count Data (Baker Hughes)
        if DataSourceType.BAKER_HUGHES.name in data_frames:
            data_frames[DataSourceType.BAKER_HUGHES.name] = (
                reshape_baker_hughes_to_db(
                    data_frames[DataSourceType.BAKER_HUGHES.name]
                )
            )
            await handler.push(
                "rig_count_data",
                "commodity",
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
            logger.info(
                f"Inserted {len(data_frames[DataSourceType.BAKER_HUGHES.name])} rows into commodity.rig_count_data"
            )


def main():
    api_key = os.getenv("EIA_API_KEY")
    if not api_key:
        raise ValueError("EIA_API_KEY environment variable is required")

    pipeline = OilPipeLine(
        start_date=date(2024, 1, 1), end_date=date.today(), api_key=api_key
    )

    os.makedirs("data", exist_ok=True)

    print("Fetching data sources...")
    data_frames: Dict[str, pd.DataFrame] = pipeline.fetch_all_data()

    print("Saving to PostGreSQL Database")
    asyncio.run(save_to_db(data_frames))


if __name__ == "__main__":
    main()
