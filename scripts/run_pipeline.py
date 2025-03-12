import os
from datetime import date
from typing import Dict

import pandas as pd
from neptunedb.db_config import DBConfig
from neptunedb.dbreader import AsyncDBReader

from oil_dashboard.config.data_source_config import DataSourceType
from oil_dashboard.pipeline.feature_engineering import generate_features
from oil_dashboard.pipeline.oil_pipeline import OilPipeLine


def reshape_for_db(df):
    """Converts the wide-format DataFrame into a long format suitable for PostgreSQL."""
    df = df.reset_index()  # Ensure Date is a column

    # ✅ Melt DataFrame into long format (Date, Ticker, Column, Value)
    df_long = df.melt(id_vars=["Date"], var_name="Column", value_name="Value")

    # ✅ Extract Ticker and OHLCV type from column names
    df_long[["Type", "Ticker"]] = df_long["Column"].str.split("_", expand=True)

    # ✅ Pivot table to get correct structure: (Date, Ticker, Open, High, Low, Close, Volume)
    df_final = df_long.pivot(
        index=["Date", "Ticker"], columns="Type", values="Value"
    ).reset_index()

    return df_final


async def save_to_db(data_frames: Dict[str, pd.DataFrame]) -> None:
    """Saves the dataframe to postgresql

    Parameters
    ----------
    master_df : pd.DataFrame
        _description_
    """
    config = DBConfig.from_env()

    async with AsyncDBReader(config) as reader:
        if DataSourceType.YAHOO_FINANCE.name in data_frames:
            yahoo_df = reshape_for_db(
                data_frames[DataSourceType.YAHOO_FINANCE.name]
            )
            await reader.push(
                yahoo_df.itertuples(index=False), "commodity.price_data"
            )
        # ✅ Save inventory data (EIA)
        if DataSourceType.EIA.name in data_frames:
            await reader.push(
                data_frames[DataSourceType.EIA.name].itertuples(index=False),
                "commodity.inventory_data",
            )

        if DataSourceType.BAKER_HUGHES.name in data_frames:
            await reader.push(
                "commodity.rig_count_data",
                data_frames[DataSourceType.BAKER_HUGHES.name].itertuples(
                    index=False
                ),
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

    print("Generating Features...")
    master_df = generate_features(data_frames)
    master_df.reset_index(inplace=True)
    master_df.rename(columns={"index": "Date"}, inplace=True)

    print("Saving data to CSV...")
    filename = "data/oil_market_data.csv"
    master_df.to_csv(filename, index=False)

    print(f"Pipeline completed successfully! Data saved to {filename}")


if __name__ == "__main__":
    main()
