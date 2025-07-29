import asyncio
import os
from pathlib import Path
from typing import List

import pandas as pd
from neptunedb import AsyncDBHandler
from neptunedb.db_config import DBConfig

from oil_dashboard.config.sql_tables import (
    RIG_COUNT_DATA_TABLE_COLUMNS,
    SQLTableType,
)


def load_us_rigs_from_csv(path_to_dir: str) -> pd.DataFrame:
    base_path = Path(path_to_dir)
    dfs: List[pd.DataFrame] = []
    for file in base_path.glob("*.csv"):
        df = pd.read_csv(file)  # type: ignore
        df["date"] = pd.to_datetime(df["date"])  # type: ignore
        dfs.append(df)
    data = pd.concat(dfs, ignore_index=True)
    data.sort_values("date", inplace=True)  # type: ignore
    data["weekly_change"] = data["total_rigs"].diff()
    return data


async def main():
    base_path: str = os.getcwd()
    path_to_dir = os.path.join(base_path, "data")
    table = load_us_rigs_from_csv(path_to_dir=path_to_dir)
    config = DBConfig.from_env()
    async with AsyncDBHandler(config) as handler:
        await handler.push(
            table_name=SQLTableType.RIG_COUNT_DATA.value,
            schema_name=SQLTableType.SCHEMA_NAME.value,
            columns=RIG_COUNT_DATA_TABLE_COLUMNS,
            table=table,
        )


if __name__ == "__main__":
    asyncio.run(main())
