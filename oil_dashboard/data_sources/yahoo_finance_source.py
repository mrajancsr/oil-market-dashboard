import os
from dataclasses import dataclass
from typing import Optional

import pandas as pd
import yfinance as yf  # type: ignore

from oil_dashboard.config.data_source_config import (
    DataSourceConfig,
    DataSourceType,
)
from oil_dashboard.data_sources.base_source import DataSource


@dataclass
class YahooFinanceSource(DataSource):
    config: DataSourceConfig
    cache_dir: str = "data/cache"

    def __post_init__(self):
        if self.config.source_type != DataSourceType.YAHOO_FINANCE:
            raise ValueError("Config must be for Yahoo Finance data source")

    def fetch(self) -> Optional[pd.DataFrame]:
        if not self.config.tickers:
            raise ValueError(
                "Tickers must be provided for YahooFinanceDataSource"
            )  # noqa

        if not self.config.start_date or not self.config.end_date:
            raise TypeError("start date and end date must be provided")

        start = self.config.start_date.strftime("%Y-%m-%d")
        end = self.config.end_date.strftime("%Y-%m-%d")

        ticker_map = {t.ticker: t.name for t in self.config.tickers}

        tickers = list(ticker_map.keys())

        os.makedirs(self.cache_dir, exist_ok=True)

        # Generate a cache filename
        cache_file = os.path.join(
            self.cache_dir, f"{'_'.join(tickers)}_{start}_{end}.csv"
        )

        path_to_cache_file = os.path.join(os.getcwd(), cache_file)

        if os.path.exists(path_to_cache_file):
            print(f"[CACHE HIT] Loading Yahoo data from {cache_file}")
            df = pd.read_csv(
                path_to_cache_file, parse_dates=["date"], index_col="date"
            )
            return df

        print(f"[FETCH] Downloading {tickers} from Yahoo Finance")

        # get the close price
        oil_data: Optional[pd.DataFrame] = yf.download(  # type: ignore
            tickers,
            start=start,
            end=end,
            progress=True,
        )

        if isinstance(oil_data, pd.DataFrame):
            oil_data.rename(columns=ticker_map, inplace=True)
            oil_data.index.name = "date"

            # Flatten multiindex to get clean column names
            oil_data.columns = [
                "_".join(col).strip() for col in oil_data.columns
            ]
            oil_data.rename(columns=str.lower, inplace=True)

            # save to cache
            oil_data.to_csv(path_to_cache_file)
            return oil_data
