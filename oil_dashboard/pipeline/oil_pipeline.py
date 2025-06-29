from dataclasses import dataclass
from datetime import date
from typing import Dict, List, Optional

import pandas as pd

from oil_dashboard.config.constants import (
    BAKER_HUGES_US_RIG_COUNT_URL,
    EIA_URL,
)
from oil_dashboard.config.data_source_config import (
    DataSourceConfig,
    DataSourceType,
)
from oil_dashboard.config.eia_config import EIA_CRUDE_INVENTORY_REQUEST_PARAMS
from oil_dashboard.config.tickers import TICKERS
from oil_dashboard.data_sources.base_source import DataSource
from oil_dashboard.data_sources.data_source_factory import DataSourceFactory


@dataclass
class OilPipeLine:
    """Handles the data pipeline for fetching crude oil-related datasets.

    This class is responsible for generating configurations for different
    data sources (Yahoo Finance, EIA, Baker Hughes), fetching data,
    and returning them as Pandas DataFrames.

    Attributes
    ----------
    start_date: date
        The start date for data retrieval
    end_date: date
        The end date for data retrieval
    api_key: str
        The API key required for EIA data retrieval
    """

    start_date: date
    end_date: date
    api_key: str

    def generate_data_source_configs(
        self,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> List[DataSourceConfig]:
        """Generates configuration objects for each data source

        Parameters
        ----------
        start_date : Optional[date], default=None
            The start date for data retrieval.
            Defaults to the instance's start_date
        end_date : Optional[date], default=None
            The end date for data retrieval.
            Defaults to the instance's end_date

        Returns
        -------
        List[DataSourceConfig]
            A List of DataSourceConfig objects containing configurations
            for Yahoo Finance EIA and Baker Hughes data sources
        """
        start_date = start_date or self.start_date
        end_date = end_date or self.end_date
        return [
            DataSourceConfig(
                source_type=DataSourceType.YAHOO_FINANCE,
                tickers=TICKERS,
                start_date=start_date,
                end_date=end_date,
            ),
            DataSourceConfig(
                source_type=DataSourceType.EIA,
                base_url=EIA_URL,
                api_key=self.api_key,
                start_date=start_date,
                end_date=end_date,
                request_params=EIA_CRUDE_INVENTORY_REQUEST_PARAMS,
            ),
            DataSourceConfig(
                source_type=DataSourceType.BAKER_HUGHES,
                base_url=BAKER_HUGES_US_RIG_COUNT_URL,
            ),
        ]

    def fetch_all_data(self) -> Dict[str, pd.DataFrame]:
        """Fetches data from all configured sources and returns them
        as a dict of DataFrames


        Returns
        -------
        Dict[str, pd.DataFrame]
            A dictionary where the keys are data source names (as strings)
            and the values are Pandas DataFrames containing the retrieved data.
        """
        configs: List[DataSourceConfig] = self.generate_data_source_configs()

        data_frames: Dict[str, pd.DataFrame] = {}
        for config in configs:
            data_source: DataSource = DataSourceFactory.create_data_source(
                config
            )
            df = data_source.fetch()
            if not isinstance(df, pd.DataFrame):
                continue
            data_frames[config.source_type.name] = df

        return data_frames
