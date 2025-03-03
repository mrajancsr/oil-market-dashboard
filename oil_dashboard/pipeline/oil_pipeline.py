from dataclasses import dataclass
from datetime import date
from typing import Dict, List

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
    start_date: date
    end_date: date
    api_key: str

    def generate_data_source_configs(
        self, start_date: date = None, end_date: date = None
    ) -> List[DataSourceConfig]:
        """Generates configs for each DataSource

        Parameters
        ----------
        start_date : date, optional
            _description_, by default None
        end_date : date, optional
            _description_, by default None

        Returns
        -------
        List[DataSourceConfig]
            _description_
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
        configs: List[DataSourceConfig] = self.generate_data_source_configs()

        data_frames = {}
        for config in configs:
            data_source: DataSource = DataSourceFactory.create_data_source(
                config
            )
            df = data_source.fetch()
            data_frames[config.source_type.name] = df

        return data_frames
