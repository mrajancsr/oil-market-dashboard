from dataclasses import dataclass

import pandas as pd

from oil_dashboard.config.data_source_config import (
    DataSourceConfig,
    DataSourceType,
)
from oil_dashboard.data_sources.base_source import DataSource


@dataclass
class BakerHughesSource(DataSource):
    """Fetch historical Crude_Oil_Inventory data from the EIA API"""

    config: DataSourceConfig

    def __post_init__(self):
        if self.config.source_type != DataSourceType.BAKER_HUGHES:
            raise ValueError("Config must be for BakerHughes Data Source")

    def fetch(self) -> pd.DataFrame:
        if not self.config.base_url:
            raise ValueError("BakerHughes base url must be provided")

        tables = pd.read_html(self.config.base_url)
        rig_count_table = tables[0]

        # filter for US only
        us_rig_data: pd.DataFrame = rig_count_table.loc[
            rig_count_table["Area"] == "U.S."
        ]

        return us_rig_data
