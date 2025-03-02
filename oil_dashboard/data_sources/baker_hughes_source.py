from dataclasses import dataclass
from datetime import date

import pandas as pd
import requests

from oil_dashboard.config.constants import (
    BAKER_HUGES_US_RIG_COUNT_URL,
    BAKER_HUGHES_COLUMNS_US,
)
from oil_dashboard.config.data_source_config import DataSourceConfig, DataSourceType
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
        ].iloc[0]

        # convert into clean dataframe
        data = {
            "Date": pd.to_datetime(
                us_rig_data[BAKER_HUGHES_COLUMNS_US["date"]], format="%d %b %Y"
            ),
            "US Rig Count": int(us_rig_data[BAKER_HUGHES_COLUMNS_US["count"]]),
            "Weekly Change": int(us_rig_data[BAKER_HUGHES_COLUMNS_US["weekly_change"]]),
            "Prior Week Date": pd.to_datetime(
                us_rig_data[BAKER_HUGHES_COLUMNS_US["prior_week_count_date"]]
            ),
            "Yearly Change": int(us_rig_data[BAKER_HUGHES_COLUMNS_US["yearly_change"]]),
        }

        return pd.DataFrame([data])
