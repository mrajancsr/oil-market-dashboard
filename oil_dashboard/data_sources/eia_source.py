from dataclasses import dataclass
from typing import Any, Dict, List

import pandas as pd
import requests

from oil_dashboard.config.data_source_config import (
    DataSourceConfig,
    DataSourceType,
)
from oil_dashboard.data_sources.base_source import DataSource


@dataclass
class EIASource(DataSource):
    """Fetch historical Crude_Oil_Inventory data from the EIA API"""

    config: DataSourceConfig

    def __post_init__(self):
        if self.config.source_type != DataSourceType.EIA:
            raise ValueError("Config must be for EIA data source")

    def fetch(self) -> pd.DataFrame:
        if not self.config.api_key or not self.config.base_url:
            raise ValueError("EIA API and base url must be provided")

        if not self.config.request_params:
            raise ValueError("request parameters must be provided")

        if not self.config.start_date or not self.config.end_date:
            raise ValueError("start date and end date must be provided")

        params = self.config.request_params.copy()
        params["api_key"] = self.config.api_key
        params["start"] = self.config.start_date.strftime("%Y-%m-%d")
        params["end"] = self.config.end_date.strftime("%Y-%m-%d")

        response = requests.get(self.config.base_url, params=params)
        if response.status_code != 200:
            raise Exception(
                f"Failed to fetch data: {response.status_code}, {response.text}"  # noqa
            )

        data = response.json()

        if "response" not in data or "data" not in data["response"]:
            raise Exception("Unexpected response format")

        records: List[Dict[str, Any]] = data["response"]["data"]

        if not records:
            raise Exception("No inventory data returned.")

        inventory_data = pd.DataFrame(records)

        # Ensure 'period' is datetime and set as index
        inventory_data["period"] = pd.to_datetime(inventory_data["period"])  # type: ignore
        inventory_data.set_index("period", inplace=True)  # type: ignore

        # Keep only the relevant column, rename it, and ensure data type
        inventory_data = inventory_data[["value"]]
        inventory_data.rename(
            columns={"value": "crude_oil_inventory"}, inplace=True
        )
        inventory_data["crude_oil_inventory"] = inventory_data[
            "crude_oil_inventory"
        ].astype(int)

        inventory_data.dropna(inplace=True)  # type: ignore

        return inventory_data
