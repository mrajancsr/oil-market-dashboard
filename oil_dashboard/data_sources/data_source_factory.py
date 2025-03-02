from dataclasses import dataclass

from oil_dashboard.config.data_source_config import DataSourceConfig, DataSourceType
from oil_dashboard.data_sources.baker_hughes_source import BakerHughesSource
from oil_dashboard.data_sources.base_source import DataSource
from oil_dashboard.data_sources.eia_source import EIASource
from oil_dashboard.data_sources.yahoo_finance_source import YahooFinanceSource


@dataclass
class DataSourceFactory:
    @staticmethod
    def create_data_source(config: DataSourceConfig) -> DataSource:
        if config.source_type == DataSourceType.YAHOO_FINANCE:
            return YahooFinanceSource(config)
        elif config.source_type == DataSourceType.EIA:
            return EIASource(config)
        elif config.source_type == DataSourceType.BAKER_HUGHES:
            return BakerHughesSource(config)
        else:
            raise ValueError(f"Unknown data source type: {config.source_type}")
