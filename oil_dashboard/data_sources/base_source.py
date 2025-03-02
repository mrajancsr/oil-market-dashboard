from abc import ABC, abstractmethod
from dataclasses import dataclass

from oil_dashboard.config.data_source_config import DataSourceConfig


@dataclass
class DataSource(ABC):
    """Abstract Base Class representing the source of Data"""

    config: DataSourceConfig

    @abstractmethod
    def fetch(self):
        pass
