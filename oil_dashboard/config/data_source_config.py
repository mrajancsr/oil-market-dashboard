from dataclasses import dataclass
from datetime import date
from enum import Enum
from typing import Any, Dict, List, Optional

from oil_dashboard.config.tickers import TickerMapping


class DataSourceType(Enum):
    YAHOO_FINANCE = "yahoo_finance"
    EIA = "eia"
    BAKER_HUGHES = "baker_hughes"


@dataclass
class DataSourceConfig:
    source_type: DataSourceType
    base_url: Optional[str] = None
    api_key: Optional[str] = None
    tickers: Optional[List[TickerMapping]] = None
    start_date: Optional[date] = None
    end_date: Optional[date] = None
    request_params: Optional[Dict[str, Any]] = None
