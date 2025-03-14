from enum import Enum


class SQLTableType(Enum):
    PRICE_DATA = "price_data"
    TECHNICAL_INDICATORS = "technical_indicators"
    RIG_COUNT_DATA = "rig_count_data"
    INVENTORY_DATA = "inventory_data"
    FEATURES = "features"


TECHNICAL_INDICATORS = [
    "MA50",
    "MA200",
    "BB_Upper",
    "BB_Lower",
    "RSI",
    "MACD",
    "MACD_Signal",
]

# Tables in PostGreSQL
TECHNICAL_INDICATORS_TABLE_COLUMNS = ["date", "symbol"] + TECHNICAL_INDICATORS
PRICE_DATA_TABLE_COLUMNS = [
    "date",
    "symbol",
    "open",
    "high",
    "low",
    "close",
    "volume",
]
RIG_COUNT_DATA_TABLE_COLUMNS = [
    "date",
    "total_rigs",
    "oil_rigs",
    "gas_rigs",
    "misc_rigs",
    "weekly_change",
    "yoy_change",
]
