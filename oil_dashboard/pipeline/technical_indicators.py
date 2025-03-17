from typing import List

import pandas as pd

from oil_dashboard.utils.dataframe_utils import validate_column_presence


def calculate_moving_average(
    df: pd.DataFrame, column: str, windows: List[int] = [50, 200]
) -> pd.DataFrame:
    """Calculates moving average for a given column

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame containing price data
    column : str
        Column name to calculate MAs for
    windows : List[int], optional
        moving average windows to calculate, by default [50, 100]

    Returns
    -------
    pd.DataFrame
        DataFrame with additional columns for each moving average

    Raises
    ------
    KeyError
        if the particular column is not found
    """
    validate_column_presence(df, column)

    if len(df) < max(windows):
        raise ValueError(
            f"Not enough data points ({len(df)}) to calculate {max(windows)}-day MA."  # noqa
        )

    return df.assign(
        **{
            f"{column}_ma{window}": df[column].rolling(window).mean()
            for window in windows
        }
    ).copy()


def calculate_bollinger_bands(
    df: pd.DataFrame, column: str, window: int = 20
) -> pd.DataFrame:
    """Calculate Bollinger Bands

    Upper Band = MA + 2 * Std Dev
    Lower Band = MA - 2 * Std Dev

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame containing price data
    column : str
        Column name to calculate Bollinger Bands
    window : int, optional
        Rolling window for Bollinger Bands, by default 20

    Returns
    -------
    pd.DataFrame
        DataFrame with upper and lower Bollinger Bands columns

    Raises
    ------
    KeyError
        If column name is not found in price data
    """

    validate_column_presence(df, column)

    rolling_mean = df[column].rolling(window=window).mean()
    rolling_std = df[column].rolling(window=window).std()

    return df.assign(
        **{
            f"{column}_bb_upper": rolling_mean + (2 * rolling_std),
            f"{column}_bb_lower": rolling_mean - (2 * rolling_std),
        }
    ).copy()


def calculate_rsi(prices: pd.Series, window: int = 14) -> pd.Series:
    """Calculate the Relative Strength Index

    RSI = 100 - (100 / (1 + RS))
    RS = Average Gain / Average Loss

    Parameters
    ----------
    prices : pd.Series
        Price Series (typically closing prices)
    window : int, optional
        Number of periods for RSI calculation, by default 14

    Returns
    -------
    pd.Series
        RSI values
    """
    if len(prices) < window:
        raise ValueError(
            f"Not enough data points ({len(prices)}) to calculate {window}-day RSI."  # noqa
        )
    delta = prices.diff()

    gain = delta.where(delta > 0, 0.0)
    loss = -delta.where(delta < 0, 0.0)

    avg_gain = gain.rolling(window=window, min_periods=window).mean()
    avg_loss = loss.rolling(window=window, min_periods=window).mean()

    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))

    return rsi


def calculate_macd(
    df: pd.DataFrame,
    column: str = "wti",
    fast: int = 12,
    slow: int = 26,
    signal: int = 9,
) -> pd.DataFrame:
    """Calculate MACD and MACD Signal Line

    MACD = EMA(12) - EMA(26)
    Signal Line = EMA(9) of MACD

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame containing price data
    column : str, optional
        Column to calculate MACD for, by default "wti"
    fast : int, optional
        Fast EMA period, by default 12
    slow : int, optional
        Slow EMA period, by default 26
    signal : int, optional
        Signal Line EMA period, by default 9

    Returns
    -------
    pd.DataFrame
        DataFrame with MACD and MACD Signal Columns

    Raises
    ------
    KeyError
        if column not found in DataFrame
    """
    validate_column_presence(df, column)

    if len(df) < slow:
        raise ValueError(
            f"Not enough data points ({len(df)}) to calculate {slow}-day MACD."
        )

    exp12 = df[column].ewm(span=fast, adjust=False).mean()
    exp26 = df[column].ewm(span=slow, adjust=False).mean()
    return df.assign(
        **{
            f"{column}_macd": exp12 - exp26,
            f"{column}_macd_signal": (exp12 - exp26)
            .ewm(span=signal, adjust=False)
            .mean(),
        }
    ).copy()


def add_technical_indicators(
    df: pd.DataFrame, columns: List[str]
) -> pd.DataFrame:
    """
    Add technical indicators to the oil price data.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame containing at least WTI prices.

    Returns
    -------
    pd.DataFrame
        DataFrame with new columns for technical indicators.
    """
    # Ensure Date index is sorted (just in case)
    df = df.sort_index()
    for column in columns:
        validate_column_presence(df, column)

        df = calculate_moving_average(df, column)
        df = calculate_bollinger_bands(df, column)
        df[f"{column}_rsi"] = calculate_rsi(df[column])
        df = calculate_macd(df, column)

    return df
