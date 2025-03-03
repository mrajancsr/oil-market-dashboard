import pandas as pd


def add_technical_indicators(df: pd.DataFrame) -> pd.DataFrame:
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

    # --- Moving Averages ---
    df["WTI_MA50"] = df["WTI"].rolling(window=50).mean()
    df["WTI_MA200"] = df["WTI"].rolling(window=200).mean()

    # --- Bollinger Bands ---
    rolling_mean = df["WTI"].rolling(window=20).mean()
    rolling_std = df["WTI"].rolling(window=20).std()
    df["WTI_BB_Upper"] = rolling_mean + (2 * rolling_std)
    df["WTI_BB_Lower"] = rolling_mean - (2 * rolling_std)

    # --- RSI ---
    delta = df["WTI"].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
    rs = gain / loss
    df["WTI_RSI"] = 100 - (100 / (1 + rs))

    # --- MACD ---
    exp12 = df["WTI"].ewm(span=12, adjust=False).mean()
    exp26 = df["WTI"].ewm(span=26, adjust=False).mean()
    df["WTI_MACD"] = exp12 - exp26
    df["WTI_MACD_Signal"] = df["WTI_MACD"].ewm(span=9, adjust=False).mean()

    return df
