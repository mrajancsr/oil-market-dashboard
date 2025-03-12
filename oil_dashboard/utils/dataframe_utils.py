"""The purpose of this file is to perform validation, checking, fixing"""

import pandas as pd


def validate_column_presence(df: pd.DataFrame, column: str):
    if column not in df:
        raise KeyError(
            f"Column '{column}' not found in DataFrame. "
            f"Available columns: {df.columns.tolist()}"
        )

    # Rest of your logic...
