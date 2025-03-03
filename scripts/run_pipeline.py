import os
from datetime import date
from typing import Dict

import pandas as pd

from oil_dashboard.pipeline.feature_engineering import generate_features
from oil_dashboard.pipeline.oil_pipeline import OilPipeLine


def main():
    api_key = os.getenv("EIA_API_KEY")
    if not api_key:
        raise ValueError("EIA_API_KEY environment variable is required")

    pipeline = OilPipeLine(
        start_date=date(2023, 1, 1), end_date=date.today(), api_key=api_key
    )

    os.makedirs("data", exist_ok=True)

    print("Fetching data sources...")
    data_frames: Dict[str, pd.DataFrame] = pipeline.fetch_all_data()

    print("Generating Features...")
    master_df = generate_features(data_frames)
    master_df.reset_index(inplace=True)
    master_df.rename(columns={"index": "Date"}, inplace=True)

    print("Saving data to CSV...")
    filename = f"data/oil_market_data.csv"
    master_df.to_csv(filename, index=False)

    print(f"Pipeline completed successfully! Data saved to {filename}")


if __name__ == "__main__":
    main()
