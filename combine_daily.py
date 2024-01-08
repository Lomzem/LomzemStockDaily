import datetime
import logging
import os

import pandas as pd

from get_daily import RAWDATA_PATH


def combine_daily() -> pd.DataFrame:
    parquet_files = [RAWDATA_PATH / file for file in os.listdir(RAWDATA_PATH)]
    df = pd.concat(pd.read_parquet(file) for file in parquet_files)
    df = df.sort_values(by=["date", "ticker"]).reset_index(drop=True)

    # add useful columns
    df["pclose"] = df.groupby("ticker").close.transform("shift")
    df.dropna(inplace=True)
    df["gap"] = df["close"] / df["pclose"] - 1

    filepath = RAWDATA_PATH.parent / "combined_daily.parquet"
    df.to_parquet(filepath)
    logging.info(f"Combined daily data to {filepath}")
    return df


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    df = combine_daily()
