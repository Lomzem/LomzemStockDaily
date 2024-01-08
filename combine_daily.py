import datetime
import logging
import os

import pandas as pd

from get_daily import RAWDATA_PATH


COMBINED_DAILY_PATH = RAWDATA_PATH.parent / "combined_daily.parquet"


def combine_daily() -> pd.DataFrame:
    parquet_files = [RAWDATA_PATH / file for file in os.listdir(RAWDATA_PATH)]
    df = pd.concat(pd.read_parquet(file) for file in parquet_files)
    df = df.sort_values(by=["date", "ticker"]).reset_index(drop=True)

    # add useful columns
    df["pclose"] = df.groupby("ticker").close.transform("shift")
    df.dropna(inplace=True)
    df["gap"] = df["open"] / df["pclose"] - 1
    df["change"] = df["close"] / df["open"] - 1
    df["closed_red"] = df["close"] < df["open"]

    df.to_parquet(COMBINED_DAILY_PATH)
    logging.info(f"Combined daily data to {COMBINED_DAILY_PATH}")
    return df


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    df = combine_daily()
