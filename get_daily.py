import datetime
import logging
import holidays
import os
import pathlib
import time
from urllib3.exceptions import MaxRetryError

import pandas as pd
from polygon import RESTClient

RAWDATA_PATH = pathlib.Path("/home/lomzem/coding/LomzemStock/daily_stuff/rawdailydata/")


pd.options.display.max_columns = None


def get_polygon_daily(date: datetime.date):
    client = RESTClient(os.getenv("POLYGON_API_KEY"))
    while True:
        try:
            resp = client.get_grouped_daily_aggs(date.strftime("%Y-%m-%d"))
            break
        except MaxRetryError:
            logging.debug("Request limit exceeded, waiting 15s")
            time.sleep(15)
            continue

    df = pd.DataFrame(resp)

    df = df[
        ["timestamp", "open", "high", "low", "close", "volume", "vwap", "transactions"]
    ]
    df.rename(columns={"timestamp": "date", "transactions": "trades"}, inplace=True)
    df.dropna(inplace=True)

    # convert timestamp to date
    df["date"] = pd.to_datetime(df["date"], unit="ms").dt.date

    # assign types
    df = df.astype(
        {
            "open": "float32",
            "high": "float32",
            "low": "float32",
            "close": "float32",
            "volume": "int32",
            "vwap": "float32",
            "trades": "int32",
            "date": "datetime64[ns]",
        }
    )
    path = RAWDATA_PATH / f"{date.year}-{date.month}-{date.day}.parquet"
    df.to_parquet(path)
    logging.info(f"Done with {date}")


def main():
    OLDEST_DATE = datetime.date(2023, 12, 1)
    current_date = datetime.date.today()

    # loop in reverse from today to oldest
    for date in reversed(pd.date_range(OLDEST_DATE, current_date)):
        # check if file already exists:
        if pathlib.Path(
            RAWDATA_PATH / f"{date.year}-{date.month}-{date.day}.parquet"
        ).exists():
            logging.debug(f"Skipping {date}, already exists")
            continue

        # check if date is a weekend:
        if date.weekday() >= 5:
            logging.debug(f"Skipping {date}, is a weekend")
            continue

        # check if date is a holiday:
        if date in holidays.US():
            logging.debug(f"Skipping {date}, is a holiday")
            continue

        # make sure current_date is past 7pm or one day in the past:
        if date.date() == current_date and datetime.datetime.now().hour <= 19:
            logging.debug(f"Skipping {date}, too soon after market close")
            continue

        get_polygon_daily(date)

    print("Done!")


if __name__ == "__main__":
    # logging.basicConfig(level=logging.DEBUG)
    logging.basicConfig(level=logging.INFO)
    main()
