import unittest
import datetime
import pandas as pd


from combine_daily import combine_daily


class test_combine_daily(unittest.TestCase):
    def test_combine_daily(self):
        df = combine_daily()
        df = df.query("ticker == 'AAPL'")

        def compare_close_pclose(date_1, date_2):
            close_1 = df.loc[(df["date"] == date_1), "close"].values[0]
            close_2 = df.loc[(df["date"] == date_2), "pclose"].values[0]

            assert (
                close_1 == close_2
            ), f"Close for {date_1} is not equal to close for {date_2}, {close_1} != {close_2}"

        compare_close_pclose("2023-12-04", "2023-12-05")
        compare_close_pclose("2023-12-12", "2023-12-13")
