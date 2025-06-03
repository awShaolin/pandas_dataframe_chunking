import random
import numpy as np
import pandas as pd
import logging


class DataFrameGenerator:
    def __init__(self,
                 start_date: str,
                 end_date: str,
                 n_rows: int,
                 freq: str,
                 n_unique_dates: int = 100,
                 random_seed: int = None):
        """
        :param start_date: Start date string or datetime.
        :param end_date: End date string or datetime.
        :param n_rows: Total number of rows to generate.
        :param freq: Frequency ('s', 'min', 'h', 'd').
        :param n_unique_dates: How many unique dates to generate.
        :param random_seed: Seed for reproducibility.
        """
        self.start_date = pd.to_datetime(start_date)
        self.end_date = pd.to_datetime(end_date)
        self.n_rows = n_rows
        self.freq = freq
        self.n_unique_dates = n_unique_dates
        self.random_seed = random_seed

        if self.random_seed is not None:
            self.rng = np.random.default_rng(self.random_seed)
            random.seed(self.random_seed)
        else:
            self.rng = np.random.default_rng()

        self._validate_params()

        logging.info(f"DataFrameGenerator initialized with: "
                     f"start_date={self.start_date}, end_date={self.end_date}, "
                     f"n_rows={self.n_rows}, freq='{self.freq}', "
                     f"n_unique_dates={self.n_unique_dates}, random_seed={self.random_seed}")


    def _validate_params(self):
        if self.start_date >= self.end_date:
            raise ValueError("start_date must be before end_date")
        if self.n_rows < 0:
            raise ValueError("n_rows must be positive")
        if self.freq not in ["s", "min", "h", "d"]:
            raise ValueError("freq must be one of s, min, h, d")
        if self.n_unique_dates > self.n_rows:
            raise ValueError("n_unique_dates must be less than or equal to n_rows")


    def generate(self) -> pd.DataFrame:
        """
        Generate DataFrame based on start date and end date.
        :return: pd.DataFrame
        """
        freq_to_pandas = {
            's': 'S',
            'min': 'min',
            'h': 'H',
            'd': 'D'
        }
        unique_dates = pd.date_range(start=self.start_date, end=self.end_date, freq=freq_to_pandas[self.freq])
        if len(unique_dates) < self.n_unique_dates:
            raise ValueError("Not enough unique dates can be generated with given range and frequency.")

        selected_dates = self.rng.choice(unique_dates, size=self.n_unique_dates, replace=False)
        repeated_dates = self.rng.choice(selected_dates, size=self.n_rows, replace=True)
        repeated_dates = self.rng.permutation(repeated_dates)
        df = pd.DataFrame({'date': repeated_dates})

        logging.info(f"Generated DataFrame with {df.shape[0]} rows and {self.n_unique_dates} unique dates.")
        return df
