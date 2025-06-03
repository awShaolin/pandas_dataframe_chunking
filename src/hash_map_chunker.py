import logging

import pandas as pd
from logger_config import *

class HashMapChunker:
    def __init__(self,
                 min_chunk_size: int,
                 column: str = "date"):
        """
        :param min_chunk_size: minimum chunk size
        :param column: column name for chunking
        """

        if min_chunk_size < 1:
            logging.error("min_chunk_size must be greater than 0.")
            raise ValueError("min_chunk_size must be greater than 0.")
        self.min_chunk_size = min_chunk_size
        self.column = column

    def chunk(self, df: pd.DataFrame) -> list:
        """

        :param df:
        :return: list of chunks
        """
        if self.column not in df.columns:
            logging.error("Column name must be in df.")
            raise ValueError("Column name must be in df.")

        logging.info(f"Starting chunking with min_chunk_size={self.min_chunk_size} on column='{self.column}'")

        grouped = {}
        # for idx, row in df.iterrows():
        #     key = row[self.column]
        #     if key not in grouped:
        #         grouped[key] = []
        #     grouped[key].append(idx)

        for row in df.itertuples(index=True):
            idx = row.Index
            key = getattr(row, self.column)
            grouped.setdefault(key, []).append(idx)

        logging.info(f"Grouped into {len(grouped)} unique keys.")
        logging.debug(grouped)

        chunks = []
        current_chunk_indices = []

        for key in sorted(grouped.keys()):
            indices = grouped[key]
            current_chunk_indices.extend(indices)

            if len(current_chunk_indices) >= self.min_chunk_size:
                chunk = df.loc[current_chunk_indices]
                chunks.append(chunk)
                current_chunk_indices = []

        if current_chunk_indices:
            chunk = df.loc[current_chunk_indices]
            chunks.append(chunk)

        logging.info(f"Finished chunking with min_chunk_size={self.min_chunk_size}")
        logging.info(f"Created {len(chunks)} chunks.")
        return chunks

