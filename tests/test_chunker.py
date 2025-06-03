import unittest
import pandas as pd
import numpy as np
from dataframe_generator import DataFrameGenerator
from hash_map_chunker import HashMapChunker


class Tester(unittest.TestCase):
    def setUp(self):
        generator_basic = DataFrameGenerator(
            start_date='2023-01-01',
            end_date='2023-01-10',
            n_rows=10,
            freq='d',
            n_unique_dates=10,
            random_seed=42
        )
        self.df_basic = generator_basic.generate()

        generator_multi = DataFrameGenerator(
            start_date='2023-01-01',
            end_date='2023-01-10',
            n_rows=20,
            freq='d',
            n_unique_dates=10,
            random_seed=42
        )
        self.df_multi = generator_multi.generate()
        self.df_multi['value'] = np.random.randint(0, 100, size=20)


    def test_empty_df(self):
        """Test that chunking an empty DataFrame returns an empty list of chunks."""
        df_empty = pd.DataFrame({'date':[]})
        chunker = HashMapChunker(5, 'date')
        chunks = chunker.chunk(df_empty)
        self.assertEqual(len(chunks), 0)


    def test_small_df_single_chunk(self):
        """Test that a small DataFrame smaller than min_chunk_size results in a single chunk."""
        chunker = HashMapChunker(20, 'date')
        chunks = chunker.chunk(self.df_basic)
        self.assertEqual(len(chunks), 1)


    def test_chunk_min_size(self):
        """Test that each chunk (except the last) has at least min_chunk_size rows."""
        chunker = HashMapChunker(3, 'date')
        chunks = chunker.chunk(self.df_basic)
        for chunk in chunks[:-1]:
            self.assertGreaterEqual(len(chunk), 3)


    def test_multiple_columns(self):
        """Test that the chunker preserves all columns when splitting the DataFrame."""
        chunker = HashMapChunker(4, 'date')
        chunks = chunker.chunk(self.df_multi)
        for chunk in chunks:
            self.assertIn('date', chunk.columns)
            self.assertIn('value', chunk.columns)


    def test_invalid_column_name(self):
        """Test that chunking with an invalid column name raises a ValueError."""
        chunker = HashMapChunker(5, 'invalid_column')
        with self.assertRaises(ValueError):
            chunker.chunk(self.df_basic)


    def test_large_dataset_stress(self):
        """Stress test the chunker with a large DataFrame to ensure it handles big datasets."""
        generator_large = DataFrameGenerator(
            start_date='2023-01-01',
            end_date='2023-02-01',
            n_rows=10000000,
            freq='min',
            n_unique_dates=100,
            random_seed=123
        )
        df_large = generator_large.generate()
        chunker = HashMapChunker(1000, 'date')
        chunks = chunker.chunk(df_large)
        total_rows = sum(len(chunk) for chunk in chunks)
        self.assertEqual(total_rows, len(df_large))


    def test_no_date_overlap(self):
        """Test that dates do not overlap between different chunks."""
        chunker = HashMapChunker(3, 'date')
        chunks = chunker.chunk(self.df_basic)
        seen_dates = set()
        for chunk in chunks:
            chunk_dates = set(chunk['date'])
            self.assertTrue(seen_dates.isdisjoint(chunk_dates))
            seen_dates.update(chunk_dates)


    def test_total_length_consistency(self):
        """Test that the total number of rows across all chunks matches the original DataFrame."""
        chunker = HashMapChunker(3, 'date')
        chunks = chunker.chunk(self.df_basic)
        total_rows = sum(len(chunk) for chunk in chunks)
        self.assertEqual(total_rows, len(self.df_basic))
