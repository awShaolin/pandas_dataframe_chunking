# pandas_dataframe_chunking
Pandas DataFrame Chunker is a Python project designed to efficiently split large pandas DataFrames into chunks based on a specified column.

## Prerequisites
- Python 3.8+
- pip (Python package manager)

## Installation
Clone the repository:
```bash
git clone https://github.com/awShaolin/pandas_dataframe_chunking
cd pandas_dataframe_chunking
```
Create and activate a virtual environment:
```bash
python3 -m venv venv
# mac/linux
source venv/bin/activate
#windows
venv\Scripts\activate
```
nstall the required packages:
```bash
pip install -r requirements.txt
```

## Components
**DataFrameGenerator**
- Generates random pandas DataFrames.
- Allows control over the number of rows, unique dates, frequency, and random seed.

**HashMapChunker**

HashMapChunker use the following algorythm: Creates hash-map (dict) with _key_: date and _value_: idx of ``pd.DataFrame`` raw. 
Based on this hash-map we get groups of distinct dates. Then it sorts groups and get indices sequentially. 
When the accumulated indices reach or exceed the minimum chunk size, create a new chunk from the DataFrame.

**Complexity:**

n - rows in df, m - unique keys (dates)

Time: O(n + m log m) 

Space: O(n + m log m) 

**Profiler**
Measures execution time and peak memory usage of functions.

**Tester**
Unit tests written using unittest and compatible with pytest.

## Testing
To run the tests:
```bash
cd pandas_dataframe_chunking # you should be in the root directory
pytest -v tests/test_chunker.py
```
Tests: 
- Chunking an empty DataFrame.
- Chunking a small DataFrame.
- Verifying chunk sizes.
- Working with multiple columns.
- Handling invalid column names.
- Stress testing with large datasets.
- Ensuring no overlap between chunked data.
- Checking total consistency of rows.