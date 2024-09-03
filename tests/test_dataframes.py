import pytest
import pandas as pd
import polars as pl
import tempfile
import os
from hashstash.utils.dataframes import *
logger.setLevel(logging.CRITICAL+1)

@pytest.fixture
def sample_data():
    return {
        'A': [1, 2, 3],
        'B': ['a', 'b', 'c'],
        'C': [4.0, 5.0, 6.0]
    }

def test_metadataframe_init(sample_data):
    mdf = MetaDataFrame(sample_data)
    assert isinstance(mdf.df, (pd.DataFrame, pl.DataFrame))
    assert list(mdf.columns) == ['A', 'B', 'C']

def test_metadataframe_to_pandas(sample_data):
    mdf = MetaDataFrame(sample_data, df_engine='polars')
    pandas_df = mdf.to_pandas()
    assert isinstance(pandas_df.df, pd.DataFrame)

def test_metadataframe_to_polars(sample_data):
    mdf = MetaDataFrame(sample_data, df_engine='pandas')
    polars_df = mdf.to_polars()
    assert isinstance(polars_df.df, pl.DataFrame)

def test_metadataframe_filter(sample_data):
    mdf = MetaDataFrame(sample_data)
    filtered = mdf.filter(mdf['A'] > 1)
    assert len(filtered) == 2

def test_metadataframe_select_columns(sample_data):
    mdf = MetaDataFrame(sample_data)
    selected = mdf.select_columns(['A', 'B'])
    assert list(selected.columns) == ['A', 'B']

def test_metadataframe_assign():
    mdf = MetaDataFrame({'A': [1, 2, 3]})
    result = mdf.assign(B=lambda x: x['A'] * 2, C=10)
    assert list(result.columns) == ['A', 'B', 'C']
    assert result['B'].tolist() == [2, 4, 6]
    assert result['C'].tolist() == [10, 10, 10]

def test_get_working_io_engines():
    engines = get_working_io_engines()
    assert isinstance(engines, set)
    assert 'csv' in engines
    assert 'json' in engines

def test_get_working_df_engines():
    engines = get_working_df_engines()
    assert isinstance(engines, set)
    assert 'pandas' in engines
    assert 'polars' in engines

def test_has_index():
    df_with_index = pd.DataFrame({'A': [1, 2, 3]}).set_index('A')
    df_without_index = pd.DataFrame({'A': [1, 2, 3]})
    
    assert has_index(df_with_index) == True
    assert has_index(df_without_index) == False

def test_reset_index():
    df = pd.DataFrame({'A': [1, 2, 3]}).set_index('A')
    reset_df = reset_index(df)
    assert 'A' in reset_df.columns
    assert has_index(reset_df) == False

def test_set_index():
    df = pd.DataFrame({'A': [1, 2, 3], 'B': ['a', 'b', 'c']})
    indexed_df = set_index(df, index_columns=['A'])
    assert has_index(indexed_df) == True
    assert indexed_df.index.name == 'A'

# New tests

def test_metadataframe_getitem(sample_data):
    mdf = MetaDataFrame(sample_data)
    assert mdf['A'].tolist() == [1, 2, 3]
    assert isinstance(mdf[['A', 'B']], MetaDataFrame)

def test_metadataframe_setitem(sample_data):
    mdf = MetaDataFrame(sample_data)
    mdf['D'] = [7, 8, 9]
    assert 'D' in mdf.columns
    assert mdf['D'].tolist() == [7, 8, 9]

def test_metadataframe_len(sample_data):
    mdf = MetaDataFrame(sample_data)
    assert len(mdf) == 3

def test_metadataframe_applymap():
    mdf = MetaDataFrame({'A': [1, 2, 3], 'B': [4, 5, 6]})
    result = mdf.applymap(lambda x: x * 2)
    assert result['A'].tolist() == [2, 4, 6]
    assert result['B'].tolist() == [8, 10, 12]

def test_metadataframe_max(sample_data):
    mdf = MetaDataFrame(sample_data)
    assert mdf.max()['A'] == 3
    assert mdf.max()['C'] == 6.0

def test_metadataframe_eq(sample_data):
    mdf1 = MetaDataFrame(sample_data)
    mdf2 = MetaDataFrame(sample_data)
    assert mdf1 == mdf2

def test_metadataframe_merge():
    mdf1 = MetaDataFrame({'A': [1, 2, 3], 'B': ['a', 'b', 'c']})
    mdf2 = MetaDataFrame({'A': [2, 3, 4], 'C': ['x', 'y', 'z']})
    merged = mdf1.merge(mdf2, on='A')
    assert list(merged.columns) == ['A', 'B', 'C']
    assert len(merged) == 2

def test_metadataframe_concat():
    mdf1 = MetaDataFrame({'A': [1, 2], 'B': ['a', 'b']})
    mdf2 = MetaDataFrame({'A': [3, 4], 'B': ['c', 'd']})
    concatenated = mdf1.concat(mdf2)
    assert len(concatenated) == 4
    assert concatenated['A'].tolist() == [1, 2, 3, 4]

@pytest.mark.parametrize("io_engine", ["csv", "parquet", "json", "feather", "pickle"])
def test_metadataframe_write_read(sample_data, io_engine):
    mdf = MetaDataFrame(sample_data)
    with tempfile.NamedTemporaryFile(suffix=f".{io_engine}") as tmp:
        mdf.write(tmp.name, io_engine=io_engine, compression=RAW_NO_COMPRESS)
        read_mdf = MetaDataFrame.read(tmp.name, io_engine=io_engine, compression=RAW_NO_COMPRESS)
        assert list(read_mdf.columns) == list(mdf.columns)
        assert read_mdf.shape == mdf.shape

def test_get_io_engine():
    assert get_io_engine("csv") == "csv"
    with pytest.raises(ValueError):
        get_io_engine("invalid_engine")

def test_check_io_engine():
    assert check_io_engine("csv") == True
    assert check_io_engine("invalid_engine") == False

def test_check_df_engine():
    assert check_df_engine("pandas") == True
    assert check_df_engine("polars") == True
    assert check_df_engine("invalid_engine") == False

def test_get_df_engine():
    assert get_df_engine("pandas") == "pandas"
    with pytest.raises(ValueError):
        get_df_engine("invalid_engine")

def test_get_dataframe_engine():
    pd_df = pd.DataFrame({'A': [1, 2, 3]})
    pl_df = pl.DataFrame({'A': [1, 2, 3]})
    assert get_dataframe_engine(pd_df) == "pandas"
    assert get_dataframe_engine(pl_df) == "polars"

def test_set_index_with_prefix():
    df = pd.DataFrame({'_A': [1, 2, 3], 'B': ['a', 'b', 'c']})
    indexed_df = set_index(df, prefix_columns='_', reset_prefix=True)
    assert has_index(indexed_df) == True
    assert indexed_df.index.name == 'A'

def test_reset_index_with_prefix():
    df = pd.DataFrame({'A': [1, 2, 3], 'B': ['a', 'b', 'c']}).set_index('A')
    reset_df = reset_index(df, prefix_columns='_')
    assert '_A' in reset_df.columns
    assert has_index(reset_df) == False

# Add more tests as needed for other functions and edge cases

def test_metadataframe_to_dict(sample_data):
    mdf = MetaDataFrame(sample_data)
    result = mdf.to_dict()
    assert isinstance(result, dict)
    assert 'data' in result
    assert 'df_engine' in result

@pytest.mark.parametrize("file_format", ["csv", "parquet", "json", "feather"])
def test_metadataframe_write_methods(sample_data, file_format, tmp_path):
    mdf = MetaDataFrame(sample_data)
    file_path = tmp_path / f"test.{file_format}"
    
    if file_format == "csv":
        mdf.to_csv(file_path)
    elif file_format == "parquet":
        mdf.to_parquet(file_path)
    elif file_format == "json":
        mdf.to_json(file_path)
    elif file_format == "feather":
        mdf.to_feather(file_path)
    
    assert file_path.exists()

def test_metadataframe_concat():
    mdf1 = MetaDataFrame({'A': [1, 2], 'B': ['a', 'b']})
    mdf2 = MetaDataFrame({'A': [3, 4], 'B': ['c', 'd']})
    result = mdf1.concat(mdf2)
    assert len(result) == 4
    assert list(result.columns) == ['A', 'B']

def test_metadataframe_assign():
    mdf = MetaDataFrame({'A': [1, 2, 3]})
    result = mdf.assign(B=lambda x: x['A'] * 2, C=10)
    assert list(result.columns) == ['A', 'B', 'C']
    assert result['B'].tolist() == [2, 4, 6]
    assert result['C'].tolist() == [10, 10, 10]

def test_get_working_io_engines():
    engines = get_working_io_engines()
    assert isinstance(engines, set)
    assert 'csv' in engines
    assert 'json' in engines

def test_get_io_engine():
    assert get_io_engine('csv') == 'csv'
    with pytest.raises(ValueError):
        get_io_engine('invalid_engine')

def test_check_io_engine():
    assert check_io_engine('csv') == True
    assert check_io_engine('invalid_engine') == False

def test_get_working_df_engines():
    engines = get_working_df_engines()
    assert isinstance(engines, set)
    assert 'pandas' in engines
    assert 'polars' in engines

def test_check_df_engine():
    assert check_df_engine('pandas') == True
    assert check_df_engine('polars') == True
    assert check_df_engine('invalid_engine') == False

def test_get_df_engine():
    assert get_df_engine('pandas') == 'pandas'
    with pytest.raises(ValueError):
        get_df_engine('invalid_engine')

def test_get_dataframe_engine():
    pd_df = pd.DataFrame({'A': [1, 2, 3]})
    pl_df = pl.DataFrame({'A': [1, 2, 3]})
    mdf = MetaDataFrame({'A': [1, 2, 3]})
    
    assert get_dataframe_engine(pd_df) == 'pandas'
    assert get_dataframe_engine(pl_df) == 'polars'
    assert get_dataframe_engine(mdf) == mdf.df_engine
    assert get_dataframe_engine([1, 2, 3]) is None

def test_reset_index():
    df = pd.DataFrame({'A': [1, 2, 3], 'B': ['a', 'b', 'c']}).set_index('A')
    result = reset_index(df, prefix_columns='_')
    assert '_A' in result.columns
    assert has_index(result) == False

def test_set_index():
    df = pd.DataFrame({'A': [1, 2, 3], 'B': ['a', 'b', 'c']})
    result = set_index(df, index_columns=['A'])
    assert has_index(result) == True
    assert result.index.name == 'A'

    df = pd.DataFrame({'_A': [1, 2, 3], 'B': ['a', 'b', 'c']})
    result = set_index(df, prefix_columns='_', reset_prefix=True)
    assert has_index(result) == True
    assert result.index.name == 'A'

def test_has_index():
    pd_df = pd.DataFrame({'A': [1, 2, 3]}).set_index('A')
    pl_df = pl.DataFrame({'A': [1, 2, 3]})
    
    assert has_index(pd_df) == True
    assert has_index(pl_df) == False
    with pytest.raises(ValueError):
        has_index([1, 2, 3])

    with pytest.raises(ValueError):
        has_index("invalid_df")

# Add more tests for edge cases and error handling
def test_metadataframe_invalid_engine():
    with pytest.raises(ValueError):
        MetaDataFrame({'A': [1, 2, 3]}, df_engine='invalid_engine')

def test_metadataframe_read_invalid_engine():
    with pytest.raises(ValueError):
        MetaDataFrame.read('test.csv', io_engine='invalid_engine')

def test_metadataframe_write_invalid_engine(sample_data, tmp_path):
    mdf = MetaDataFrame(sample_data)
    file_path = tmp_path / "test.invalid"
    with pytest.raises(ValueError):
        mdf.write(file_path, io_engine='invalid_engine')