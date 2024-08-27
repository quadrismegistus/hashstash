from hashstash.profilers import *
import pytest
import numpy as np
import pandas as pd

def test_generate_primitive():
    result = generate_primitive()
    assert isinstance(result, (int, float, str, bool)) or result is None

def test_generate_numpy_array():
    result = generate_numpy_array()
    assert isinstance(result, np.ndarray)
    assert 1 <= result.ndim <= 3
    assert all(1 <= dim <= 100 for dim in result.shape)

def test_generate_pandas_dataframe():
    result = generate_pandas_dataframe()
    assert isinstance(result, pd.DataFrame)
    assert 1 <= result.shape[0] <= 100
    assert 1 <= result.shape[1] <= 10

def test_generate_pandas_series():
    result = generate_pandas_series()
    assert isinstance(result, pd.Series)
    assert 1 <= len(result) <= 100

def test_generate_complex_data():
    result = generate_complex_data(1000)
    assert isinstance(result, dict)
    assert "nested_structure" in result
    assert isinstance(result["dataframe"], pd.DataFrame)
    assert isinstance(result["numpy_array"], np.ndarray)
    assert isinstance(result["series"], pd.Series)
    assert isinstance(result["large_list"], list)
    assert isinstance(result["large_dict"], dict)

@pytest.mark.parametrize("data_type", [
    "list", "dict", "numpy", "pandas_df", "pandas_series"
])
def test_generate_data(data_type):
    result = generate_data(1000, data_types=[data_type])
    if data_type == "list":
        assert isinstance(result, list)
    elif data_type == "dict":
        assert isinstance(result, dict)
    elif data_type == "numpy":
        assert isinstance(result, np.ndarray)
    elif data_type == "pandas_df":
        assert isinstance(result, pd.DataFrame)
    elif data_type == "pandas_series":
        assert isinstance(result, pd.Series)

def test_generate_list():
    result = generate_list()
    assert isinstance(result, list)
    assert len(result) <= 10
    assert all(isinstance(item, (int, float, str, bool)) or item is None for item in result)

def test_generate_dict():
    result = generate_dict()
    assert isinstance(result, dict)
    assert len(result) <= 10
    assert all(isinstance(value, list) for value in result.values())

def test_time_function():
    def dummy_function(x):
        time.sleep(0.1)
        return x * 2

    result, execution_time = time_function(dummy_function, 5)
    assert result == 10
    assert isinstance(execution_time, float)
    assert execution_time > 0

def test_get_data_type():
    assert isinstance(get_data_type(42), str)
    assert isinstance(get_data_type("hello"), str)
    assert isinstance(get_data_type([1, 2, 3]), str)

def test_hashstash_profiler():
    with HashStash().tmp() as stash:
        profiler = HashStashProfiler(stash)
        
        # Test profile method
        result = profiler.profile(size=1000, iterations=2, num_proc=1, stash=stash)
        assert isinstance(result, pd.DataFrame)
        assert len(result) > 0
        assert all(column in result.columns for column in [
            'Engine', 'Serializer', 'Encoding', 'Data Type', 'Size (MB)',
            'Raw Size (MB)', 'Operation', 'Time (s)', 'Rate (it/s)',
            'Speed (MB/s)', 'Cached Size (MB)', 'Compression Ratio (%)'
        ])

def test_profile_one():
    with HashStash().tmp() as stash:
        profiler = HashStashProfiler(stash)
        
        result = profiler.profile_one(stash, size=1000)
        assert isinstance(result, list)
        assert len(result) > 0
        assert all(isinstance(item, dict) for item in result)
        assert all(key in result[0] for key in [
            'Engine', 'Serializer', 'Encoding', 'Data Type', 'Size (MB)',
            'Raw Size (MB)', 'Operation', 'Time (s)', 'Rate (it/s)',
            'Speed (MB/s)', 'Cached Size (MB)', 'Compression Ratio (%)'
        ])

def test_profile_df():
    with HashStash().tmp() as stash:
        profiler = HashStashProfiler(stash)
        
        df = profiler.profile(size=1000, iterations=2, num_proc=1, stash=stash)
        result = profiler.profile_df(df=df)
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) > 0
        assert all(column in result.columns for column in [
            'Time (s)', 'Rate (it/s)', 'Speed (MB/s)', 'Cumulative Time (s)',
            'Cumulative Size (MB)', 'Cached Size (MB)', 'Compression Ratio (%)'
        ])

def test_run_profiles():
    result = HashStashProfiler.run_profiles(
        iterations=2,
        size=1000,
        engines=['memory'],
        serializers=['pickle'],
        num_procs=[1],
        num_proc=1,
        progress=False
    )
    
    assert isinstance(result, pd.DataFrame)
    assert len(result) > 0
    assert all(column in result.columns for column in [
        'Engine', 'Serializer', 'Encoding', 'Data Type', 'Size (MB)',
        'Raw Size (MB)', 'Operation', 'Time (s)', 'Rate (it/s)',
        'Speed (MB/s)', 'Cached Size (MB)', 'Compression Ratio (%)'
    ])