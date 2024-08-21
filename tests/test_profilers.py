from hashstash.profilers import *
import pytest
import numpy as np
import pandas as pd
import pytest
import pandas as pd
import numpy as np
from hashstash.profilers.engine_profiler import HashStashProfiler, run_engine_profile
from hashstash import HashStash


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
    "primitive", "list", "dict", "numpy", "pandas_df", "pandas_series"
])
def test_generate_data(data_type):
    result = generate_data(1000, data_types=[data_type])
    if data_type == "primitive":
        assert isinstance(result, (int, float, str, bool)) or result is None
    elif data_type == "list":
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
    assert 'int' in get_data_type(42)
    assert 'str' in get_data_type("hello")
    assert 'list' in get_data_type([1, 2, 3])

def test_compare_serializers():
    obj = {"test": "data"}
    results = compare_serializers(obj)
    assert isinstance(results, list)
    assert len(results) > 0
    assert all(isinstance(result, dict) for result in results)
    assert all(key in results[0] for key in [
        'serializer_name', 'data_type', 'encoding', 'serialize_speed',
        'deserialize_speed', 'serialize_time', 'deserialize_time',
        'encode_time', 'decode_time', 'encode_serialize_time',
        'decode_deserialize_time', 'size_mb', 'input_size_mb'
    ])

def test_run_comparison():
    results = run_comparison(10)
    assert isinstance(results, list)
    assert len(results) > 0
    assert all(isinstance(result, dict) for result in results)

@pytest.mark.parametrize("iterations", [1, 5])
def test_run_comparisons(iterations):
    results_df = run_comparisons(iterations=iterations, num_proc=1)
    assert isinstance(results_df, pd.DataFrame)
    assert len(results_df) > 0
    assert all(column in results_df.columns for column in [
        'serializer_name', 'data_type', 'encoding', 'serialize_speed',
        'deserialize_speed', 'serialize_time', 'deserialize_time',
        'encode_time', 'decode_time', 'encode_serialize_time',
        'decode_deserialize_time', 'size_mb', 'input_size_mb'
    ])

def test_run():
    results = run(iterations=2)
    assert isinstance(results, pd.DataFrame)
    assert len(results) > 0
    assert all(column in results.columns for column in [
        'data_type', 'serializer_name', 'encoding', 'serialize_speed',
        'deserialize_speed', 'serialize_time', 'deserialize_time',
        'encode_time', 'decode_time', 'input_size_mb', 'size_mb'
    ])

def test_hashstash_profiler():
    stash = HashStash().tmp()
    profiler = HashStashProfiler(stash)
    
    # Test profile method
    result = profiler.profile(size=[1000], iterations=2, num_proc=1)
    assert isinstance(result, pd.DataFrame)
    assert len(result) > 0
    assert all(column in result.columns for column in [
        'Engine', 'Compress', 'Base64', 'Size (MB)', 'Raw Size (MB)',
        'Operation', 'Time (s)', 'Rate (it/s)', 'Speed (MB/s)',
        'Cached Size (MB)', 'Compression Ratio (%)'
    ])

def test_profile_stash_transaction():
    stash = HashStash().tmp()
    profiler = HashStashProfiler(stash)
    
    result = profiler.profile_stash_transaction(stash, size=1000)
    assert isinstance(result, list)
    assert len(result) > 0
    assert all(isinstance(item, dict) for item in result)
    assert all(key in result[0] for key in [
        'Engine', 'Compress', 'Base64', 'Size (MB)', 'Raw Size (MB)',
        'Operation', 'Time (s)', 'Rate (it/s)', 'Speed (MB/s)',
        'Cached Size (MB)', 'Compression Ratio (%)'
    ])

def test_profile_df():
    stash = HashStash().tmp()
    profiler = HashStashProfiler(stash)
    
    df = profiler.profile(size=[1000], iterations=2, num_proc=1)
    result = profiler.profile_df(df=df)
    
    assert isinstance(result, pd.DataFrame)
    assert len(result) > 0
    assert all(column in result.columns for column in [
        'Time (s)', 'Rate (it/s)', 'Speed (MB/s)', 'Cumulative Time (s)',
        'Cumulative Size (MB)', 'Cached Size (MB)', 'Compression Ratio (%)'
    ])

def test_run_engine_profile():
    result = run_engine_profile(iterations=10)
    
    assert isinstance(result, pd.DataFrame)
    assert len(result) > 0
    assert all(column in result.columns for column in [
        'Engine', 'Compress', 'Base64', 'Operation', 'Speed (MB/s)',
        'Time (s)', 'Raw Size (MB)', 'Cached Size (MB)', 'Compression Ratio (%)'
    ])