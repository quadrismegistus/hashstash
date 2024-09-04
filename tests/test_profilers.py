import sys; sys.path.append('..')
from hashstash.profilers import *
logger.setLevel(logging.CRITICAL+1)
import pytest
import numpy as np
import pandas as pd

def test_generate_primitive():
    result = generate_primitive()
    assert isinstance(result, (int, float, str, bool)) or result is None


@pytest.mark.parametrize("data_type", [
    "primitive", "list", "dict",  "pandas_df",  "meta_df"
])
def test_generate_data(data_type):
    result = generate_data(1000, data_type=data_type)
    if data_type == "primitive":
        assert isinstance(result, (int, float, str, bool)) or result is None
    elif data_type == "list":
        assert isinstance(result, list)
    elif data_type == "dict":
        assert isinstance(result, dict)
    elif data_type == "pandas_df":
        #print(type(result))
        assert isinstance(result, pd.DataFrame)
    elif data_type == "meta_df":
        assert isinstance(result, MetaDataFrame)

def test_generate_data_dataframe():
    result = generate_data_dataframe(1000).df
    assert isinstance(result, pd.DataFrame)
    assert result.shape[0] > 0
    assert result.shape[1] > 0
    assert 'id' in result.columns
    assert all(col.startswith(('int_', 'float_', 'str_', 'bool_')) for col in result.columns if col != 'id')

def test_generate_dict():
    result = generate_dict(1000)
    assert isinstance(result, dict)
    assert len(result) > 0
    assert all(isinstance(key, str) and isinstance(value, list) for key, value in result.items())

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
        #print(result.columns)
        assert all(column in result.columns for column in [
            'Engine', 'Serializer', 'Encoding', 'Data Type', 'Size (B)',
            'Raw Size (B)', 'Iteration', 'Num Proc'
        ])

def test_profile_one():
    with HashStash().tmp() as stash:
        profiler = HashStashProfiler(stash)
        
        result = profile_stash_transaction(stash, size=1000)
        assert isinstance(result, dict)
        assert len(result) > 0
        #pprint(result)
        for key in [
            'Raw Size (B)', 'Set Time (s)', 'Get Time (s)',
            'Encode Time (s)', 'Decode Time (s)', 'Serialize Time (s)',
            'Deserialize Time (s)',
        ]:
            assert key in result

# def test_run_profiles():
#     result = HashStashProfiler.run_profiles(
#         iterations=2,
#         size=1000,
#         engines=['memory'],
#         serializers=['pickle'],
#         num_procs=[1],
#         num_proc=1,
#         progress=False
#     )
    
#     assert isinstance(result, pd.DataFrame)
#     assert len(result) > 0
#     #print(result.columns)
#     assert all(column in result.columns for column in [
#         'Engine', 'Serializer', 'Encoding', 'Data Type', 'Size (B)','Iteration', 'Num Proc',
#         'Raw Size (B)', 'Set Time (s)', 'Get Time (s)',
#         'Encode Time (s)', 'Decode Time (s)', 'Serialize Time (s)',
#         'Deserialize Time (s)',

#     ])

# def test_get_size_data():
#     result = HashStashProfiler.get_size_data(
#         iterations=2,
#         size=1000,
#         engines=['memory'],
#         serializers=['pickle'],
#         num_procs=[1],
#         num_proc=1,
#         progress=False
#     )
    
#     assert isinstance(result, pd.DataFrame)
#     assert len(result) > 0
#     assert all(column in result.columns for column in [
#         'Engine', 'Serializer', 'Encoding', 'Data Type', 'Size (B)',
#         'Raw Size (B)', 'Iteration', 'Num Proc', 'Cumulative Raw Size (B)',
#         'Raw Size (KB)', 'Cumulative Serialized Size (B)', 'Serialized Size (KB)',
#         'Serialized Compression Ratio', 'Cumulative Encoded Size (B)',
#         'Encoded Size (KB)', 'Encoded Compression Ratio'
#     ])

# def test_get_profile_data():
#     result = HashStashProfiler.get_profile_data(
#         iterations=2,
#         size=10,
#         engines=['memory'],
#         serializers=['pickle'],
#         num_procs=[1],
#         num_proc=1,
#         progress=False
#     ).reset_index()
    
#     assert isinstance(result, pd.DataFrame)
#     assert len(result) > 0
#     for column in [
#         'Data Type', 'Engine', 'Serializer', 'Encoding', 'Num Proc',
#         'Operation', 'Iteration', 'Raw Size (B)', 'Time (s)', 'Rate (it/s)',
#         'Speed (MB/s)', 'Time (ms) Rolling', 'Rate (it/s) Rolling',
#         'Speed (MB/s) Rolling'
#     ]:
#         assert column in result.columns

# def test_plot():
#     df = HashStashProfiler.get_profile_data(
#         iterations=2,
#         size=10,
#         engines=['memory'],
#         serializers=['pickle'],
#         num_procs=[1],
#         num_proc=1,
#         progress=False
#     )
    
#     plot = HashStashProfiler.plot(df)
#     assert plot is not None
