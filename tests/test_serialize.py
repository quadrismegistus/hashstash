import pytest
import numpy as np
import pandas as pd
from hashdict.utils.serialize import Serializer, Deserializer

@pytest.fixture
def serializer():
    return Serializer()

@pytest.fixture
def deserializer():
    return Deserializer()

def test_serializer_jsonable(serializer):
    assert serializer.get(42) == 42
    assert serializer.get("hello") == "hello"
    assert serializer.get([1, 2, 3]) == [1, 2, 3]
    assert serializer.get({"a": 1, "b": 2}) == {"a": 1, "b": 2}

def test_serializer_numpy(serializer):
    arr = np.array([1, 2, 3])
    result = serializer.get(arr)
    assert result['py/object'] == 'numpy.ndarray'
    assert result['values'] == [1, 2, 3]
    assert result['dtype'] == 'int64'
    assert result['shape'] == (3,)

def test_serializer_pandas_df(serializer):
    df = pd.DataFrame({'A': [1, 2, 3], 'B': [4, 5, 6]})
    result = serializer.get(df)
    assert result['py/object'] == 'pandas.core.frame.DataFrame'
    assert result['values'] == [[1, 4], [2, 5], [3, 6]]
    assert result['columns'] == ['A', 'B']
    assert result['index'] == []

def test_serializer_pandas_series(serializer):
    series = pd.Series([1, 2, 3], name='test')
    result = serializer.get(series)
    assert result['py/object'] == 'pandas.core.series.Series'
    assert result['values'] == [1, 2, 3]
    assert result['index'] == [0, 1, 2]
    assert result['name'] == 'test'
    assert result['dtype'] == 'int64'

def test_serializer_function(serializer):
    def test_func(x):
        return x * 2
    
    result = serializer.get(test_func)
    assert 'py/function' in result
    assert 'test_func' in result['py/function']
    assert 'def test_func(x):' in result['__src__']
    assert 'return x * 2' in result['__src__']

def test_deserializer_jsonable(deserializer):
    assert deserializer.get(42) == 42
    assert deserializer.get("hello") == "hello"
    assert deserializer.get([1, 2, 3]) == [1, 2, 3]
    assert deserializer.get({"a": 1, "b": 2}) == {"a": 1, "b": 2}

def test_deserializer_numpy(deserializer):
    serialized = {
        'py/object': 'numpy.ndarray',
        'values': [1, 2, 3],
        'dtype': 'int64',
        'shape': (3,)
    }
    result = deserializer.get_numpy(serialized)
    assert isinstance(result, np.ndarray)
    assert np.array_equal(result, np.array([1, 2, 3]))

def test_deserializer_pandas_df(deserializer):
    serialized = {
        'py/object': 'pandas.core.frame.DataFrame',
        'values': [[1, 4], [2, 5], [3, 6]],
        'columns': ['A', 'B'],
        'index': []
    }
    result = deserializer.get_pandas_df(serialized)
    assert isinstance(result, pd.DataFrame)
    assert result.equals(pd.DataFrame({'A': [1, 2, 3], 'B': [4, 5, 6]}))

def test_deserializer_pandas_series(deserializer):
    serialized = {
        'py/object': 'pandas.core.series.Series',
        'values': [1, 2, 3],
        'index': [0, 1, 2],
        'name': 'test',
        'dtype': 'int64'
    }
    result = deserializer.get_pandas_series(serialized)
    assert isinstance(result, pd.Series)
    assert result.equals(pd.Series([1, 2, 3], name='test'))

def test_deserializer_function(deserializer):
    serialized = {
        'py/function': 'test_serialize.test_deserializer_function.<locals>.test_func',
        '__src__': 'def test_func(x):\n    return x * 2\n'
    }
    result = deserializer.get(serialized)
    assert callable(result)
    assert result(3) == 6

def test_roundtrip(serializer, deserializer):
    original = {
        'int': 42,
        'str': 'hello',
        'list': [1, 2, 3],
        'dict': {'a': 1, 'b': 2},
        'numpy': np.array([1, 2, 3]),
        'pandas_df': pd.DataFrame({'A': [1, 2, 3], 'B': [4, 5, 6]}),
        'pandas_series': pd.Series([1, 2, 3], name='test')
    }
    
    serialized = serializer.get(original)
    deserialized = {}
    for key, value in serialized.items():
        if isinstance(value, dict) and 'py/object' in value:
            if 'numpy.ndarray' in value['py/object']:
                deserialized[key] = deserializer.get_numpy(value)
            elif 'pandas.core.frame.DataFrame' in value['py/object']:
                deserialized[key] = deserializer.get_pandas_df(value)
            elif 'pandas.core.series.Series' in value['py/object']:
                deserialized[key] = deserializer.get_pandas_series(value)
            else:
                deserialized[key] = deserializer.get(value)
        else:
            deserialized[key] = deserializer.get(value)
    
    assert deserialized['int'] == original['int']
    assert deserialized['str'] == original['str']
    assert deserialized['list'] == original['list']
    assert deserialized['dict'] == original['dict']
    assert np.array_equal(deserialized['numpy'], original['numpy'])
    assert deserialized['pandas_df'].equals(original['pandas_df'])
    assert deserialized['pandas_series'].equals(original['pandas_series'])