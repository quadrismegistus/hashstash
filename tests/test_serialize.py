from hashstash import *
import numpy as np
import pandas as pd
config.set_serializer('custom')


def test_serialize_jsonable():
    assert json.loads(serialize(42)) == 42
    assert json.loads(serialize("hello")) == "hello"
    assert json.loads(serialize([1, 2, 3])) == [1, 2, 3]
    assert json.loads(serialize({"a": 1, "b": 2})) == {"a": 1, "b": 2}

def test_serialize_numpy():
    arr = np.array([1, 2, 3])
    result = json.loads(serialize(arr))
    assert result[OBJ_ADDR_KEY] == 'numpy.ndarray'
    assert result[OBJ_ARGS_KEY] == [b64encode(arr.tobytes()).decode('utf-8')]

def test_serialize_pandas_df():
    df = pd.DataFrame({'A': [1, 2, 3], 'B': [4, 5, 6]})
    result = json.loads(serialize(df))
    assert result[OBJ_ADDR_KEY] == 'pandas.core.frame.DataFrame'
    assert result[OBJ_ARGS_KEY] == [[[1, 4], [2, 5], [3, 6]]] # extra list for args
    assert result[OBJ_KWARGS_KEY]['columns'] == ['A', 'B']
    assert result[OBJ_KWARGS_KEY]['index_columns'] == []

def test_serialize_pandas_series():
    series = pd.Series([1, 2, 3], name='test')
    result = json.loads(serialize(series))
    assert result[OBJ_ADDR_KEY] == 'pandas.core.series.Series'
    assert result[OBJ_ARGS_KEY] == [[1, 2, 3]]
    assert result[OBJ_KWARGS_KEY]['name'] == 'test'

def test_serialize_function():
    def test_func(x):
        return x * 2
    
    result = json.loads(serialize(test_func))
    assert OBJ_ADDR_KEY in result
    assert 'test_func' in result[OBJ_ADDR_KEY]
    assert 'def test_func(x):' in result[OBJ_SRC_KEY]
    assert 'return x * 2' in result[OBJ_SRC_KEY]

def test_deserialize_jsonable():
    assert deserialize(serialize(42)) == 42
    assert deserialize(serialize("hello")) == "hello"
    assert deserialize(serialize([1, 2, 3])) == [1, 2, 3]
    assert deserialize(serialize({"a": 1, "b": 2})) == {"a": 1, "b": 2}

def test_deserialize_numpy():
    arr = np.array([1, 2, 3])
    serialized = serialize(arr)
    result = deserialize(serialized)
    assert isinstance(result, np.ndarray)
    assert np.array_equal(result, arr)

def test_deserialize_pandas_df():
    df = pd.DataFrame({'A': [1, 2, 3], 'B': [4, 5, 6]})
    serialized = serialize(df)
    result = deserialize(serialized)
    assert isinstance(result, pd.DataFrame)
    assert result.equals(df)

def test_deserialize_pandas_series():
    series = pd.Series([1, 2, 3], name='test')
    serialized = serialize(series)
    result = deserialize(serialized)
    assert isinstance(result, pd.Series)
    assert result.equals(series)

def test_deserialize_function():
    def test_func(x):
        return x * 2
    
    serialized = serialize(test_func)
    result = deserialize(serialized)
    assert callable(result)
    assert result(3) == 6

def test_roundtrip():
    original = {
        'int': 42,
        'str': 'hello',
        'list': [1, 2, 3],
        'dict': {'a': 1, 'b': 2},
        'numpy': np.array([1, 2, 3]),
        'pandas_df': pd.DataFrame({'A': [1, 2, 3], 'B': [4, 5, 6]}),
        'pandas_series': pd.Series([1, 2, 3], name='test')
    }
    
    serialized = serialize(original)
    deserialized = deserialize(serialized)
    
    assert deserialized['int'] == original['int']
    assert deserialized['str'] == original['str']
    assert deserialized['list'] == original['list']
    assert deserialized['dict'] == original['dict']
    assert np.array_equal(deserialized['numpy'], original['numpy'])
    assert deserialized['pandas_df'].equals(original['pandas_df'])
    assert deserialized['pandas_series'].equals(original['pandas_series'])