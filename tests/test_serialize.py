from hashstash import *
import numpy as np
import pandas as pd
import pytest
import base64
from hashstash.constants import SERIALIZER_TYPES
logger.setLevel(logging.DEBUG)

SERIALIZER_TYPES = ['jsonpickle_ext']
config.set_serializer('jsonpickle_ext')

def test_serialize_jsonable():
    assert json.loads(serialize(42)) == 42
    assert json.loads(serialize("hello")) == "hello"
    assert json.loads(serialize([1, 2, 3])) == [1, 2, 3]
    assert json.loads(serialize({"a": 1, "b": 2})) == {"a": 1, "b": 2}

def test_serialize_numpy():
    arr = np.array([1, 2, 3])
    result = json.loads(serialize(arr))
    assert result[OBJ_ADDR_KEY] == 'numpy.ndarray'
    assert result[OBJ_ARGS_KEY] == [base64.b64encode(arr.tobytes()).decode('utf-8')]

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

@pytest.fixture(params=list(SERIALIZER_TYPES))
def serializer_type(request):
    return request.param

def test_serialize_deserialize_all_types(serializer_type):
    config.set_serializer(serializer_type)
    
    original = {
        'int': 42,
        'str': 'hello',
        'list': [1, 2, 3],
        'dict': {'a': 1, 'b': 2},
        'numpy': np.array([1, 2, 3]),
        'pandas_df': pd.DataFrame({'A': [1, 2, 3], 'B': [4, 5, 6]}),
        'pandas_series': pd.Series([1, 2, 3], name='test'),
        'set': {1, 2, 3},
        'tuple': (1, 2, 3),
        'bytes': b'hello'
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
    assert deserialized['set'] == original['set']
    assert deserialized['tuple'] == original['tuple']
    assert deserialized['bytes'] == original['bytes']

def test_get_working_serializers():
    working_serializers = get_working_serializers()
    assert isinstance(working_serializers, list)
    assert all(s in working_serializers for s in SERIALIZER_TYPES)

def test_serialize_class():
    class TestClass:
        def __init__(self, x):
            self.x = x
        
        def method(self):
            return self.x * 2
    
    serialized = serialize(TestClass)
    deserialized = deserialize(serialized)

    assert isinstance(deserialized, type)
    instance = deserialized(5)
    assert instance.x == 5
    assert instance.method() == 10

def test_serialize_instance():
    class TestClass:
        def __init__(self, x):
            self.x = x
        
        def method(self):
            return self.x * 2
    
    original = TestClass(5)
    serialized = serialize(original)
    deserialized = deserialize(serialized)
    
    # assert isinstance(deserialized, TestClass)
    assert deserialized.__class__.__name__ == 'TestClass'
    assert deserialized.x == 5
    assert deserialized.method() == 10

def test_serialize_nested_structure():
    nested = {
        'list_of_dicts': [{'a': 1}, {'b': 2}],
        'dict_of_lists': {'x': [1, 2], 'y': [3, 4]},
        'tuple_with_list': ([1, 2], [3, 4]),
        'set_of_tuples': {(1, 2), (3, 4)}
    }
    
    serialized = serialize(nested)
    deserialized = deserialize(serialized)
    
    assert deserialized == nested

def test_serialize_custom_objects():
    class CustomObject:
        def __init__(self, value):
            self.value = value
        
        def __eq__(self, other):
            return isinstance(other, CustomObject) and self.value == other.value
    
    original = CustomObject(42)
    serialized = serialize(original)
    deserialized = deserialize(serialized)
    
    assert deserialized == original

def test_serialize_lambda():
    original = lambda x: x * 2
    serialized = serialize(original)
    deserialized = deserialize(serialized)
    
    assert callable(deserialized)
    assert deserialized(3) == 6

def test_serialize_generator():
    def gen():
        yield from range(3)
    
    original = gen()
    serialized = serialize(original)
    deserialized = deserialize(serialized)
    
    assert list(deserialized) == [0, 1, 2]

def test_serialize_large_data():
    large_list = list(range(1000000))
    serialized = serialize(large_list)
    deserialized = deserialize(serialized)
    
    assert deserialized == large_list

def test_serialize_circular_reference():
    a = []
    a.append(a)
    
    serialized = serialize(a)
    deserialized = deserialize(serialized)
    
    assert isinstance(deserialized, list)
    assert deserialized[0] is deserialized

def test_serialize_set():
    s = {1, 2, 3}
    result = json.loads(serialize(s))
    assert result[OBJ_ADDR_KEY] == 'builtins.set'
    assert result[OBJ_ARGS_KEY] == [1, 2, 3]

def test_serialize_tuple():
    t = (1, 2, 3)
    result = json.loads(serialize(t))
    assert result[OBJ_ADDR_KEY] == 'builtins.tuple'
    assert result[OBJ_ARGS_KEY] == [1, 2, 3]

def test_serialize_bytes():
    b = b'hello'
    result = json.loads(serialize(b))
    assert result[OBJ_ADDR_KEY] == 'builtins.bytes'
    assert result[OBJ_ARGS_KEY] == [base64.b64encode(b).decode('utf-8')]

def test_deserialize_set():
    s = {1, 2, 3}
    serialized = serialize(s)
    result = deserialize(serialized)
    assert isinstance(result, set)
    assert result == s

def test_deserialize_tuple():
    t = (1, 2, 3)
    serialized = serialize(t)
    result = deserialize(serialized)
    assert isinstance(result, tuple)
    assert result == t

def test_deserialize_bytes():
    b = b'hello'
    serialized = serialize(b)
    result = deserialize(serialized)
    assert isinstance(result, bytes)
    assert result == b