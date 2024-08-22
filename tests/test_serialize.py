import pytest
from hashstash import *
import numpy as np
import pandas as pd
from pandas.testing import assert_frame_equal
SERIALIZERS = ['custom']  # only one in need of testing and only one that can handle all these tests/cases
from pathlib import Path


def test_serialize_jsonable():
    assert json.loads(serialize(42)) == 42
    assert json.loads(serialize("hello")) == "hello"
    assert json.loads(serialize([1, 2, 3])) == [1, 2, 3]
    assert json.loads(serialize({"a": 1, "b": 2})) == {"a": 1, "b": 2}

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

@pytest.fixture(params=list(SERIALIZERS))
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
    assert all(s in working_serializers for s in SERIALIZER_TYPES.__args__)

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
            return other.__class__.__name__ == self.__class__.__name__ and self.value == other.value
    
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
    print('SERIALIZED', serialized)
    deserialized = deserialize(serialized)
    print('DESERIALIZED', deserialized)
    assert list(deserialized) == [0, 1, 2]

def test_serialize_large_data():
    large_list = list(range(10000))
    serialized = serialize(large_list)
    deserialized = deserialize(serialized)
    
    assert deserialized == large_list

# def test_serialize_circular_reference():
#     a = []
#     a.append(a)
    
#     serialized = serialize(a)
#     deserialized = deserialize(serialized)
    
#     assert isinstance(deserialized, list)
    # assert deserialized[0] is deserialized

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

def test_serialize_path():
    path = Path('/tmp/test_file.txt')
    serialized = serialize(path)
    deserialized = deserialize(serialized)
    assert isinstance(deserialized, Path)
    assert str(deserialized) == str(path)

def test_serialize_nested_function():
    def outer(x):
        def inner(y):
            return x + y
        return inner
    
    serialized = serialize(outer)
    deserialized = deserialize(serialized)
    
    assert callable(deserialized)
    inner_func = deserialized(5)
    assert callable(inner_func)
    assert inner_func(3) == 8

def test_serialize_class_with_methods():
    class TestClass:
        def __init__(self, x):
            self.x = x
        
        def method1(self):
            return self.x * 2
        
        def method2(self, y):
            return self.x + y
    
    serialized = serialize(TestClass)
    deserialized = deserialize(serialized)
    
    assert isinstance(deserialized, type)
    instance = deserialized(5)
    assert instance.x == 5
    assert instance.method1() == 10
    assert instance.method2(3) == 8

def test_serialize_numpy_object_array():
    arr = np.array([1, 'two', [3, 4]], dtype=object)
    serialized = serialize(arr)
    deserialized = deserialize(serialized)
    
    assert isinstance(deserialized, np.ndarray)
    assert deserialized.dtype == object
    assert deserialized[0] == 1
    assert deserialized[1] == 'two'
    assert deserialized[2] == [3, 4]

def test_serialize_pandas_dataframe_with_index():
    df = pd.DataFrame({'A': [1, 2, 3], 'B': [4, 5, 6]}, index=['x', 'y', 'z'])
    serialized = serialize(df)
    deserialized = deserialize(serialized)

    print(df)
    print()
    print(deserialized)
    
    assert isinstance(deserialized, pd.DataFrame)
    assert_frame_equal(deserialized, df)
    assert list(deserialized.index) == ['x', 'y', 'z']

def test_serialize_complex_nested_structure():
    complex_obj = {
        'list': [1, 2, np.array([3, 4, 5])],
        'dict': {'a': pd.Series([1, 2, 3]), 'b': Path('/tmp/test.txt')},
        'tuple': (lambda x: x*2, {1, 2, 3}),
        'dataframe': pd.DataFrame({'A': [1, 2], 'B': [3, 4]}, index=['x', 'y'])
    }
    
    serialized = serialize(complex_obj)
    deserialized = deserialize(serialized)
    
    assert isinstance(deserialized['list'][2], np.ndarray)
    assert np.array_equal(deserialized['list'][2], np.array([3, 4, 5]))
    assert isinstance(deserialized['dict']['a'], pd.Series)
    assert deserialized['dict']['a'].equals(pd.Series([1, 2, 3]))
    assert isinstance(deserialized['dict']['b'], Path)
    assert str(deserialized['dict']['b']) == '/tmp/test.txt'
    assert callable(deserialized['tuple'][0])
    assert deserialized['tuple'][0](3) == 6
    assert deserialized['tuple'][1] == {1, 2, 3}
    assert isinstance(deserialized['dataframe'], pd.DataFrame)
    assert deserialized['dataframe'].equals(complex_obj['dataframe'])