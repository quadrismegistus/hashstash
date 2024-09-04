import pytest
from hashstash import *
logger.setLevel(logging.CRITICAL+1)
import numpy as np
import pandas as pd
from pandas.testing import assert_frame_equal
from pathlib import Path
from hashstash.constants import SERIALIZER_TYPES

serializers = ['hashstash']

@pytest.fixture(params=serializers)
def serializer_type(request):
    return request.param

@pytest.fixture
def cache(serializer_type, tmp_path):
    cache = MemoryHashStash(name=f"{serializer_type}_cache", root_dir=tmp_path, serializer=serializer_type)
    cache.clear()
    yield cache

class TestSerializers:
    def test_serialize_deserialize_basic_types(self, cache):
        data = {
            'int': 42,
            'float': 3.14,
            'str': 'hello',
            'bool': True,
            'list': [1, 2, 3],
            'dict': {'a': 1, 'b': 2}
        }
        for key, value in data.items():
            cache[key] = value
            assert cache[key] == value

    def test_serialize_deserialize_numpy(self, cache):
        arr = np.array([1, 2, 3])
        cache['numpy_array'] = arr
        result = cache['numpy_array']
        assert isinstance(result, np.ndarray)
        assert np.array_equal(result, arr)

    def test_serialize_deserialize_pandas_df(self, cache):
        df = pd.DataFrame({'A': [1, 2, 3], 'B': [4, 5, 6]})
        cache['pandas_df'] = df
        result = cache['pandas_df']
        assert isinstance(result, pd.DataFrame)
        assert_frame_equal(result, df)

    def test_serialize_deserialize_pandas_series(self, cache):
        series = pd.Series([1, 2, 3], name='test')
        cache['pandas_series'] = series
        result = cache['pandas_series']
        assert isinstance(result, pd.Series)
        assert result.equals(series)

    def test_serialize_deserialize_function(self, cache):
        def test_func(x):
            return x * 2
        
        cache['function'] = test_func
        result = cache['function']
        assert callable(result)
        assert result(3) == 6

    def test_serialize_deserialize_class(self, cache):
        class TestClass:
            def __init__(self, x):
                self.x = x
            
            def method(self):
                return self.x * 2
        
        cache['class'] = TestClass
        result = cache['class']
        assert isinstance(result, type)
        instance = result(5)
        assert instance.x == 5
        assert instance.method() == 10

    def test_serialize_deserialize_instance(self, cache):
        class TestClass:
            def __init__(self, x):
                self.x = x
            
            def method(self):
                return self.x * 2
        
        original = TestClass(5)
        cache['instance'] = original
        result = cache['instance']
        
        assert result.__class__.__name__ == 'TestClass'
        assert result.x == 5
        assert result.method() == 10

    def test_serialize_deserialize_nested_structure(self, cache):
        nested = {
            'list_of_dicts': [{'a': 1}, {'b': 2}],
            'dict_of_lists': {'x': [1, 2], 'y': [3, 4]},
            'tuple_with_list': ([1, 2], [3, 4]),
            'set_of_tuples': {(1, 2), (3, 4)}
        }
        
        cache['nested'] = nested
        result = cache['nested']
        
        assert result == nested

    def test_serialize_deserialize_path(self, cache):
        path = Path('/tmp/test_file.txt')
        cache['path'] = path
        result = cache['path']
        assert isinstance(result, Path)
        assert str(result) == str(path)

    def test_serialize_deserialize_complex_nested_structure(self, cache):
        complex_obj = {
            'list': [1, 2, np.array([3, 4, 5])],
            'dict': {'a': pd.Series([1, 2, 3]), 'b': Path('/tmp/test.txt')},
            'tuple': (lambda x: x*2, {1, 2, 3}),
            'dataframe': pd.DataFrame({'A': [1, 2], 'B': [3, 4]}, index=['x', 'y'])
        }
        
        cache['complex'] = complex_obj
        result = cache['complex']
        
        assert isinstance(result['list'][2], np.ndarray)
        assert np.array_equal(result['list'][2], np.array([3, 4, 5]))
        assert isinstance(result['dict']['a'], pd.Series)
        assert result['dict']['a'].equals(pd.Series([1, 2, 3]))
        assert isinstance(result['dict']['b'], Path)
        assert str(result['dict']['b']) == '/tmp/test.txt'
        assert callable(result['tuple'][0])
        assert result['tuple'][0](3) == 6
        assert result['tuple'][1] == {1, 2, 3}
        assert isinstance(result['dataframe'], pd.DataFrame)
        assert result['dataframe'].equals(complex_obj['dataframe'])

    def test_serializer_type(self, cache, serializer_type):
        assert cache.serializer == serializer_type

    def test_serializer_performance(self, cache):
        import time

        large_data = {f"key_{i}": f"value_{i}" * 1000 for i in range(1000)}
        
        start_time = time.time()
        cache['large_data'] = large_data
        write_time = time.time() - start_time

        start_time = time.time()
        result = cache['large_data']
        read_time = time.time() - start_time

        assert result == large_data
        #print(f"\nSerializer: {cache.serializer}")
        #print(f"Write time: {write_time:.4f} seconds")
        #print(f"Read time: {read_time:.4f} seconds")