from hashstash import *
import unittest
import tempfile
import shutil
import os
import json
import random
import time
import pytest
import pandas as pd
logger.setLevel(logging.DEBUG)
config.set_serializer('custom')

start_redis_server()

TEST_CLASSES = [
    PairtreeHashStash,
    SqliteHashStash,
    MemoryHashStash,
    ShelveHashStash,
    RedisHashStash,
    DiskCacheHashStash,
    LMDBHashStash,
    # PickleDBHashStash,
]


@pytest.fixture(params=TEST_CLASSES)
def cache(request, tmp_path):
    cache_type = request.param
    cache = cache_type(name=f"{cache_type.__name__.lower()}_cache", root_dir=tmp_path)
    cache.clear()
    yield cache


class TestHashStash:
    def test_set_get(self, cache):
        cache["test_key"] = "test_value"
        assert cache["test_key"] == "test_value"

    def test_contains(self, cache):
        cache["test_key"] = "test_value"
        assert "test_key" in cache

    def test_get_default(self, cache):
        assert cache.get("non_existent_key", "default") == "default"

    def test_clear(self, cache):
        cache["test_key1"] = "test_value1"
        cache["test_key2"] = "test_value2"
        cache.clear()
        assert len(cache) == 0

    def test_len(self, cache):
        cache["test_key1"] = "test_value1"
        cache["test_key2"] = "test_value2"
        assert len(cache) == 2

    def test_encoding_size_reduction(self, cache):
        large_data = {f"key_{i}": f"value_{i}" * 1000 for i in range(1000)}
        raw_size = len(json.dumps(large_data).encode())

        cache["large_data"] = large_data

        if isinstance(cache, ShelveHashStash):
            time.sleep(0.1)  # Add a small delay to ensure data is written

        cached_size = self._get_cached_size(cache, "large_data")
        assert cached_size < raw_size

        compression_ratio = cached_size / raw_size
        print(f"Compression ratio ({type(cache).__name__}): {compression_ratio:.2%}")

        retrieved_data = cache["large_data"]
        assert retrieved_data == large_data

    def test_multiple_data_types(self, cache):
        test_data = {
            "string": "Hello, world!" * 1000,
            "number": 12345,
            "list": list(range(1000)),
            "nested": {"a": [1, 2, 3], "b": {"c": "nested" * 100}},
        }

        raw_size = len(json.dumps(test_data).encode())

        cache["test_data"] = test_data
        if isinstance(cache, ShelveHashStash):
            time.sleep(0.1)  # Add a small delay to ensure data is written

        cached_size = self._get_cached_size(cache, "test_data")
        assert cached_size < raw_size

        compression_ratio = cached_size / raw_size
        print(
            f"Compression ratio for mixed data ({type(cache).__name__}): {compression_ratio:.2%}"
        )

        retrieved_data = cache["test_data"]
        assert retrieved_data == test_data

    def test_very_large_data_compression(self, cache):
        very_large_data = {
            "large_string": "".join(
                random.choices("abcdefghijklmnopqrstuvwxyz", k=1_000_000)
            ),
            "large_list": [random.randint(1, 1000000) for _ in range(100_000)],
            "large_nested": {
                f"key_{i}": {
                    "nested_string": "".join(
                        random.choices("ABCDEFGHIJKLMNOPQRSTUVWXYZ", k=100)
                    ),
                    "nested_list": [random.random() for _ in range(100)],
                }
                for i in range(1_000)
            },
        }

        raw_size = len(json.dumps(very_large_data).encode())

        cache["very_large_data"] = very_large_data
        if isinstance(cache, ShelveHashStash):
            time.sleep(0.1)  # Add a small delay to ensure data is written

        cached_size = self._get_cached_size(cache, "very_large_data")
        assert cached_size < raw_size

        compression_ratio = cached_size / raw_size
        print(f"Very large data compression ({type(cache).__name__}):")
        print(f"Raw size: {raw_size / 1024 / 1024:.2f} MB")
        print(f"Cached size: {cached_size / 1024 / 1024:.2f} MB")
        print(f"Compression ratio: {compression_ratio:.2%}")
        print(f"Space saved: {(raw_size - cached_size) / 1024 / 1024:.2f} MB")

        retrieved_data = cache["very_large_data"]
        assert retrieved_data == very_large_data

    def test_cache_path(self, cache, tmp_path):
        print([cache, cache.path, tmp_path])
        if not isinstance(cache, MemoryHashStash):
            assert str(cache.path).startswith(str(tmp_path))

    def test_cache_encoding(self, cache):
        test_data = {"key": "value"}
        serialized_data = json.dumps(test_data)
        encoded_data = cache.encode_value(serialized_data)
        assert isinstance(encoded_data, bytes if not cache.string_values else str)
        decoded_data = cache.decode_value(encoded_data)
        
        # Check if decoded_data is already a dictionary
        if isinstance(decoded_data, dict):
            deserialized_data = decoded_data
        else:
            deserialized_data = json.loads(decoded_data)
        
        assert deserialized_data == test_data

    def test_cache_hash(self, cache):
        test_data = b"test data"
        hashed_data = cache.hash(test_data)
        assert isinstance(hashed_data, str)
        assert len(hashed_data) == 32  # MD5 hash length

    def test_cache_context_manager(self, cache):
        with cache as db:
            db["test_key"] = "test_value"
        assert cache["test_key"] == "test_value"

    def test_cache_iter(self, cache):
        cache["key1"] = "value1"
        cache["key2"] = "value2"
        keys = list(cache)
        assert len(keys) == 2
        assert "key1" in keys
        assert "key2" in keys

    def test_cache_values(self, cache):
        cache["key1"] = "value1"
        cache["key2"] = "value2"
        values = list(cache.values())
        assert len(values) == 2
        assert "value1" in values
        assert "value2" in values

    def test_cache_items(self, cache):
        cache["key1"] = "value1"
        cache["key2"] = "value2"
        items = list(cache.items())
        assert len(items) == 2
        assert ("key1", "value1") in items
        assert ("key2", "value2") in items

    def test_sub_function_results(self, cache):
        def example_func(x, y):
            return x + y

        sub_stash = cache.sub_function_results(example_func)
        assert isinstance(sub_stash, BaseHashStash)
        assert "stashed_result" in sub_stash.dbname
        assert "example_func" in sub_stash.dbname

    def test_assemble_ld_and_df(self, cache):
        cache[{"func": lambda x: x, "args": (1,), "kwargs": {}}] = {"result": 1}
        cache[{"func": lambda x: x, "args": (2,), "kwargs": {}}] = {"result": 2}

        ld = cache.assemble_ld()
        assert len(ld) == 2
        assert all("_arg1" in item for item in ld)
        assert all("result" in item for item in ld)

        df = cache.assemble_df()
        assert len(df) == 2
        assert "_arg1" in df.columns
        assert "result" in df.columns
    
    def test_assemble_ld_with_list(self, cache):
        cache["list_key"] = [{"a": 1}, {"b": 2}, 3, 4]
        ld = cache.assemble_ld()
        assert len(ld) == 4
        assert ld[0] == {"_key": "list_key", "a": 1}
        assert ld[1] == {"_key": "list_key", "b": 2}
        assert ld[2] == {"_key": "list_key", "result": 3}
        assert ld[3] == {"_key": "list_key", "result": 4}

    def test_assemble_ld_with_dataframe(self, cache):
        df = pd.DataFrame({'col1': [1, 2], 'col2': ['a', 'b']})
        cache["df_key"] = df
        ld = cache.assemble_ld()
        assert len(ld) == 2
        assert ld[0] == {"_key": "df_key", "col1": 1, "col2": "a"}
        assert ld[1] == {"_key": "df_key", "col1": 2, "col2": "b"}

    def test_assemble_ld_mixed_types(self, cache):
        cache["key1"] = {"result": "simple_dict"}
        cache["key2"] = [1, 2, {"nested": "dict"}]
        df = pd.DataFrame({'col1': [3, 4], 'col2': ['c', 'd']})
        cache["key3"] = df

        ld = cache.assemble_ld()
        assert len(ld) == 6  # 1 + 3 + 2

        # Check simple dict
        assert {"_key": "key1", "result": "simple_dict"} in ld

        # Check list items
        assert {"_key": "key2", "result": 1} in ld
        assert {"_key": "key2", "result": 2} in ld
        assert {"_key": "key2", "nested": "dict"} in ld

        # Check dataframe rows
        assert {"_key": "key3", "col1": 3, "col2": "c"} in ld
        assert {"_key": "key3", "col1": 4, "col2": "d"} in ld

    def test_assemble_df(self, cache):
        cache["key1"] = {"result": "simple_dict"}
        cache["key2"] = [1, 2, {"nested": "dict"}]
        df = pd.DataFrame({'col1': [3, 4], 'col2': ['c', 'd']})
        cache["key3"] = df

        result_df = cache.assemble_df()
        
        assert isinstance(result_df, pd.DataFrame)
        assert len(result_df) == 6
        assert set(result_df.columns) == {"_key", "result", "nested", "col1", "col2"}

    def test_df_property(self, cache):
        cache["key1"] = {"result": "simple_dict"}
        cache["key2"] = [1, 2, {"nested": "dict"}]
        df = pd.DataFrame({'col1': [3, 4], 'col2': ['c', 'd']})
        cache["key3"] = df

        result_df = cache.df
        
        assert isinstance(result_df, pd.DataFrame)
        assert len(result_df) == 6
        assert set(result_df.columns) == {"_key", "result", "nested", "col1", "col2"}

    def test_delete_key(self, cache):
        # Add a key-value pair
        cache["test_key"] = "test_value"
        assert "test_key" in cache

        # Delete the key
        del cache["test_key"]
        assert "test_key" not in cache

        # Attempt to delete a non-existent key
        with pytest.raises(KeyError):
            del cache["non_existent_key"]

    def test_to_dict(self, cache):
        cache_dict = cache.to_dict()
        assert isinstance(cache_dict, dict)
        assert "engine" in cache_dict
        assert "root_dir" in cache_dict
        assert "compress" in cache_dict
        assert "b64" in cache_dict
        assert "name" in cache_dict
        assert "dbname" in cache_dict
        assert "serializer" in cache_dict

    def test_from_dict(self, cache):
        cache_dict = cache.to_dict()
        new_cache = BaseHashStash.from_dict(cache_dict)
        assert isinstance(new_cache, BaseHashStash)
        assert new_cache.engine == cache.engine
        assert new_cache.root_dir == cache.root_dir
        assert new_cache.compress == cache.compress
        assert new_cache.b64 == cache.b64
        assert new_cache.name == cache.name
        assert new_cache.dbname == cache.dbname
        assert new_cache.serializer == cache.serializer

    def test_sub(self, cache):
        sub_cache = cache.sub(name="sub_cache")
        assert isinstance(sub_cache, BaseHashStash)
        assert sub_cache.name == "sub_cache"
        assert sub_cache.engine == cache.engine

    def test_tmp(self, cache):
        tmp_cache = cache.tmp()
        assert isinstance(tmp_cache, BaseHashStash)
        assert tmp_cache.root_dir != cache.root_dir
        assert tmp_cache.name == "tmp"

    def test_setdefault(self, cache):
        assert cache.setdefault("new_key", "default_value") == "default_value"
        assert cache["new_key"] == "default_value"
        assert cache.setdefault("new_key", "another_value") == "default_value"

    def test_pop(self, cache):
        cache["pop_key"] = "pop_value"
        assert cache.pop("pop_key") == "pop_value"
        assert "pop_key" not in cache
        assert cache.pop("non_existent", "default") == "default"

    def test_popitem(self, cache):
        cache.clear()
        cache["popitem_key"] = "popitem_value"
        item = cache.popitem()
        assert item == "popitem_value"
        assert len(cache) == 0

    def test_keys_l(self, cache):
        cache.clear()
        cache["key1"] = "value1"
        cache["key2"] = "value2"
        keys = cache.keys_l()
        assert isinstance(keys, list)
        assert set(keys) == {"key1", "key2"}

    def test_values_l(self, cache):
        cache.clear()
        cache["key1"] = "value1"
        cache["key2"] = "value2"
        values = cache.values_l()
        assert isinstance(values, list)
        assert set(values) == {"value1", "value2"}

    def test_items_l(self, cache):
        cache.clear()
        cache["key1"] = "value1"
        cache["key2"] = "value2"
        items = cache.items_l()
        assert isinstance(items, list)
        assert set(items) == {("key1", "value1"), ("key2", "value2")}

    def test_copy(self, cache):
        cache.clear()
        cache["key1"] = "value1"
        cache["key2"] = "value2"
        copied = cache.copy()
        assert isinstance(copied, dict)
        assert copied == {"key1": "value1", "key2": "value2"}

    def test_update(self, cache):
        cache.clear()
        cache["key1"] = "value1"
        cache.update({"key2": "value2"}, key3="value3")
        assert dict(cache.items()) == {"key1": "value1", "key2": "value2", "key3": "value3"}

    def test_hash(self, cache):
        data = b"test data"
        hashed = cache.hash(data)
        assert isinstance(hashed, str)
        assert len(hashed) == 32  # MD5 hash length

    def test_stashed_result(self, cache):
        @cache.stashed_result
        def test_func(x):
            return x * 2

        result = test_func(5)
        assert result == 10
        assert test_func.stash[{"func": test_func, "args": (5,), "kwargs": {}}] == 10

    def test_sub_function_results(self, cache):
        def test_func(x):
            return x * 2

        sub_stash = cache.sub_function_results(test_func)
        assert isinstance(sub_stash, BaseHashStash)
        assert "stashed_result" in sub_stash.dbname
        assert "test_func" in sub_stash.dbname

    @staticmethod
    def _get_cached_size(cache, key):
        return len(cache[key])

class TestHashStashFactory:
    def test_engine_selection(self):
        for engine in ENGINES:
            stash = HashStash(engine=engine)
            assert stash.engine == engine

    def test_name_parameter(self):
        name = "test_stash"
        stash = HashStash(name=name)
        assert stash.name == name

    def test_dbname_parameter(self):
        dbname = "test_db"
        stash = HashStash(dbname=dbname)
        assert stash.dbname == dbname

    def test_compress_parameter(self):
        stash = HashStash(compress=True)
        assert stash.compress == True
        stash = HashStash(compress=False)
        assert stash.compress == False

    def test_b64_parameter(self):
        stash = HashStash(b64=True)
        assert stash.b64 == True
        stash = HashStash(b64=False)
        assert stash.b64 == False

    def test_serializer_parameter(self):
        serializer = "json"
        stash = HashStash(serializer=serializer)
        assert serializer in stash.serializer

    def test_root_dir_parameter(self):
        root_dir = "/tmp/test_root"
        stash = HashStash(root_dir=root_dir)
        assert stash.root_dir == root_dir

    def test_invalid_engine(self):
        with pytest.raises(ValueError):
            HashStash(engine="invalid_engine")

    def test_default_parameters(self):
        stash = HashStash()
        assert stash.engine == config.engine
        assert stash.name == DEFAULT_NAME
        assert stash.dbname == DEFAULT_DBNAME
        assert stash.compress == config.compress
        assert stash.b64 == config.b64
        assert set(stash.serializer) == set(get_working_serializers(config.serializer))

    def test_multiple_parameters(self):
        name = "multi_test"
        engine = "sqlite"
        dbname = "multi_db"
        compress = True
        b64 = False
        serializer = "pickle"

        stash = HashStash(
            name=name,
            engine=engine,
            dbname=dbname,
            compress=compress,
            b64=b64,
            serializer=serializer
        )

        assert stash.name == name
        assert stash.engine == engine
        assert stash.dbname == dbname
        assert stash.compress == compress
        assert stash.b64 == b64
        assert serializer in stash.serializer

## specific engine tests

import pytest
import redis
from hashstash.engines.redis import start_redis_server, REDIS_HOST, REDIS_PORT, REDIS_DB

@pytest.fixture(scope="module")
def redis_client():
    return redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)

def test_start_redis_server():
    # In GitHub Actions, Redis is already running, so this should just connect
    start_redis_server()
    time.sleep(30)
    start_redis_server()
    
    # Verify that we can connect to Redis
    client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)
    assert client.ping()

def test_encode_path():
    cache = HashStash(engine='pairtree')
    assert os.path.isabs(cache.encode_path('unencoded_key'))

if __name__ == "__main__":
    pytest.main([__file__])