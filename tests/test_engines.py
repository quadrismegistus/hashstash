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
from hashstash.engines.jsonl import JSONLHashStash
# logger.setLevel(logging.DEBUG)
logger.setLevel(logging.CRITICAL+1)

# Plain connectivity probes: importing a test module must never shell out to
# docker or start containers. CI provides these servers via services;
# developers run their own (e.g. `docker run -d -p 6379:6379 redis`).
try:
    import redis as _redis

    _redis.Redis(host="localhost", port=6379, socket_connect_timeout=1).ping()
    REDIS_AVAILABLE = True
    REDIS_SKIP_REASON = ""
except Exception as e:
    REDIS_AVAILABLE = False
    REDIS_SKIP_REASON = f"No Redis server on localhost:6379: {e}"

try:
    from pymongo import MongoClient as _MongoClient

    _MongoClient(
        host="localhost", port=27017, serverSelectionTimeoutMS=1000
    ).server_info()
    MONGO_AVAILABLE = True
    MONGO_SKIP_REASON = ""
except Exception as e:
    MONGO_AVAILABLE = False
    MONGO_SKIP_REASON = f"No MongoDB server on localhost:27017: {e}"

from hashstash.engines.shelve import ShelveHashStash

import importlib


def _optional_engine(module, clsname, dep):
    """A TEST_CLASSES entry for an engine whose dependency is optional (and, for
    leveldb/plyvel, not in the CI `dev` extra) — guard the IMPORT, not just the
    test, so a missing dep skips cleanly instead of erroring at collection."""
    try:
        importlib.import_module(dep)
        cls = getattr(importlib.import_module(module), clsname)
        return pytest.param(cls, id=clsname)
    except Exception as e:  # dep missing / import error
        return pytest.param(None, marks=pytest.mark.skip(reason=f"{dep} unavailable: {e}"), id=clsname)


TEST_CLASSES = [
    PairtreeHashStash,
    SqliteHashStash,
    MemoryHashStash,
    pytest.param(RedisHashStash, marks=pytest.mark.skipif(not REDIS_AVAILABLE, reason=REDIS_SKIP_REASON)),
    DiskCacheHashStash,
    LMDBHashStash,
    pytest.param(MongoHashStash, marks=pytest.mark.skipif(not MONGO_AVAILABLE, reason=MONGO_SKIP_REASON)),
    JSONLHashStash,
    ShelveHashStash,
    # newer general-purpose KV engines were only covered by their own test files,
    # not this shared dict-contract battery — bring them in (guarded on optional
    # deps). NOTE: DataFrameHashStash is intentionally NOT here: it is a
    # specialized DataFrame store (values are wrapped as MetaDataFrame; scalar
    # append-mode, assembly and version metadata don't apply), covered by its own
    # tests/test_dataframes.py.
    _optional_engine("hashstash.engines.duckdb_engine", "DuckDBHashStash", "duckdb"),
    _optional_engine("hashstash.engines.leveldb", "LevelDBHashStash", "plyvel"),
    _optional_engine("hashstash.engines.fsspec", "FsspecHashStash", "fsspec"),
]


@pytest.fixture(params=TEST_CLASSES)
def cache(request, tmp_path):
    cache_type = request.param
    # safe=False so these broad round-trip tests exercise the full code-capable
    # serializer on every engine. Networked engines (redis/mongo) now default to
    # safe=True (see test_engine_safe_default.py); this fixture opts back in to
    # keep testing what it always tested. We trust our own writes here.
    cache = cache_type(
        os.path.join(tmp_path, f"{cache_type.__name__.lower()}_cache"), safe=False
    )
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

        if self._compression_active(cache):
            cached_size = self._get_cached_size(cache, "large_data")
            assert cached_size < raw_size

        retrieved_data = cache["large_data"]
        assert retrieved_data == large_data

    # dataframes serialized special
    def test_df_keys(self, cache):
        import pandas as pd
        df = pd.DataFrame({
            "name":["cica","kutya"],
            "goodness":["nagyon jó","nagyon rossz"]
        })
        key = "cica-kutya"
        cache[key] = df
        cache[df] = key
        assert cache[df] == key

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

        if self._compression_active(cache):
            cached_size = self._get_cached_size(cache, "test_data")
            assert cached_size < raw_size

        retrieved_data = cache["test_data"]
        assert retrieved_data == test_data

    def test_very_large_data_compression(self, cache):
        # a two-symbol alphabet keeps the payload compressible: fully random
        # text has no redundancy, so asserting compression on it must fail
        very_large_data = {
            "large_string": "".join(random.choices("ab", k=1_000_000)),
            "large_list": [random.randint(1, 1000000) for _ in range(1000)],
            "large_nested": {
                f"key_{i}": {
                    "nested_string": "".join(
                        random.choices("ABCDEFGHIJKLMNOPQRSTUVWXYZ", k=10)
                    ),
                    "nested_list": [random.random() for _ in range(10)],
                }
                for i in range(1_00)
            },
        }

        raw_size = len(json.dumps(very_large_data).encode())

        cache["very_large_data"] = very_large_data
        if isinstance(cache, ShelveHashStash):
            time.sleep(0.1)  # Add a small delay to ensure data is written

        if self._compression_active(cache):
            cached_size = self._get_cached_size(cache, "very_large_data")
            assert cached_size < raw_size

        retrieved_data = cache["very_large_data"]
        assert retrieved_data == very_large_data

    def test_cache_path(self, cache, tmp_path):
        #print([cache, cache.path, tmp_path])
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

    def test_assemble_ld(self, cache):
        #print(len(cache))
        cache['x'] = 1
        cache['y'] = 2
        cache['z'] = {'result':3}
        cache['zz'] = {'result':4}

        ld = cache.assemble_ld()
        #pprint(ld)
        assert len(ld) == 4
        assert all("_key" in item for item in ld)
        assert sum("_value" in item for item in ld) == 2
        assert sum("result" in item for item in ld) == 2


    def test_assemble_ld_with_list(self, cache):
        cache["list_key"] = [{"a": 1}, {"b": 2}, 3, 4]
        cache["df_key"] = pd.DataFrame({'col1': [1, 2], 'col2': ['a', 'b']})
        ld = cache.assemble_ld()
        #pprint(ld)
        assert len(ld) == 6
        assert all("_key" in item for item in ld)
        # assert ld[0]['a']==1
        # assert ld[1]['b']==2
        # assert ld[2]['_value']==3
        # assert ld[3]['_value']==4

    def test_assemble_ld_with_dataframe(self, cache):
        df = pd.DataFrame({'col1': [1, 2], 'col2': ['a', 'b']})
        cache["df_key"] = df
        ld = cache.assemble_ld()
        assert len(ld) == 2
        #pprint(ld)
        assert ld[0] == {"_key": "df_key", "col1": 1, "col2": "a"}
        assert ld[1] == {"_key": "df_key", "col1": 2, "col2": "b"}

    def test_assemble_ld_mixed_types(self, cache):
        cache["key1"] = {"result": "simple_dict"}
        cache["key2"] = [1, 2, {"nested": "dict"}]
        df = pd.DataFrame({'col1': [3, 4], 'col2': ['c', 'd']})
        cache["key3"] = df

        ld = cache.assemble_ld(with_metadata=False)
        assert len(ld) == 6  # 1 + 3 + 2

        # Check simple dict
        assert {"_key": "key1", "result": "simple_dict"} in ld

        # Check list items
        assert {"_key": "key2", "_value": 1} in ld
        assert {"_key": "key2", "_value": 2} in ld
        assert {"_key": "key2", "nested": "dict"} in ld

        # Check dataframe rows
        assert {"_key": "key3", "col1": 3, "col2": "c"} in ld
        assert {"_key": "key3", "col1": 4, "col2": "d"} in ld

    def test_assemble_df(self, cache):
        cache["key1"] = {"result": "simple_dict"}
        cache["key2"] = [1, 2, {"nested": "dict"}]
        df = pd.DataFrame({'col1': [3, 4], 'col2': ['c', 'd']})
        cache["key3"] = df

        result_df = cache.assemble_df(with_metadata=False)
        assert is_dataframe(result_df)
        #print(result_df)

        assert len(result_df) == 6
        assert set(result_df.columns) == {"_value", "result", "nested", "col1", "col2"}

        #print(result_df)
        assert set(result_df.index.names) == {'_key'}

        result_df = cache.assemble_df(with_metadata=True)
        assert is_dataframe(result_df)
        assert set(result_df.index.names) == {'_key'} | set(cache.metadata_cols)

        

    def test_df_property(self, cache):
        cache["key1"] = {"result": "simple_dict"}
        cache["key2"] = [1, 2, {"nested": "dict"}]
        df = pd.DataFrame({'col1': [3, 4], 'col2': ['c', 'd']})
        cache["key3"] = df

        result_df = cache.df
        
        assert is_dataframe(result_df)
        assert len(result_df) == 6
        assert set(result_df.columns) == {"_value", "result", "nested", "col1", "col2"}

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
        sub_cache = cache.sub("sub_cache")
        assert isinstance(sub_cache, BaseHashStash)
        assert sub_cache.root_dir.endswith("sub_cache")
        assert sub_cache.engine == cache.engine

    def test_tmp(self, cache):
        with cache.tmp() as tmp_cache:
            assert isinstance(tmp_cache, BaseHashStash)
            assert tmp_cache.root_dir != cache.root_dir
            assert tmp_cache.name == cache.name
            assert tmp_cache.dbname.startswith('tmp/')
            assert tmp_cache.is_tmp
            assert not cache.is_tmp

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
        assert item == ("popitem_key", "popitem_value")
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

        test_func.stash.clear()
        result = test_func(5)
        assert test_func.stash.is_function_stash
        func_key = test_func.stash.new_function_key(5)
        # assert func_key == {'args': (5,), 'kwargs': {}}
        assert func_key == ((5,),{})
        assert test_func.stash.get_func(5) == result
        assert test_func.stash.get(func_key) == result
        assert test_func.stash.keys_l() == [func_key]

    def test_sub_function_results(self, cache):
        def test_func(x):
            return x * 2

        sub_stash = cache.sub_function_results(test_func)
        assert isinstance(sub_stash, BaseHashStash)
        assert "stashed_result" in sub_stash.dbname
        assert "test_func" in sub_stash.dbname

    @staticmethod
    def _get_cached_size(cache, key):
        """Bytes of the encoded (serialized+compressed+b64) stored value.
        The old version measured len() of the DECODED object — comparing a
        dict's key count against a byte length, which asserted nothing."""
        value = cache[key]
        encoded = cache.encode_value(cache.new_unencoded_value(value, unencoded_key=key))
        return len(encoded.encode()) if isinstance(encoded, str) else len(encoded)

    @staticmethod
    def _compression_active(cache):
        return cache.compress not in {False, None, RAW_NO_COMPRESS}

    def test_append_mode(self, cache):
        cache.append_mode = True
        cache["key1"] = "value1"
        cache["key1"] = "value2"
        assert cache.get("key1") == "value2"
        assert cache.get_all("key1") == ["value1", "value2"]

    def test_get_all_with_metadata(self, cache):
        cache.append_mode = False
        cache["key1"] = "value1"
        cache["key1"] = "value2"
        result = cache.get_all("key1", with_metadata=True, all_results=False)
        assert len(result) == 1
        assert result[0]["_value"] == "value2"

    def test_get_all_with_metadata_append_mode(self, cache):
        cache.append_mode = True
        cache["key1"] = "value1"
        cache["key1"] = "value2"
        result = cache.get_all("key1", with_metadata=True)
        assert len(result) == 2
        assert result[0]["_version"] == 1
        assert result[0]["_value"] == "value1"
        assert result[1]["_version"] == 2
        assert result[1]["_value"] == "value2"

    

    def test_get_all_without_metadata(self, cache):
        cache["key1"] = "value1"
        cache["key1"] = "value2"
        result = cache.get_all("key1", with_metadata=False, all_results=False)
        assert result == ["value2"]

    def test_get_all_all_results(self, cache):
        cache.append_mode = True
        cache["key1"] = "value1"
        cache["key1"] = "value2"
        result = cache.get_all("key1", all_results=True, with_metadata=False)
        assert result == ["value1", "value2"]

    def test_get_default_value(self, cache):
        assert cache.get("non_existent_key", default="default_value") == "default_value"

    def test_get_as_string(self, cache):
        cache["key1"] = {"nested": "value"}
        result = cache.get("key1", as_string=True)
        assert isinstance(result, str)
        assert "nested" in result and "value" in result


    def test_function_stash(self, cache):
        def test_func(x, y):
            return x + y

        func_stash = cache.sub_function_results(test_func)
        cache.clear()
        func_stash.clear()
        assert func_stash.is_function_stash
        assert func_stash is not cache
        assert func_stash.path != cache.path

        # func_stash.set((1,2), 3)
        
        # #print('func_stash',func_stash.keys_l())
        # #print('cache',cache.keys_l())
        # assert len(func_stash) == 1
        # assert len(cache) == 0

        # cache.set((1,2), 3)
        # assert len(func_stash) == 1
        # assert len(cache) == 1




class TestHashStashFactory:
    def test_engine_selection(self):
        from hashstash.config import get_working_engines

        working = get_working_engines()
        for engine in ENGINES:
            # engines whose backing package isn't installed (e.g. leveldb without
            # plyvel) can't be constructed here; skip rather than hard-fail
            if engine not in working:
                continue
            stash = HashStash(engine=engine)
            assert stash.engine == engine


    def test_dbname_parameter(self):
        dbname = "test_db"
        stash = HashStash(dbname=dbname)
        assert stash.dbname == dbname

    def test_compress_parameter(self):
        stash = HashStash(compress=True)
        assert stash.compress in {OPTIMAL_COMPRESS, DEFAULT_COMPRESS}
        stash = HashStash(compress=False)
        assert stash.compress == RAW_NO_COMPRESS

    def test_b64_parameter(self):
        stash = HashStash(b64=True)
        assert stash.b64 == True
        stash = HashStash(b64=False)
        assert stash.b64 == False

    def test_serializer_parameter(self):
        # unknown serializers raise instead of being silently coerced to the default
        with pytest.raises(ValueError):
            HashStash(serializer="not_a_serializer")
        stash = HashStash(serializer="pickle")
        assert stash.serializer == "pickle"

    def test_root_dir_parameter(self):
        root_dir = "/tmp/test_root"
        stash = HashStash(root_dir=root_dir)
        assert stash.root_dir == root_dir

    def test_invalid_engine(self):
        # a typo'd engine used to silently fall back to pairtree, writing to the wrong store
        with pytest.raises(ValueError):
            HashStash(engine="invalid_engine")

    def test_default_parameters(self):
        config = Config()
        stash = HashStash()
        assert stash.engine == config.engine
        assert stash.name == DEFAULT_NAME
        assert stash.dbname == DEFAULT_DBNAME
        assert stash.compress == config.compress
        assert stash.b64 == config.b64
        assert stash.serializer in get_working_serializers()

    def test_multiple_parameters(self):
        path = "multi_test"
        engine = "sqlite"
        dbname = "multi_db"
        compress = True
        b64 = False
        serializer = "pickle"

        stash = HashStash(
            root_dir=path,
            engine=engine,
            dbname=dbname,
            compress=compress,
            b64=b64,
            serializer=serializer
        )

        assert stash.root_dir == os.path.join(Config().root_dir, path)
        assert stash.engine == engine
        assert stash.dbname == dbname
        assert stash.compress in {OPTIMAL_COMPRESS, DEFAULT_COMPRESS}
        assert stash.b64 == b64
        assert serializer == stash.serializer

    def test_assemble_ld_with_nested_dicts(self, cache):
        cache.clear()
        
        # Populate cache with random animals
        import random
        for n in range(100):
            cache[f'Animal {n+1}'] = {
                'name': random.choice(['cat', 'dog']), 
                'goodness': random.choice(['good', 'bad']),
                'other': {
                    'age': random.randint(1, 10),
                }
            }
        
        # Get the list of dictionaries
        result = cache.ld
        
        # Assertions
        assert isinstance(result, list)
        assert len(result) == 100
        
        expected_keys = {'_key', 'name', 'goodness', 'other.age'}
        
        for item in result:
            assert isinstance(item, dict)
            assert set(item.keys()) == expected_keys
            assert item['_key'].startswith('Animal ')
            assert item['name'] in ['cat', 'dog']
            assert item['goodness'] in ['good', 'bad']
            assert isinstance(item['other.age'], int)
            assert 1 <= item['other.age'] <= 10

## specific engine tests

import pytest
import redis
from hashstash.engines.redis import start_redis_server, REDIS_HOST, REDIS_PORT, REDIS_DB

# @pytest.fixture(scope="module")
# def redis_client():
#     return redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)

# def test_start_redis_server():
#     start_redis_server()
#     time.sleep(5)
#     start_redis_server()
    
#     # Verify that we can connect to Redis
#     client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)
#     assert client.ping()

# def test_start_mongo_server():
#     start_mongo_server()
#     time.sleep(5)
#     start_mongo_server()
    
#     # Verify that we can connect to Redis
#     from pymongo import MongoClient
#     client = MongoClient(host=MONGO_HOST, port=MONGO_PORT)
#     assert client.admin.command('ping')['ok'] == 1

def test_encode_path():
    cache = HashStash(engine='pairtree')
    assert os.path.isabs(cache.get_path_key('unencoded_key'))


class TestJSONLB64False:
    def test_b64_false_honored(self, tmp_path):
        stash = JSONLHashStash(str(tmp_path), b64=False)
        assert stash.b64 == False

    def test_directory_name_no_b64_suffix(self, tmp_path):
        stash = JSONLHashStash(str(tmp_path), b64=False)
        assert "+b64" not in stash.path_dirname

    def test_roundtrip(self, tmp_path):
        stash = JSONLHashStash(str(tmp_path), b64=False)
        stash["item_a"] = {"field1": True, "val": "hello"}
        assert stash["item_a"] == {"field1": True, "val": "hello"}

    def test_jsonl_file_is_not_base64(self, tmp_path):
        # b64=False now implies flat=True, so dict values are inlined (no __value__ field)
        stash = JSONLHashStash(str(tmp_path), b64=False)
        stash["item_a"] = {"field1": True, "val": "hello"}
        with open(stash.path) as f:
            line = json.loads(f.readline())
        key_raw = line[stash.key_name]
        import base64
        def looks_like_b64(s):
            if not isinstance(s, str):
                return False
            try:
                decoded = base64.b64decode(s.encode(), validate=True)
                return base64.b64encode(decoded).decode() == s
            except Exception:
                return False
        assert not looks_like_b64(key_raw), f"key looks like base64: {key_raw!r}"
        # flat mode: dict fields are inlined directly, no __value__ wrapper
        assert stash.flat == True
        assert "__value__" not in line
        assert line["field1"] == True

class TestJSONLFlat:
    def test_flat_sets_b64_false(self, tmp_path):
        s = JSONLHashStash(str(tmp_path), flat=True)
        assert s.flat == True
        assert s.b64 == False
        assert s.compress == "raw"

    def test_dict_value_inlined(self, tmp_path):
        s = JSONLHashStash(str(tmp_path), flat=True)
        s["doc_1"] = {"field1": True, "val": "hello"}
        with open(s.path) as f:
            row = json.loads(f.readline())
        assert row["__key__"] == "doc_1"
        assert row["field1"] == True
        assert row["val"] == "hello"
        assert "__value__" not in row

    def test_roundtrip_dict(self, tmp_path):
        s = JSONLHashStash(str(tmp_path), flat=True)
        s["k"] = {"a": 1, "b": [1, 2, 3]}
        assert s["k"] == {"a": 1, "b": [1, 2, 3]}

    def test_roundtrip_dict_key(self, tmp_path):
        s = JSONLHashStash(str(tmp_path), flat=True)
        s[{"id": "doc_001"}] = {"label": "drama"}
        assert s[{"id": "doc_001"}] == {"label": "drama"}

    def test_roundtrip_tuple_key(self, tmp_path):
        s = JSONLHashStash(str(tmp_path), flat=True)
        s[("a", 1)] = {"note": "x"}
        assert s[("a", 1)] == {"note": "x"}

    def test_non_dict_fallback_to_value_field(self, tmp_path):
        s = JSONLHashStash(str(tmp_path), flat=True)
        s["scalar"] = "just a string"
        with open(s.path) as f:
            row = json.loads(f.readline())
        assert "__value__" in row
        assert s["scalar"] == "just a string"

    def test_delete(self, tmp_path):
        s = JSONLHashStash(str(tmp_path), flat=True)
        s["k"] = {"x": 1}
        del s["k"]
        assert "k" not in s
        assert len(s) == 0

    def test_reserved_field_raises(self, tmp_path):
        s = JSONLHashStash(str(tmp_path), flat=True)
        with pytest.raises(ValueError, match="reserved field names"):
            s["k"] = {"__key__": "bad"}

    def test_items(self, tmp_path):
        s = JSONLHashStash(str(tmp_path), flat=True)
        s["a"] = {"x": 1}
        s["b"] = {"x": 2}
        result = dict(s.items())
        assert result == {"a": {"x": 1}, "b": {"x": 2}}

    def test_factory_flat(self, tmp_path):
        s = HashStash(root_dir=str(tmp_path), engine="jsonl", flat=True)
        s["k"] = {"v": 42}
        assert s["k"] == {"v": 42}
        assert s.flat == True


if __name__ == "__main__":
    pytest.main([__file__])