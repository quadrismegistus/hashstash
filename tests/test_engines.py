import unittest
import tempfile
import shutil
import os
import json
import random
import time
import pytest
from hashstash.engines.files import FileHashStash
from hashstash.engines.sqlite import SqliteHashStash
from hashstash.engines.memory import MemoryHashStash
from hashstash.engines.shelve import ShelveHashStash
from hashstash.engines.redis import RedisHashStash
from hashstash.engines.pickledb import PickleDBHashStash
from hashstash.engines.diskcache import DiskCacheHashStash
from hashstash.engines.lmdb import LMDBHashStash

TEST_CLASSES = [
    FileHashStash,
    SqliteHashStash,
    MemoryHashStash,
    ShelveHashStash,
    RedisHashStash,
    # PickleDBHashStash,
    DiskCacheHashStash,
    LMDBHashStash
]


@pytest.fixture(params=TEST_CLASSES)
def cache(request, tmp_path):
    cache_type = request.param
    if cache_type == MemoryHashStash:
        cache = cache_type()
    else:
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
        if not isinstance(cache, MemoryHashStash):
            assert str(cache.path).startswith(str(tmp_path))

    def test_cache_encoding(self, cache):
        test_data = {"key": "value"}
        encoded_data = cache.encode(test_data)
        assert isinstance(encoded_data, bytes if not cache.string_values else str)
        decoded_data = cache.decode(encoded_data)
        assert decoded_data == test_data

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

    @staticmethod
    def _get_cached_size(cache, key):
        return len(cache[key])
        


if __name__ == "__main__":
    pytest.main([__file__])