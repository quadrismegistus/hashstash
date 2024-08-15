import unittest
import tempfile
import shutil
import os
import json
import random
import time
import pytest
from filehashcache import FileHashCache
from filehashcache.engines.sqlite import SqliteHashCache
from filehashcache.engines.memory import MemoryHashCache
from filehashcache.engines.shelve import ShelveHashCache
from filehashcache.engines.redis import RedisHashCache

@pytest.fixture(scope="class")
def temp_dir(request):
    temp_dir = tempfile.mkdtemp()
    request.cls.temp_dir = temp_dir
    yield temp_dir
    shutil.rmtree(temp_dir)

@pytest.mark.usefixtures("temp_dir")
class TestHashCache:
    @pytest.fixture(params=[FileHashCache, SqliteHashCache, MemoryHashCache, ShelveHashCache, RedisHashCache])
    def cache(self, request):
        cache_type = request.param
        if cache_type == FileHashCache:
            return cache_type(os.path.join(self.temp_dir, "file_cache"))
        elif cache_type == SqliteHashCache:
            return cache_type(os.path.join(self.temp_dir, "sqlite_cache.db"))
        elif cache_type == MemoryHashCache:
            return cache_type()
        elif cache_type == ShelveHashCache:
            return cache_type(os.path.join(self.temp_dir, "shelve_cache"))
        elif cache_type == RedisHashCache:
            return cache_type(os.path.join(self.temp_dir, "redis_cache"))
        else:
            raise ValueError(f"Unknown cache type: {cache_type}")

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
        cache.clear()
        cache["test_key1"] = "test_value1"
        cache["test_key2"] = "test_value2"
        assert len(cache) == 2

    def test_encoding_size_reduction(self, cache):
        large_data = {f"key_{i}": f"value_{i}" * 1000 for i in range(1000)}
        raw_size = len(json.dumps(large_data).encode())
        
        cache["large_data"] = large_data
        
        if isinstance(cache, ShelveHashCache):
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
            "nested": {"a": [1, 2, 3], "b": {"c": "nested" * 100}}
        }
        
        raw_size = len(json.dumps(test_data).encode())
        
        cache["test_data"] = test_data
        if isinstance(cache, ShelveHashCache):
            time.sleep(0.1)  # Add a small delay to ensure data is written
        
        cached_size = self._get_cached_size(cache, "test_data")
        assert cached_size < raw_size
        
        compression_ratio = cached_size / raw_size
        print(f"Compression ratio for mixed data ({type(cache).__name__}): {compression_ratio:.2%}")
        
        retrieved_data = cache["test_data"]
        assert retrieved_data == test_data

    def test_very_large_data_compression(self, cache):
        very_large_data = {
            "large_string": "".join(random.choices("abcdefghijklmnopqrstuvwxyz", k=1_000_000)),
            "large_list": [random.randint(1, 1000000) for _ in range(100_000)],
            "large_nested": {
                f"key_{i}": {
                    "nested_string": "".join(random.choices("ABCDEFGHIJKLMNOPQRSTUVWXYZ", k=100)),
                    "nested_list": [random.random() for _ in range(100)]
                } for i in range(1_000)
            }
        }
        
        raw_size = len(json.dumps(very_large_data).encode())
        
        cache["very_large_data"] = very_large_data
        if isinstance(cache, ShelveHashCache):
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

    @staticmethod
    def _get_cached_size(cache, key):
        if isinstance(cache, FileHashCache):
            file_path = cache._encode_filepath(key)
            return os.path.getsize(file_path)
        else:
            return len(cache.db[cache._encode_key(key)])

if __name__ == '__main__':
    pytest.main([__file__])