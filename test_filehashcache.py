import unittest
import tempfile
import shutil
import os
import json
import random
import time
from filehashcache import FileHashCache
from filehashcache.engines.sqlite import SqliteHashCache
from filehashcache.engines.memory import MemoryHashCache
from filehashcache.engines.shelve import ShelveHashCache
import shelve
import statistics
from filehashcache.filehashcache import BaseHashCache

class TestHashCache(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.file_cache = FileHashCache(os.path.join(self.temp_dir, "file_cache"))
        self.sqlite_cache = SqliteHashCache(os.path.join(self.temp_dir, "sqlite_cache.db"))
        self.memory_cache = MemoryHashCache()
        self.shelve_cache = ShelveHashCache(os.path.join(self.temp_dir, "shelve_cache"))
        self.caches = [self.file_cache, self.sqlite_cache, self.memory_cache, self.shelve_cache]

    def tearDown(self):
        shutil.rmtree(self.temp_dir)

    def test_set_get(self):
        for cache in self.caches:
            with self.subTest(cache=type(cache).__name__):
                cache["test_key"] = "test_value"
                self.assertEqual(cache["test_key"], "test_value")

    def test_contains(self):
        for cache in self.caches:
            with self.subTest(cache=type(cache).__name__):
                cache["test_key"] = "test_value"
                result = "test_key" in cache
                if not result:
                    print(f"test_contains failed for {type(cache).__name__}")
                self.assertTrue(result)

    def test_get_default(self):
        for cache in self.caches:
            with self.subTest(cache=type(cache).__name__):
                self.assertEqual(cache.get("non_existent_key", "default"), "default")

    def test_clear(self):
        for cache in self.caches:
            with self.subTest(cache=type(cache).__name__):
                cache["test_key1"] = "test_value1"
                cache["test_key2"] = "test_value2"
                cache.clear()
                self.assertEqual(len(cache), 0)

    def test_len(self):
        for cache in self.caches:
            with self.subTest(cache=type(cache).__name__):
                cache.clear()  # Ensure the cache is empty
                cache["test_key1"] = "test_value1"
                cache["test_key2"] = "test_value2"
                print(f"Cache path after adding items: {cache.path}")  # Debug print
                length = len(cache)
                if length != 2:
                    print(f"test_len failed for {type(cache).__name__}: len = {length}")
                self.assertEqual(length, 2)

    def test_encoding_size_reduction(self):
        for cache in self.caches:
            with self.subTest(cache=type(cache).__name__):
                large_data = {f"key_{i}": f"value_{i}" * 1000 for i in range(1000)}
                raw_size = len(json.dumps(large_data).encode())
                
                cache["large_data"] = large_data
                if isinstance(cache, ShelveHashCache):
                    time.sleep(0.1)  # Add a small delay to ensure data is written
                
                try:
                    if isinstance(cache, FileHashCache):
                        file_path = cache._encode_filepath("large_data")
                        cached_size = os.path.getsize(file_path)
                    elif isinstance(cache, SqliteHashCache):
                        cached_size = len(cache.db[cache._encode_key("large_data")])
                    elif isinstance(cache, MemoryHashCache):
                        cached_size = len(cache._cache[cache._encode_key("large_data")])
                    elif isinstance(cache, ShelveHashCache):
                        with shelve.open(cache.db_path) as db:
                            cached_size = len(db[cache._encode_key("large_data")])
                    
                    self.assertLess(cached_size, raw_size)
                    
                    compression_ratio = cached_size / raw_size
                    print(f"Compression ratio ({type(cache).__name__}): {compression_ratio:.2%}")
                    
                    retrieved_data = cache["large_data"]
                    self.assertEqual(retrieved_data, large_data)
                except KeyError:
                    self.fail(f"KeyError occurred for {type(cache).__name__}")

    def test_multiple_data_types(self):
        for cache in self.caches:
            with self.subTest(cache=type(cache).__name__):
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
                
                try:
                    if isinstance(cache, FileHashCache):
                        file_path = cache._encode_filepath("test_data")
                        cached_size = os.path.getsize(file_path)
                    elif isinstance(cache, SqliteHashCache):
                        cached_size = len(cache.db[cache._encode_key("test_data")])
                    elif isinstance(cache, MemoryHashCache):
                        cached_size = len(cache._cache[cache._encode_key("test_data")])
                    elif isinstance(cache, ShelveHashCache):
                        with shelve.open(cache.db_path) as db:
                            cached_size = len(db[cache._encode_key("test_data")])
                    
                    self.assertLess(cached_size, raw_size)
                    
                    compression_ratio = cached_size / raw_size
                    print(f"Compression ratio for mixed data ({type(cache).__name__}): {compression_ratio:.2%}")
                    
                    retrieved_data = cache["test_data"]
                    self.assertEqual(retrieved_data, test_data)
                except KeyError:
                    self.fail(f"KeyError occurred for {type(cache).__name__}")

    def test_very_large_data_compression(self):
        for cache in self.caches:
            with self.subTest(cache=type(cache).__name__):
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
                
                try:
                    if isinstance(cache, FileHashCache):
                        file_path = cache._encode_filepath("very_large_data")
                        cached_size = os.path.getsize(file_path)
                    elif isinstance(cache, SqliteHashCache):
                        cached_size = len(cache.db[cache._encode_key("very_large_data")])
                    elif isinstance(cache, MemoryHashCache):
                        cached_size = len(cache._cache[cache._encode_key("very_large_data")])
                    elif isinstance(cache, ShelveHashCache):
                        with shelve.open(cache.db_path) as db:
                            cached_size = len(db[cache._encode_key("very_large_data")])
                    
                    self.assertLess(cached_size, raw_size)
                    
                    compression_ratio = cached_size / raw_size
                    print(f"Very large data compression ({type(cache).__name__}):")
                    print(f"Raw size: {raw_size / 1024 / 1024:.2f} MB")
                    print(f"Cached size: {cached_size / 1024 / 1024:.2f} MB")
                    print(f"Compression ratio: {compression_ratio:.2%}")
                    print(f"Space saved: {(raw_size - cached_size) / 1024 / 1024:.2f} MB")
                    
                    retrieved_data = cache["very_large_data"]
                    self.assertEqual(retrieved_data, very_large_data)
                except KeyError:
                    self.fail(f"KeyError occurred for {type(cache).__name__}")

if __name__ == '__main__':
    unittest.main()