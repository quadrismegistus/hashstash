import unittest
import tempfile
import shutil
import os
import json
import random
from filehashcache import FileHashCache
from filehashcache.engines.sqlite import SqliteHashCache
import statistics

class TestHashCache(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.file_cache = FileHashCache(os.path.join(self.temp_dir, "file_cache"))
        self.sqlite_cache = SqliteHashCache(os.path.join(self.temp_dir, "sqlite_cache.db"))

    def tearDown(self):
        shutil.rmtree(self.temp_dir)

    def test_set_get(self):
        for cache in [self.file_cache, self.sqlite_cache]:
            with self.subTest(cache=type(cache).__name__):
                cache["test_key"] = "test_value"
                self.assertEqual(cache["test_key"], "test_value")

    def test_contains(self):
        for cache in [self.file_cache, self.sqlite_cache]:
            with self.subTest(cache=type(cache).__name__):
                cache["test_key"] = "test_value"
                self.assertTrue("test_key" in cache)
                self.assertFalse("non_existent_key" in cache)

    def test_get_default(self):
        for cache in [self.file_cache, self.sqlite_cache]:
            with self.subTest(cache=type(cache).__name__):
                self.assertEqual(cache.get("non_existent_key", "default"), "default")

    def test_clear(self):
        for cache in [self.file_cache, self.sqlite_cache]:
            with self.subTest(cache=type(cache).__name__):
                cache["test_key1"] = "test_value1"
                cache["test_key2"] = "test_value2"
                cache.clear()
                self.assertEqual(len(cache), 0)

    def test_len(self):
        for cache in [self.file_cache, self.sqlite_cache]:
            with self.subTest(cache=type(cache).__name__):
                cache["test_key1"] = "test_value1"
                cache["test_key2"] = "test_value2"
                self.assertEqual(len(cache), 2)

    def test_encoding_size_reduction(self):
        for cache in [self.file_cache, self.sqlite_cache]:
            with self.subTest(cache=type(cache).__name__):
                large_data = {f"key_{i}": f"value_{i}" * 1000 for i in range(1000)}
                raw_size = len(json.dumps(large_data).encode())
                
                cache["large_data"] = large_data
                
                if isinstance(cache, FileHashCache):
                    file_path = cache._get_file_path("large_data")
                    cached_size = os.path.getsize(file_path)
                else:
                    cached_size = len(cache["large_data"])
                
                self.assertLess(cached_size, raw_size)
                
                compression_ratio = cached_size / raw_size
                print(f"Compression ratio ({type(cache).__name__}): {compression_ratio:.2%}")
                
                retrieved_data = cache["large_data"]
                self.assertEqual(retrieved_data, large_data)

    def test_multiple_data_types(self):
        for cache in [self.file_cache, self.sqlite_cache]:
            with self.subTest(cache=type(cache).__name__):
                test_data = {
                    "string": "Hello, world!" * 1000,
                    "number": 12345,
                    "list": list(range(1000)),
                    "nested": {"a": [1, 2, 3], "b": {"c": "nested" * 100}}
                }
                
                raw_size = len(json.dumps(test_data).encode())
                
                cache["test_data"] = test_data
                
                if isinstance(cache, FileHashCache):
                    file_path = cache._get_file_path("test_data")
                    cached_size = os.path.getsize(file_path)
                else:
                    cached_size = len(cache.db["test_data"])
                
                self.assertLess(cached_size, raw_size)
                
                compression_ratio = cached_size / raw_size
                print(f"Compression ratio for mixed data ({type(cache).__name__}): {compression_ratio:.2%}")
                
                retrieved_data = cache["test_data"]
                self.assertEqual(retrieved_data, test_data)

    def test_very_large_data_compression(self):
        for cache in [self.file_cache, self.sqlite_cache]:
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
                
                if isinstance(cache, FileHashCache):
                    file_path = cache._get_file_path("very_large_data")
                    cached_size = os.path.getsize(file_path)
                else:
                    cached_size = len(cache.db["very_large_data"])
                
                self.assertLess(cached_size, raw_size)
                
                compression_ratio = cached_size / raw_size
                print(f"Very large data compression ({type(cache).__name__}):")
                print(f"Raw size: {raw_size / 1024 / 1024:.2f} MB")
                print(f"Cached size: {cached_size / 1024 / 1024:.2f} MB")
                print(f"Compression ratio: {compression_ratio:.2%}")
                print(f"Space saved: {(raw_size - cached_size) / 1024 / 1024:.2f} MB")
                
                retrieved_data = cache["very_large_data"]
                self.assertEqual(retrieved_data, very_large_data)

if __name__ == '__main__':
    unittest.main()