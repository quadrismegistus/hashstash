import unittest
import tempfile
import shutil
import os
import json
import random
from filehashcache import FileHashCache
import statistics

class TestFileHashCache(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.cache = FileHashCache(self.temp_dir)

    def tearDown(self):
        shutil.rmtree(self.temp_dir)

    def test_set_get(self):
        self.cache["test_key"] = "test_value"
        self.assertEqual(self.cache["test_key"], "test_value")

    def test_contains(self):
        self.cache["test_key"] = "test_value"
        self.assertTrue("test_key" in self.cache)
        self.assertFalse("non_existent_key" in self.cache)

    def test_get_default(self):
        self.assertEqual(self.cache.get("non_existent_key", "default"), "default")

    def test_clear(self):
        self.cache["test_key1"] = "test_value1"
        self.cache["test_key2"] = "test_value2"
        self.cache.clear()
        self.assertEqual(len(self.cache), 0)

    def test_len(self):
        self.cache["test_key1"] = "test_value1"
        self.cache["test_key2"] = "test_value2"
        self.assertEqual(len(self.cache), 2)

    def test_encoding_size_reduction(self):
        # Create a large dictionary
        large_data = {f"key_{i}": f"value_{i}" * 1000 for i in range(1000)}
        
        # Get the size of the raw JSON data
        raw_size = len(json.dumps(large_data).encode())
        
        # Store the data in the cache
        self.cache["large_data"] = large_data
        
        # Get the file path of the cached data
        file_path = self.cache._get_file_path("large_data")
        
        # Get the size of the cached file
        cached_size = os.path.getsize(file_path)
        
        # Check that the cached size is smaller than the raw size
        self.assertLess(cached_size, raw_size)
        
        # Calculate and print the compression ratio
        compression_ratio = cached_size / raw_size
        print(f"Compression ratio: {compression_ratio:.2%}")
        
        # Ensure the retrieved data matches the original
        retrieved_data = self.cache["large_data"]
        self.assertEqual(retrieved_data, large_data)

    def test_multiple_data_types(self):
        test_data = {
            "string": "Hello, world!" * 1000,
            "number": 12345,
            "list": list(range(1000)),
            "nested": {"a": [1, 2, 3], "b": {"c": "nested" * 100}}
        }
        
        raw_size = len(json.dumps(test_data).encode())
        
        self.cache["test_data"] = test_data
        file_path = self.cache._get_file_path("test_data")
        cached_size = os.path.getsize(file_path)
        
        self.assertLess(cached_size, raw_size)
        
        compression_ratio = cached_size / raw_size
        print(f"Compression ratio for mixed data: {compression_ratio:.2%}")
        
        retrieved_data = self.cache["test_data"]
        self.assertEqual(retrieved_data, test_data)

    def test_very_large_data_compression(self):
        # Create a very large dataset (approximately 100MB)
        very_large_data = {
            "large_string": "".join(random.choices("abcdefghijklmnopqrstuvwxyz", k=10_000_000)),
            "large_list": [random.randint(1, 1000000) for _ in range(1_000_000)],
            "large_nested": {
                f"key_{i}": {
                    "nested_string": "".join(random.choices("ABCDEFGHIJKLMNOPQRSTUVWXYZ", k=1000)),
                    "nested_list": [random.random() for _ in range(1000)]
                } for i in range(10_000)
            }
        }
        
        # Get the size of the raw JSON data
        raw_size = len(json.dumps(very_large_data).encode())
        
        # Store the data in the cache
        self.cache["very_large_data"] = very_large_data
        
        # Get the file path of the cached data
        file_path = self.cache._get_file_path("very_large_data")
        
        # Get the size of the cached file
        cached_size = os.path.getsize(file_path)
        
        # Check that the cached size is smaller than the raw size
        self.assertLess(cached_size, raw_size)
        
        # Calculate and print the compression ratio and sizes
        compression_ratio = cached_size / raw_size
        print(f"Very large data compression:")
        print(f"Raw size: {raw_size / 1024 / 1024:.2f} MB")
        print(f"Cached size: {cached_size / 1024 / 1024:.2f} MB")
        print(f"Compression ratio: {compression_ratio:.2%}")
        print(f"Space saved: {(raw_size - cached_size) / 1024 / 1024:.2f} MB")
        
        # Ensure the retrieved data matches the original
        retrieved_data = self.cache["very_large_data"]
        self.assertEqual(retrieved_data, very_large_data)

if __name__ == '__main__':
    unittest.main()