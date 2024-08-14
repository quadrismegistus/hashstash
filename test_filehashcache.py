import unittest
import tempfile
import shutil
from filehashcache import FileHashCache

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

if __name__ == '__main__':
    unittest.main()
