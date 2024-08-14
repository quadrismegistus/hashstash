import os
import json
import hashlib
import zlib
from base64 import b64encode, b64decode
from typing import Any, Optional
from abc import ABC, abstractmethod
from ..filehashcache import BaseHashCache

class FileHashCache(BaseHashCache):
    filename = 'dirs'

    @classmethod
    def _encode_key(self, key: str) -> str:
        """Get the file path for a given key.

        Args:
            key: The cache key.

        Returns:
            The file path for the given key.
        """
        key = super()._encode_key(key)
        dir1, dir2 = key[:2], key[2:4]
        file_name = key[4:]
        dir_path = os.path.join(dir1, dir2)
        os.makedirs(dir_path, exist_ok=True)
        return os.path.join(dir_path, file_name)
    
    @classmethod
    def _encode_filepath(self, key):
        return os.path.join(self.root_dir, self.filename, self._encode_key(key))
    
    
    def __setitem__(self, key: str, value: Any) -> None:
        """Set an item in the cache.

        Args:
            key: The cache key.
            value: The value to cache.
        """
        filepath = self._encode_filepath(key)
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        with open(filepath, 'wb') as f:
            f.write(self._encode_value(value))

    def __getitem__(self, key: str) -> Any:
        """Get an item from the cache.

        Args:
            key: The cache key.

        Returns:
            The cached value.

        Raises:
            KeyError: If the key is not found in the cache.
        """
        filepath = self._encode_filepath(key)
        if not os.path.exists(filepath):
            raise KeyError(key)
        with open(filepath, 'rb') as f:
            return self._decode_value(f.read())

    def __contains__(self, key: str) -> bool:
        """Check if a key exists in the cache.

        Args:
            key: The cache key.

        Returns:
            True if the key exists, False otherwise.
        """
        return os.path.exists(self._encode_filepath(key))

    def clear(self) -> None:
        """Clear all items from the cache."""
        for root, dirs, files in os.walk(self.root_dir, topdown=False):
            for name in files:
                os.remove(os.path.join(root, name))
            for name in dirs:
                os.rmdir(os.path.join(root, name))

    def __len__(self) -> int:
        """Return the number of items in the cache."""
        count = 0
        for root, dirs, files in os.walk(self.root_dir):
            count += len(files)
        return count

    def __iter__(self):
        """Iterate over all keys in the cache."""
        for root, dirs, files in os.walk(self.root_dir):
            for file in files:
                yield os.path.join(root, file)