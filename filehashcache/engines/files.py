import shutil
import os
import json
import hashlib
import zlib
from base64 import b64encode, b64decode
from typing import Any, Optional
from abc import ABC, abstractmethod
from ..filehashcache import BaseHashCache

class FileHashCache(BaseHashCache):
    engine = 'file'
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
        newkey = f'{key[:2]}/{key[2:4]}/{key[4:]}'
        return newkey
    
    def _encode_filepath(self, key):
        return os.path.join(self.path, self._encode_key(key))
    
    
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
        print(f"Item written to: {filepath}")  # Debug print

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
        shutil.rmtree(self.path, ignore_errors=True)
        os.makedirs(self.path, exist_ok=True)

    def __len__(self) -> int:
        """Return the number of items in the cache."""
        print(f"Cache path: {self.path}")
        count = 0
        for root, dirs, files in os.walk(self.path):
            print(f"Scanning directory: {root}")
            print(f"Files found: {files}")
            count += len(files)
        print(f"Total count: {count}")
        return count

    def __iter__(self):
        """Iterate over all keys in the cache."""
        for root, dirs, files in os.walk(self.path):
            for file in files:
                path = os.path.join(root, file)
                yield path.split(f'/{self.filename}/')[-1]
