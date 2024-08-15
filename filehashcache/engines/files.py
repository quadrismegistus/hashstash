import shutil
import os
import json
import hashlib
import zlib
from base64 import b64encode, b64decode
from typing import Any, Optional
from abc import ABC, abstractmethod
from .base import BaseHashCache
from ..constants import *

class FileHashCache(BaseHashCache):
    engine = 'file'
    filename = 'dirs'
    
    def _encode_filepath(self, key):
        key = super()._encode_key(key)
        dirname,fname = key[:2], key[2:]
        n = 100
        segmented_key = [fname[i:i+n] for i in range(0, len(fname), n)]
        return os.path.join(self.path, dirname, *segmented_key)
    
    def _decode_filepath(self, filepath):
        # Remove the base path
        relative_path = os.path.relpath(filepath, self.path)
        
        # Join all parts except the first two (which represent the first two characters of the encoded key)
        encoded_key = relative_path.replace('/','')
        
        # Decode the key
        return encoded_key
    
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
        #print(f"Item written to: {filepath}")  # Debug #print

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

    def __len__(self):
        return sum(1 for x in self._paths())

    def _paths(self):
        for root, dirs, files in os.walk(self.path):
            for file in files:
                path = os.path.join(root, file)
                yield path

    def _keys(self):
        """Iterate over all keys in the cache."""
        for path in self._paths():
            yield self._decode_filepath(path)

    def __delitem__(self, key: str) -> None:
        """Delete an item from the cache.

        Args:
            key: The cache key.

        Raises:
            KeyError: If the key is not found in the cache.
        """
        filepath = self._encode_filepath(key)
        if not os.path.exists(filepath):
            raise KeyError(key)
        os.remove(filepath)
        
        # Remove empty parent directories
        dir_path = os.path.dirname(filepath)
        while dir_path != self.path:
            if not os.listdir(dir_path):
                os.rmdir(dir_path)
                dir_path = os.path.dirname(dir_path)
            else:
                break