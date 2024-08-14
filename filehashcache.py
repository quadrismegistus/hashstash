import os
import json
import hashlib
import zlib
from base64 import b64encode, b64decode
from typing import Any, Optional

__version__ = "0.1.0"

class FileHashCache:
    """A simple file-based caching system using hash-based file names.

    This class provides a dictionary-like interface for caching objects to disk.
    It uses a two-level directory structure to organize cached files.
    """

    def __init__(self, root_dir: str = ".cache") -> None:
        """Initialize the FileHashCache.

        Args:
            root_dir: The root directory for storing cached files.
        """
        self.root_dir = root_dir
        os.makedirs(root_dir, exist_ok=True)

    def _get_file_path(self, key: str) -> str:
        """Get the file path for a given key.

        Args:
            key: The cache key.

        Returns:
            The file path for the given key.
        """
        hashed_key = hashlib.md5(key.encode()).hexdigest()
        dir1, dir2 = hashed_key[:2], hashed_key[2:4]
        file_name = hashed_key[4:]
        
        dir_path = os.path.join(self.root_dir, dir1, dir2)
        os.makedirs(dir_path, exist_ok=True)
        
        return os.path.join(dir_path, file_name)

    def __setitem__(self, key: str, value: Any) -> None:
        """Set an item in the cache.

        Args:
            key: The cache key.
            value: The value to cache.
        """
        file_path = self._get_file_path(key)
        with open(file_path, 'wb') as f:
            f.write(self._encode_cache(value))

    def __getitem__(self, key: str) -> Any:
        """Get an item from the cache.

        Args:
            key: The cache key.

        Returns:
            The cached value.

        Raises:
            KeyError: If the key is not found in the cache.
        """
        file_path = self._get_file_path(key)
        if not os.path.exists(file_path):
            raise KeyError(key)
        with open(file_path, 'rb') as f:
            return self._decode_cache(f.read())

    def __contains__(self, key: str) -> bool:
        """Check if a key exists in the cache.

        Args:
            key: The cache key.

        Returns:
            True if the key exists, False otherwise.
        """
        return os.path.exists(self._get_file_path(key))

    def get(self, key: str, default: Any = None) -> Any:
        """Get an item from the cache with a default value.

        Args:
            key: The cache key.
            default: The default value to return if the key is not found.

        Returns:
            The cached value or the default value.
        """
        try:
            return self[key]
        except KeyError:
            return default

    @staticmethod
    def _encode_cache(x: Any) -> bytes:
        """Encode an object for caching.

        Args:
            x: The object to encode.

        Returns:
            The encoded object as bytes.
        """
        return b64encode(zlib.compress(json.dumps(x).encode()))

    @staticmethod
    def _decode_cache(x: bytes) -> Any:
        """Decode a cached object.

        Args:
            x: The encoded object.

        Returns:
            The decoded object.
        """
        return json.loads(zlib.decompress(b64decode(x)).decode())

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