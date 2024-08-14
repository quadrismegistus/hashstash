import os
from typing import Any
from ..filehashcache import BaseHashCache


IN_MEMORY_CACHE = {}

class MemoryHashCache(BaseHashCache):
    def __init__(self, root_dir='', compress: bool = True, b64: bool = True) -> None:
        # global IN_MEMORY_CACHE
        super().__init__(compress=compress, b64=b64)
        self._cache = IN_MEMORY_CACHE

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass  # No cleanup needed for in-memory cache

    def __setitem__(self, key: str, value: Any) -> None:
        self._cache[key] = self._encode_cache(value)

    def __getitem__(self, key: str) -> Any:
        return self._decode_cache(self._cache[key])

    def __contains__(self, key: str) -> bool:
        return key in self._cache

    def get(self, key: str, default: Any = None) -> Any:
        try:
            return self[key]
        except KeyError:
            return default

    def clear(self) -> None:
        self._cache.clear()

    def __len__(self) -> int:
        return len(self._cache)

    def __iter__(self):
        return iter(self._cache)