from typing import Any
from ..filehashcache import BaseHashCache

IN_MEMORY_CACHE = {}

class MemoryHashCache(BaseHashCache):
    engine = 'memory'
    filename = 'in_memory'

    def __init__(
        self,
        root_dir: str = ".cache",
        compress: bool = True,
        b64: bool = True,
    ) -> None:
        super().__init__(
            root_dir=root_dir,
            compress=compress,
            b64=b64,
            ensure_dir=False
        )
        self._cache = IN_MEMORY_CACHE

    def __setitem__(self, key: str, value: Any) -> None:
        self._cache[self._encode_key(key)] = self._encode_value(value)

    def __getitem__(self, key: str) -> Any:
        try:
            return self._decode_value(self._cache[self._encode_key(key)])
        except KeyError:
            raise KeyError(key)

    def __contains__(self, key: str) -> bool:
        return self._encode_key(key) in self._cache

    def clear(self) -> None:
        self._cache.clear()

    def __len__(self) -> int:
        return len(self._cache)

    def __iter__(self):
        yield from self._cache.keys()