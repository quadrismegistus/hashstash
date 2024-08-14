import os
import json
import hashlib
import zlib
from base64 import b64encode, b64decode
from typing import Any, Optional, Literal
from abc import ABC, abstractmethod


class BaseHashCache(ABC):
    def __init__(self, compress: bool = True, b64: bool = True) -> None:
        self.compress = compress
        self.b64 = b64

    @abstractmethod
    def __enter__(self):
        pass

    @abstractmethod
    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    @abstractmethod
    def __setitem__(self, key: str, value: Any) -> None:
        pass

    @abstractmethod
    def __getitem__(self, key: str) -> Any:
        pass

    @abstractmethod
    def __contains__(self, key: str) -> bool:
        pass

    @abstractmethod
    def get(self, key: str, default: Any = None) -> Any:
        pass

    @abstractmethod
    def clear(self) -> None:
        pass

    @abstractmethod
    def __len__(self) -> int:
        pass

    @abstractmethod
    def __iter__(self):
        pass

    def _encode_cache(self, x: Any) -> bytes:
        data = json.dumps(x).encode()
        if self.compress:
            data = zlib.compress(data)
        if self.b64:
            data = b64encode(data)
        return data

    def _decode_cache(self, x: bytes) -> Any:
        if self.b64:
            x = b64decode(x)
        if self.compress:
            x = zlib.decompress(x)
        return json.loads(x.decode())



def Cache(*args, engine: Literal["file", "sqlite", "memory"] = "file", **kwargs) -> BaseHashCache:
    """
    Factory function to create the appropriate cache object.

    Args:
        * args: Additional arguments to pass to the cache constructor.
        engine: The type of cache to create ("file", "sqlite", or "memory")
        **kwargs: Additional keyword arguments to pass to the cache constructor.

    Returns:
        An instance of the appropriate BaseHashCache subclass.

    Raises:
        ValueError: If an invalid engine is provided.
    """
    if engine == "file":
        from .engines.files import FileHashCache
        return FileHashCache(*args, **kwargs)
    elif engine == "sqlite":
        from .engines.sqlite import SqliteHashCache
        return SqliteHashCache(*args, **kwargs)
    elif engine == "memory":
        from .engines.memory import MemoryHashCache
        return MemoryHashCache(*args, **kwargs)
    else:
        raise ValueError(f"Invalid engine: {engine}. Choose 'file', 'sqlite', or 'memory'.")