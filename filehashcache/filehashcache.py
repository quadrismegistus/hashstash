import os
import json
import hashlib
import zlib
from base64 import b64encode, b64decode
from typing import Any, Optional, Literal
from abc import ABC, abstractmethod
import logging

# Configure logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class BaseHashCache(ABC):
    root_dir = ".cache"
    engine = 'base'
    filename = "db"
    dbname = "unnamed"
    compress = True
    b64 = False

    def __init__(
        self,
        root_dir: str = None,
        compress: bool = None,
        b64: bool = None,
        filename: str = None,
        dbname: str = None,
        ensure_dir: bool = True,
    ) -> None:
        if root_dir is not None:
            self.root_dir = root_dir
        if compress is not None:
            self.compress = compress
        if b64 is not None:
            self.b64 = b64
        if filename is not None:
            self.filename = filename
        if dbname is not None:
            self.dbname = dbname

        self.path = self.dir = os.path.join(self.root_dir, self.engine, self.filename)
        fn,ext=os.path.splitext(self.path)
        if ext:
            self.dir = os.path.dirname(self.path)
        if ensure_dir and not os.path.exists(self.dir):
            os.makedirs(self.dir, exist_ok=True)

    def __enter__(self):
        return self

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

    def get(self, key: str, default: Any = None) -> Any:
        try:
            return self[key]
        except KeyError:
            return default

    @abstractmethod
    def clear(self) -> None:
        pass

    @abstractmethod
    def __len__(self) -> int:
        pass

    @abstractmethod
    def __iter__(self):
        pass

    @staticmethod
    def _encode_jsonb(obj):
        if hasattr(obj, "to_json"):
            obj = obj.to_json()
        return json.dumps(obj).encode()

    @staticmethod
    def _encode_hash(obj):
        return hashlib.md5(obj).hexdigest()

    @classmethod
    def _encode_key(self, obj):
        return self._encode_hash(self._encode_jsonb(obj))

    @classmethod
    def _encode_value(self, obj: Any, compress: bool = None, b64: bool = None) -> bytes:
        data = self._encode_jsonb(obj)
        if compress or self.compress:
            try:
                data = zlib.compress(data)
            except Exception as e:
                logger.error(e)
        if b64 or self.b64:
            try:
                data = b64encode(data)
            except Exception as e:
                logger.error(e)
        return data

    @classmethod
    def _decode_value(
        self,
        x: bytes,
        compress: bool = None,
        b64: bool = None,
    ) -> Any:
        if b64 or self.b64:
            try:
                x = b64decode(x)
            except Exception as e:
                logger.error(e)
        if compress or self.compress:
            try:
                x = zlib.decompress(x)
            except Exception as e:
                logger.error(e)
        return json.loads(x.decode())


def Cache(
    *args,
    root_dir=".cache",
    engine: Literal["file", "sqlite", "memory", "shelve"] = "file",
    **kwargs,
) -> BaseHashCache:
    """
    Factory function to create the appropriate cache object.

    Args:
        * args: Additional arguments to pass to the cache constructor.
        engine: The type of cache to create ("file", "sqlite", "memory", or "shelve")
        **kwargs: Additional keyword arguments to pass to the cache constructor.

    Returns:
        An instance of the appropriate BaseHashCache subclass.

    Raises:
        ValueError: If an invalid engine is provided.
    """
    os.makedirs(root_dir, exist_ok=True)

    if engine == "file":
        from .engines.files import FileHashCache

        return FileHashCache(*args, **kwargs)
    elif engine == "sqlite":
        from .engines.sqlite import SqliteHashCache

        return SqliteHashCache(*args, **kwargs)
    elif engine == "memory":
        from .engines.memory import MemoryHashCache

        return MemoryHashCache(*args, **kwargs)
    elif engine == "shelve":
        from .engines.shelve import ShelveHashCache

        return ShelveHashCache(*args, **kwargs)
    else:
        raise ValueError(
            f"Invalid engine: {engine}. Choose 'file', 'sqlite', 'memory', or 'shelve'."
        )
