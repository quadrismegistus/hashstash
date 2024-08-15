from functools import cached_property
import pickle
import base64
import os
from typing import *
import inspect
import json
import hashlib
import zlib
from base64 import b64encode, b64decode
from typing import Any, Optional, Literal
from abc import ABC, abstractmethod
import logging
import functools

# Configure logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

DEFAULT_COMPRESS=True
DEFAULT_B64=True

class BaseHashCache(ABC):
    root_dir = ".cache"
    engine = 'base'
    filename = "db"
    dbname = "unnamed"
    compress = DEFAULT_COMPRESS
    b64 = DEFAULT_B64

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

        self.path = self.dir = os.path.abspath(os.path.join(self.root_dir, self.engine, self.filename))
        self._cache = {}
        self._db = None
        self._db_r = None
        fn,ext=os.path.splitext(self.path)
        if ext:
            self.dir = os.path.dirname(self.path)
        if ensure_dir and not os.path.exists(self.dir):
            os.makedirs(self.dir, exist_ok=True)

    @cached_property
    def db(self):
        if not self._db:
            self._db = self.get_db()
        return self._db

    @cached_property
    def db_r(self):
        if not self._db_r:
            self._db_r = self.get_db(read_only=True)
        return self._db_r

    def get_db(self, read_only=False):
        return

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        for dbkey in ["_db", "_db_r"]:
            db = getattr(self, dbkey, None)
            if db is not None:
                db.close()
                setattr(self, dbkey, None)

    @abstractmethod
    def __setitem__(self, key: Any, value: Any) -> None:
        pass

    @abstractmethod
    def __getitem__(self, key: Any) -> Any:
        pass

    @abstractmethod
    def __contains__(self, key: Any) -> bool:
        pass

    @abstractmethod
    def __delitem__(self, key: Any) -> None:
        pass

    def get(self, key: Any, default: Any = None) -> Any:
        try:
            return self[key]
        except KeyError:
            return default

    @abstractmethod
    def clear(self) -> None:
        pass

    def keys(self):
        return (self._decode_key(x) for x in self._keys())
    
    @abstractmethod
    def _keys(self):
        pass
    
    def __iter__(self):
        return self.keys()
    
    def __len__(self) -> int:
        count = 0
        for _ in self._keys():
            count+=1
        return count
    
    def values(self):
        return (self[key] for key in self)
    
    def items(self):
        return ((key, self[key]) for key in self)
    

    @staticmethod
    def _encode_jsonb(obj):
        return json.dumps(obj).encode()
    
    @staticmethod
    def is_jsonable(obj):
        try:
            json.dumps(obj)
            return True
        except Exception:
            return False

    @staticmethod
    def _encode_hash(obj):
        return hashlib.md5(obj).hexdigest()
    
    @classmethod
    def _encode_binary(cls, obj):
        if isinstance(obj, type):
            return obj.__name__.encode()
        
        if isinstance(obj, (dict, list)):
            obj = cls._recursive_encode(obj)
        
        
        try:
            return cls._encode_jsonb(obj)
        except Exception as e:
            logger.info(f"JSON encoding failed: {e} {obj}")
            return str(obj).encode()

    @classmethod
    def _recursive_encode(cls, obj):
        if isinstance(obj, dict):
            return {cls._recursive_encode(k): cls._recursive_encode(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [cls._recursive_encode(item) for item in obj]
        elif isinstance(obj, tuple):
            return tuple(cls._recursive_encode(item) for item in obj)
        elif isinstance(obj, type):
            return obj.__name__
        elif hasattr(obj, 'read') and callable(obj.read):  # Check for file-like objects
            return obj.read()
        elif not cls.is_jsonable(obj):
            try:
                # Pickle and base64 encode non-JSON-serializable objects
                return {
                    "__pickle__": base64.b64encode(pickle.dumps(obj)).decode('utf-8')
                }
            except (pickle.PicklingError, TypeError):
                # If pickling fails, return a string representation
                logger.debug(f"<unpicklable object: {type(obj).__name__}>")
                return str(obj)
        return obj
    
    @classmethod
    def _recursive_decode(cls, obj):
        if isinstance(obj, dict):
            if "__pickle__" in obj:
                # Decode and unpickle the object
                return pickle.loads(base64.b64decode(obj["__pickle__"].encode('utf-8')))
            return {cls._recursive_decode(k): cls._recursive_decode(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [cls._recursive_decode(item) for item in obj]
        elif isinstance(obj, tuple):
            return tuple(cls._recursive_decode(item) for item in obj)
        return obj

    @classmethod
    def _encode_key(self, obj):
        # return self._encode_hash(self._encode_binary(list(objs)))
        return self._encode_value(obj, compress=False, b64=True).decode()
    
    @classmethod
    def _decode_key(self, key):
        # return self._encode_hash(self._encode_binary(list(objs)))
        return self._decode_value(key.encode(), compress=False, b64=True)
    
    @staticmethod
    def _encode_zlib(data, verbose=True):
        try:
            return zlib.compress(data)
        except Exception as e:
            logger.debug(f"Compression error: {e}")
            return data
        
    @staticmethod
    def _encode_b64(data, verbose=True):
        try:
            return b64encode(data)
        except Exception as e:
            logger.debug(f"Base64 encoding error: {e}")
            return data


    @classmethod
    def _encode_value(self, obj: Any, compress: bool = DEFAULT_COMPRESS, b64: bool = DEFAULT_B64) -> bytes:
        # Use the recursive encode method
        encoded_data = self._recursive_encode(obj)
        
        # Convert to JSON
        data = json.dumps(encoded_data).encode()
        
        if compress is not None:
            if compress:
                data = self._encode_zlib(data)
        elif self.compress:
            data = self._encode_zlib(data)
        
        if b64 is not None:
            if b64:
                data = self._encode_b64(data)
        elif self.b64:
            data = self._encode_b64(data)
        
        return data

    @classmethod
    def _decode_value(
        self,
        x: bytes,
        compress: bool = DEFAULT_COMPRESS,
        b64: bool = DEFAULT_B64,
    ) -> Any:
        if b64 is not None or self.b64:
            if b64:
                x = self._decode_b64(x)
        elif self.b64:
            x = self._decode_b64(x)
        
        if compress is not None or self.compress:
            if compress:
                x = self._decode_zlib(x)
        elif self.compress:
            x = self._decode_zlib(x)
        
        try:
            decoded_json = json.loads(x.decode())
            # Use the recursive decode method
            return self._recursive_decode(decoded_json)
        except json.JSONDecodeError as e:
            logger.debug(f"JSON decoding error: {e}")
            return x.decode()  # Return as string if JSON decoding fails
        except Exception as e:
            logger.error(f"Unexpected error during decoding: {e}")
            return None
    
    @staticmethod
    def _decode_b64(data: bytes) -> bytes:
        try:
            return b64decode(data)
        except Exception as e:
            logger.debug(f"Base64 decoding error: {e}")
            return data

    @staticmethod
    def _decode_zlib(data: bytes) -> bytes:
        try:
            return zlib.decompress(data)
        except Exception as e:
            logger.debug(f"Decompression error: {e}")
            return data

    @staticmethod
    def _decode_json(cls, data: bytes) -> Any:
        try:
            decoded = json.loads(data.decode())
            return cls._recursive_decode(decoded)
        except json.JSONDecodeError as e:
            logger.debug(f"JSON decoding error: {e}")
            return data.decode()  # Return as string if JSON decoding fails
        except Exception as e:
            #logger.error(f"Unexpected error during decoding: {e}")
            return None
    
    @classmethod
    def _recursive_decode(cls, obj):
        if isinstance(obj, dict):
            if "__pickle__" in obj:
                # Decode and unpickle the object
                return pickle.loads(base64.b64decode(obj["__pickle__"].encode('utf-8')))
            return {cls._recursive_decode(k): cls._recursive_decode(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [cls._recursive_decode(item) for item in obj]
        elif isinstance(obj, tuple):
            return tuple(cls._recursive_decode(item) for item in obj)
        return obj
    
    @property
    def decorator(self) -> Callable:
        def decorator(func: Callable) -> Callable:
            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                # Extract _force from kwargs, defaulting to False
                _force = kwargs.pop('_force', False)
                
                # Create a unique key based on the function contents
                try:
                    func_code = inspect.getsource(func)
                except Exception:
                    func_code = func.__name__
                key = (func_code, args, kwargs)

                # Check if the result is already in the cache and _force is False
                if not _force and key in self:
                    logger.debug(f"Cache hit for {func.__name__}. Returning cached result.")
                    return self[key]
                
                # If _force is True or cache miss, call the function
                logger.debug(f"{'Forced execution' if _force else 'Cache miss'} for {func.__name__}. Executing function.")
                result = func(*args, **kwargs)
                logger.debug(f"Caching result for {func.__name__}.")
                self[key] = result
                return result
            return wrapper
        return decorator

    def _get(self, encoded_key: str) -> Any:
        try:
            encoded_value = self.db[encoded_key]
            return self._decode_value(encoded_value)
        except KeyError:
            raise KeyError(encoded_key)

    def _set(self, encoded_key: str, value: Any) -> None:
        encoded_value = self._encode_value(value)
        self._cache[encoded_key] = encoded_value

    def _contains(self, encoded_key: str) -> bool:
        return encoded_key in self._cache


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
    elif engine == "redis":
        from .engines.redis import RedisHashCache
        return RedisHashCache(*args, **kwargs)
    else:
        raise ValueError(
            f"Invalid engine: {engine}. Choose 'file', 'sqlite', 'memory', or 'shelve'."
        )