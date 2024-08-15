from functools import cached_property, lru_cache
from typing import *
from base64 import b64encode, b64decode
import os
import logging
fcache = lru_cache(maxsize=None)


# Configure logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

DEFAULT_COMPRESS=True
DEFAULT_B64=True


@fcache
def Cache(
    engine: Literal["file", "sqlite", "memory", "shelve"] = "file",
    root_dir=".cache",
    *args,
    **kwargs,
) -> 'BaseHashCache':
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