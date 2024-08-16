from ..hashdict import *

@fcache
def HashDict(
    name: str = DEFAULT_NAME,
    engine: ENGINE_TYPES = DEFAULT_ENGINE_TYPE,
    *args,
    **kwargs,
) -> 'BaseHashDict':
    """
    Factory function to create the appropriate cache object.

    Args:
        * args: Additional arguments to pass to the cache constructor.
        engine: The type of cache to create ("file", "sqlite", "memory", "shelve", "redis", "pickledb", or "diskcache")
        **kwargs: Additional keyword arguments to pass to the cache constructor.

    Returns:
        An instance of the appropriate BaseHashDict subclass.

    Raises:
        ValueError: If an invalid engine is provided.
    """
    logger.debug(f"Cache called with engine: {engine}, name: {name}, args: {args}, kwargs: {kwargs}")
    
    if engine == "file":
        from ..engines.files import FileHashDict

        return FileHashDict(*args, name=name, **kwargs)
    elif engine == "sqlite":
        from ..engines.sqlite import SqliteHashDict

        return SqliteHashDict(*args, name=name,**kwargs)
    elif engine == "memory":
        from ..engines.memory import MemoryHashDict

        return MemoryHashDict(*args, name=name,**kwargs)
    elif engine == "shelve":
        from ..engines.shelve import ShelveHashDict

        return ShelveHashDict(*args, name=name,**kwargs)
    elif engine == "redis":
        from ..engines.redis import RedisHashDict
        return RedisHashDict(*args, name=name,**kwargs)
    elif engine == "pickledb":
        from ..engines.pickledb import PickleDBHashDict
        return PickleDBHashDict(*args, name=name, **kwargs)
    elif engine == "diskcache":
        from ..engines.diskcache import DiskCacheHashDict
        return DiskCacheHashDict(*args, name=name, **kwargs)
    else:
        raise ValueError(
            f"Invalid engine: {engine}. Choose 'file', 'sqlite', 'memory', 'shelve', 'redis', 'pickledb', or 'diskcache'."
        )
    



def retry_patiently(max_retries=10, base_delay=0.1, max_delay=10):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            retries = 0
            while True:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    retries += 1
                    if retries > max_retries:
                        raise  # Re-raise the exception if max retries reached
                    
                    delay = min(base_delay * (2 ** retries) + random.uniform(0, 0.1 * (2 ** retries)), max_delay)
                    # print(f"Attempt {retries}/{max_retries} failed. Retrying in {delay:.2f} seconds. Error: {str(e)}")
                    time.sleep(delay)
        return wrapper
    return decorator


def cached_result(_func=None, *cache_args, cache: Optional['BaseHashDict'] = None, force=False, **cache_kwargs):
    def decorator(func: Callable, _force=force) -> Callable:
        @wraps(func)
        def wrapper(*args, _force=_force, **kwargs):
            nonlocal cache
            cache_context = cache if cache is not None else HashDict(*cache_args, **cache_kwargs)
            # Create a unique key based on the function contents
            try:
                func_code = inspect.getsource(func).strip()
                if func_code and func_code[0]=='@' and '\n' in func_code:
                    func_code='\n'.join(func_code.split('\n')[1:])
            except Exception:
                func_code = func.__name__
            key = (func_code, args, kwargs)

            force = kwargs.pop('_force', _force)
            # Check if the result is already in the cache and not forcing
            if not force and key in cache_context:
                logger.debug(f"Cache hit for {func.__name__}. Returning cached result.")
                return cache_context[key]
            
            # If forcing or cache miss, call the function
            logger.debug(f"{'Forced execution' if force else 'Cache miss'} for {func.__name__}. Executing function.")
            result = func(*args, **kwargs)
            logger.debug(f"Caching result for {func.__name__}.")
            cache_context[key] = result
            return result
        return wrapper

    if _func is None:
        return decorator
    else:
        return decorator(_func)


class DictContext(UserDict):
    def __init__(self, data, *args, **kwargs):
        self.data = data

    def __enter__(self):
        return self.data

    def __exit__(self, exc_type, exc_value, traceback):
        pass  # Nothing happens on close
