from ..hashdict import *
from pprint import pprint
import textwrap

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
    elif engine == "lmdb":
        from ..engines.lmdb import LMDBHashDict
        return LMDBHashDict(*args, name=name, **kwargs)
        
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
            force = kwargs.pop('_force', _force)

            from .serialize import Serializer
            key = Serializer.encode((func,args,kwargs))
            if not force and key in cache_context:
                logger.debug(f"Cache hit for {func.__name__}. Returning cached result.")
                return cache_context[key]
            
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


def get_function_str(func):
    source = inspect.getsource(func)
    lines = source.splitlines()
    # Find the function definition line
    func_start = next((i for i, ln in enumerate(lines) if ln.lstrip().startswith('def ')), None)
    if func_start is not None:
        # Extract function body (including definition line)
        func_body = lines[func_start:]
        # Dedent the function body
        dedented_func = textwrap.dedent('\n'.join(func_body))
        return dedented_func
    return ''  # Return empty string if function definition not found

def get_function_type(func):
    return get_function_type_src(func)

def get_function_type_src(func, args=None):
    try:
        source = inspect.getsource(func)
        lines = [ln.strip() for ln in source.splitlines() if ln.strip()]
        if not lines: return 'function' # default

        if any(ln.startswith('@classmethod') for ln in lines):
            return 'classmethod'
        if any(ln.startswith('@staticmethod') for ln in lines):
            return 'staticmethod'
        
        flines = [ln for ln in lines if ln.startswith('def ')]
        if flines:
            fline = flines[0][4:]
            fname,argstr = fline.split('(',1)
            farg = argstr.split(',')[0].strip()
            if farg == 'self':
                return 'method'
            elif farg == 'cls':
                return 'classmethod'
        
        # If none of the above conditions are met, it's a regular function
        return "function"
    except Exception:
        # If we can't get the source (e.g., built-in functions), assume it's a function
        return "function"