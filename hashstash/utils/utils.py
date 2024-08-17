from ..hashstash import *
from .pmap import *


@fcache
def HashStash(
    name: str = DEFAULT_NAME,
    engine: ENGINE_TYPES = DEFAULT_ENGINE_TYPE,
    root_dir: str = None,
    compress: bool = None,
    b64: bool = None,
    filename: str = None,
    dbname: str = None,
    **kwargs,
) -> "BaseHashStash":
    """
    Factory function to create the appropriate cache object.

    Args:
        * args: Additional arguments to pass to the cache constructor.
        engine: The type of cache to create ("file", "sqlite", "memory", "shelve", "redis", "pickledb", or "diskcache")
        **kwargs: Additional keyword arguments to pass to the cache constructor.

    Returns:
        An instance of the appropriate BaseHashStash subclass.

    Raises:
        ValueError: If an invalid engine is provided.
    """
    logger.debug(
        f"Cache called with engine: {engine}, name: {name}, kwargs: {kwargs}"
    )

    if engine == "file":
        from ..engines.files import FileHashStash

        cls = FileHashStash
    elif engine == "sqlite":
        from ..engines.sqlite import SqliteHashStash

        cls = SqliteHashStash
    elif engine == "memory":
        from ..engines.memory import MemoryHashStash

        cls = MemoryHashStash
    elif engine == "shelve":
        from ..engines.shelve import ShelveHashStash

        cls = ShelveHashStash
    elif engine == "redis":
        from ..engines.redis import RedisHashStash

        cls = RedisHashStash
    elif engine == "pickledb":
        from ..engines.pickledb import PickleDBHashStash

        cls = PickleDBHashStash
    elif engine == "diskcache":
        from ..engines.diskcache import DiskCacheHashStash

        cls = DiskCacheHashStash
    elif engine == "lmdb":
        from ..engines.lmdb import LMDBHashStash
        cls = LMDBHashStash
    else:
        raise ValueError(f"Invalid engine: {engine}. Options: {", ".join(ENGINES)}.")
    
    object = cls(
        name=name,
        root_dir=root_dir,
        compress=compress,
        b64=b64,
        filename=filename,
        dbname=dbname,
        **kwargs
    )
    print(f'Created: {object}')
    return object


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

                    delay = min(
                        base_delay * (2**retries)
                        + random.uniform(0, 0.1 * (2**retries)),
                        max_delay,
                    )
                    # print(f"Attempt {retries}/{max_retries} failed. Retrying in {delay:.2f} seconds. Error: {str(e)}")
                    time.sleep(delay)

        return wrapper

    return decorator


def cached_result(
    _func=None,
    *cache_args,
    cache: Optional["BaseHashStash"] = None,
    force=False,
    **cache_kwargs,
):
    def decorator(func: Callable, _force=force) -> Callable:
        @wraps(func)
        def wrapper(*args, _force=_force, **kwargs):
            nonlocal cache
            cache_context = (
                cache if cache is not None else HashStash(*cache_args, **cache_kwargs)
            )
            force = kwargs.pop("_force", _force)

            from .serialize import Serializer

            key = Serializer.encode((func, args, kwargs))
            if not force and key in cache_context:
                logger.debug(f"Cache hit for {func.__name__}. Returning cached result.")
                return cache_context[key]

            logger.debug(
                f"{'Forced execution' if force else 'Cache miss'} for {func.__name__}. Executing function."
            )
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
    func_start = next(
        (i for i, ln in enumerate(lines) if ln.lstrip().startswith("def ")), None
    )
    if func_start is not None:
        # Extract function body (including definition line)
        func_body = lines[func_start:]
        # Dedent the function body
        dedented_func = textwrap.dedent("\n".join(func_body))
        return dedented_func
    return ""  # Return empty string if function definition not found


def get_function_type(func):
    return get_function_type_src(func)


def get_function_type_src(func, args=None):
    try:
        source = inspect.getsource(func)
        lines = [ln.strip() for ln in source.splitlines() if ln.strip()]
        if not lines:
            return "function"  # default

        if any(ln.startswith("@classmethod") for ln in lines):
            return "classmethod"
        if any(ln.startswith("@staticmethod") for ln in lines):
            return "staticmethod"

        flines = [ln for ln in lines if ln.startswith("def ")]
        if flines:
            fline = flines[0][4:]
            fname, argstr = fline.split("(", 1)
            farg = argstr.split(",")[0].strip()
            if farg == "self":
                return "method"
            elif farg == "cls":
                return "classmethod"

        # If none of the above conditions are met, it's a regular function
        return "function"
    except Exception:
        # If we can't get the source (e.g., built-in functions), assume it's a function
        return "function"


def generate_profile_sizes(
    num_sizes: int = NUM_PROFILE_SIZES,
    multiplier: int = PROFILE_SIZE_MULTIPLIER,
    initial_size: int = INITIAL_PROFILE_SIZE,
) -> tuple:
    profile_sizes = []
    for n in range(num_sizes):
        if not profile_sizes:
            profile_sizes.append(initial_size)
        else:
            profile_sizes.append(profile_sizes[-1] * multiplier)
    return tuple(profile_sizes)
