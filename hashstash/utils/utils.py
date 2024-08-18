from ..hashstash import *
from .logs import *
from .pmap import *
import types

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
    logger.debug(f'Created: {object}')
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
                    # logger.debug(f"Attempt {retries}/{max_retries} failed. Retrying in {delay:.2f} seconds. Error: {str(e)}")
                    time.sleep(delay)

        return wrapper

    return decorator







def cached_result(
    _func=None,
    *cache_args,
    cache: Optional["BaseHashStash"] = None,
    name:str='cached_result',
    force=False,
    **cache_kwargs,
):
    def decorator(func: Callable, _force=force) -> Callable:
        @wraps(func)
        def wrapper(*args, _force=_force, **kwargs):
            nonlocal cache
            cache_context = (
                cache if cache is not None else HashStash(*cache_args,name=name, **cache_kwargs)
            )
            force = kwargs.pop("_force", _force)
            from ..serialize.serialize import serialize
            key = serialize((func, args, kwargs))
            print(key)
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

def get_obj_module(obj):
    if hasattr(obj, "__module__"): return obj.__module__
    if hasattr(obj, "__class__"): return get_obj_module(obj.__class__)
    return type(obj).__module__

def get_obj_addr(obj):
    if isinstance(obj, types.FunctionType):
        # Handle functions and unbound methods
        if hasattr(obj, '__qualname__'):
            return f"{obj.__module__}.{obj.__qualname__}"
        return f"{obj.__module__}.{obj.__name__}"
    
    if isinstance(obj, types.MethodType):
        # Handle bound methods
        return f"{obj.__module__}.{obj.__self__.__class__.__name__}.{obj.__name__}"
    
    if isinstance(obj, type):
        # Handle classes
        return f"{obj.__module__}.{obj.__name__}"
    
    module = get_obj_module(obj)
    name = get_obj_name(obj)
    
    # Remove any method name from the qualname
    if '.' in name:
        name = name.rsplit('.', 1)[0]
    
    return f'{module}.{name}'

def get_obj_name(obj):
    if isinstance(obj, type):
        return obj.__name__
    if hasattr(obj, '__class__'):
        return obj.__class__.__name__
    return type(obj).__name__

def is_jsonable(obj):
    return isinstance(obj, (dict, list, str, int, float, bool, type(None)))


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



def get_function_str(func):
    try:
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
    except Exception:
        return ""


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


# __all__ = ['serialize', 'deserialize']



def prune_none_values(data, badkeys=None):
    if isinstance(data, dict):
        return {k: prune_none_values(v) for k, v in data.items() if v is not None and (not badkeys or k not in badkeys)}
    elif isinstance(data, list):
        return [prune_none_values(item) for item in data if item is not None]
    else:
        return data


def flexible_import(obj_or_path):
    if isinstance(obj_or_path, str):
        parts = obj_or_path.split('.')
        module = parts.pop(0)
        obj = importlib.import_module(module)
        
        for part in parts:
            try:
                obj = getattr(obj, part)
            except AttributeError:
                # If attribute lookup fails, try importing as a module
                try:
                    obj = importlib.import_module(f"{module}.{part}")
                    module = f"{module}.{part}"
                except ImportError:
                    print(f"Error importing {obj_or_path}: {part} not found in {module}")
                    return None
        return obj
    else:
        return flexible_import(get_obj_addr(obj_or_path))

def can_import_object(obj):
    return flexible_import(obj) is not None


def call_function_politely(func, *args, **kwargs):
    """
    Gracefully call a function with refined argument handling.
    
    :param func: The function to call
    :param args: Positional arguments
    :param kwargs: Keyword arguments
    :return: The result of the function call
    """
    logger.debug(f"Preparing to call {func.__name__} with refined arguments")

    # Handle partial functions
    if isinstance(func, partial):
        partial_args = func.args
        partial_kwargs = func.keywords or {}
        func = func.func
        args = partial_args + args
        kwargs = {**partial_kwargs, **kwargs}

    # Get the function signature
    try:
        sig = inspect.signature(func)
    except Exception:
        print((func,args,kwargs))
        return func(*args,**kwargs)
    
    # Filter args and kwargs
    allowed_params = list(sig.parameters.keys())
    filtered_args = [arg for i, arg in enumerate(args) if i < len(allowed_params)]
    filtered_kwargs = {k: v for k, v in kwargs.items() if k in allowed_params}

    # Remove kwargs that are already satisfied by args
    for i, param in enumerate(allowed_params):
        if i < len(filtered_args) and param in filtered_kwargs:
            del filtered_kwargs[param]

    logger.debug(f"Calling {func.__name__} with {len(filtered_args)} args and {len(filtered_kwargs)} kwargs")
    
    try:
        return func(*filtered_args, **filtered_kwargs)
    except Exception as e:
        logger.error(f"Error calling {func.__name__}: {e}")
        raise