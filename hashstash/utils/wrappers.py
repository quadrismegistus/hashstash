from . import *


@log.info
def cached_result(
    _func=None,
    *cache_args,
    cache: Optional["BaseHashStash"] = None,
    name:str=None,
    dbname:str=None,
    force=False,
    **cache_kwargs,
):
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args,**kwargs):
            from .encodings import encode_hash
            from ..engines import HashStash

            nonlocal cache, name, force, dbname, func

            if cache is None:
                
                cache = HashStash(*cache_args, name=name, **cache_kwargs)
            if dbname is None:
                func_name = get_obj_addr(func)
                # if func_name.startswith('__main__'):
                func_name+='__'+encode_hash(get_function_str(func))[:8]
                dbname = '/'.join(['cached_result', func_name])
            cache = cache.sub(dbname = dbname)
            wrapper.stash = cache
            log.info(cache)
            force = kwargs.pop("_force", force)
            
            key = {
                'func':func,
                'args':tuple(args),
                'kwargs': kwargs
            }
            
            if not force and key in cache:
                log.debug(f"Cache hit for {func.__name__}. Returning cached result.")
                return cache[key]

            log.debug(
                f"{'Forced execution' if force else 'Cache miss'} for {func.__name__}. Executing function."
            )
            result = func(*args, **kwargs)
            log.debug(f"Caching result for {func.__name__}.")
            cache[key] = result
            return result

        return wrapper

    if _func is None:
        return decorator
    else:
        return decorator(_func)






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
                    log.debug(f"Attempt {retries}/{max_retries} failed. Retrying in {delay:.2f} seconds. Error: {str(e)}")
                    time.sleep(delay)

        return wrapper

    return decorator



class DictContext(UserDict):
    def __init__(self, data, *args, **kwargs):
        self.data = data

    def __enter__(self):
        return self.data

    def __exit__(self, exc_type, exc_value, traceback):
        pass  # Nothing happens on close
