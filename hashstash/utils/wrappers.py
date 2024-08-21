from . import *




@log.debug
def stashed_result(
    _func=None,
    stash: Optional["BaseHashStash"] = None,
    name: str = None,
    dbname: str = None,
    force=False,
    update_on_src_change=False,
    store_args=True,
    **stash_kwargs,
):
    def decorator(func: Callable) -> Callable:
        nonlocal stash, update_on_src_change, force
        if stash is None:
            from ..engines.base import HashStash
            from ..utils.encodings import encode_hash
            from ..serializers import encode_hash
            stash = HashStash(name=name, dbname=dbname, **stash_kwargs)
        
        if get_obj_module(func) == '__main__':
            update_on_src_change = True
        
        stash = stash.sub_function_results(
            func,
            dbname = dbname,
            update_on_src_change=update_on_src_change,
            # *stash_args,
            **stash_kwargs,
        )

        func.stash = stash

        @wraps(func)
        def wrapper(*args, **kwargs):
            from ..serializers import serialize
            nonlocal force, stash, store_args
            wrapper.stash = stash

            local_force = kwargs.pop("_force", force)
            key = {"func": func, "args": tuple(args), "kwargs": kwargs}
            if not store_args:
                key = encode_hash(stash.serialize(key))

            # find it?
            if not local_force and key in stash:
                log.debug(f"Stash hit for {func.__name__}. Returning stashd result.")
                return stash[key]

            # didn't find
            note = "Forced execution" if local_force else "Stash miss"
            log.debug(f"{note} for {func.__name__}. Executing function.")

            # call func
            result = func(*args, **kwargs)

            # stash
            log.debug(f"Caching result for {func.__name__}.")
            stash[key] = result
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
                    log.debug(
                        f"Attempt {retries}/{max_retries} failed. Retrying in {delay:.2f} seconds. Error: {str(e)}"
                    )
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