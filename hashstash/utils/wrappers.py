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
    @log.info
    def decorator(func: Callable) -> Callable:
        nonlocal stash, update_on_src_change, force
        if stash is None:
            from ..engines.base import HashStash
            from ..utils.encodings import encode_hash
            from ..serializers import encode_hash

            stash = HashStash(name=name, dbname=dbname, **stash_kwargs)

        if get_obj_module(func) == "__main__":
            update_on_src_change = True

        stash = stash.sub_function_results(
            func,
            dbname=dbname,
            update_on_src_change=update_on_src_change,
            # *stash_args,
            **stash_kwargs,
        )

        func.stash = stash

        @log.info
        @wraps(func)
        def wrapper(*args, **kwargs):
            from ..serializers import serialize
            from .misc import ReusableGenerator

            nonlocal force, stash, store_args
            wrapper.stash = stash

            local_force = kwargs.pop("_force", force)
            key = {
                # "func": get_obj_addr(func),
                "args": tuple(args),
                "kwargs": kwargs,
            }
            if not store_args:
                key = encode_hash(stash.serialize(key))


            # find it?
            if not local_force:
                res = stash.get(key,as_function=False)
                if res is not None:
                    log.debug(f"Stash hit for {func.__name__}. Returning stashed result.")
                    return res

            # didn't find
            note = "Forced execution" if local_force else "Stash miss"
            log.debug(f"{note} for {func.__name__}. Executing function.")

            # call func
            result = func(*args, **kwargs)
            is_generator = inspect.isgenerator(result) or isinstance(result,ReusableGenerator)
            result = list(result) if is_generator else result
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

@log.info
def parallelized(
    _func=None,
    num_proc=None,
    progress=True,
    desc=None,
    ordered=True,
    stash=None,
    stashed=False,
    update_on_src_change=False,
    *pmap_args,
    **pmap_kwargs,
):
    from .pmap import pmap

    def decorator(func):
        nonlocal stash
        if stash is None and stashed:
            from ..engines.base import HashStash
            stash = HashStash()
        
        if stashed:
            if get_obj_module(func) == "__main__":
                update_on_src_change = True
            func_stash = stash.sub_function_results(func, dbname='pmap_result', update_on_src_change=update_on_src_change)
            func.stash = func_stash
        else:
            func_stash = None

        @wraps(func)
        def wrapper(*args, **kwargs):
            nonlocal func_stash

            # Check if we're dealing with multiple sets of arguments
            if args and isinstance(args[0], (list, tuple)):
                log.info('parallelizing')
                objects = args[0]
                if kwargs:
                    options = [kwargs] * len(objects)
                else:
                    options = [{} for _ in objects]

                # Use pmap to execute the function(s)
                results = pmap(
                    func,
                    objects=objects,
                    options=options,
                    num_proc=num_proc or os.cpu_count(),
                    progress=progress,
                    desc=desc or func.__name__,
                    ordered=ordered,
                    stash=func_stash,
                    *pmap_args,
                    **pmap_kwargs,
                )
                return list(results)  # Convert generator to list
            else:
                # Single function call
                if func_stash is not None:
                    log.info('getting from stash')
                    res = func_stash.get(args, kwargs)  # Changed from func_stash.get(*args, **kwargs)
                else:
                    res = None
                return func(*args, **kwargs) if res is None else res

        return wrapper

    if _func is None:
        return decorator
    else:
        return decorator(_func)