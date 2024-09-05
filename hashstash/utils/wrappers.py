from . import *
from .misc import is_method

def attach_stash_to_function(func, stash = None, **stash_kwargs):
    from ..engines.base import HashStash
    if stash is None:
        stash = HashStash(**stash_kwargs)
    local_stash = stash.sub_function_results(func)
    func.stash = local_stash



def retry_patiently(max_retries=10, base_delay=0.01, max_delay=1):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            retries = 0
            while True:
                if retries:
                    log.info(f'retrying {func.__name__} for the {retries+1}th time')
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    log.debug(f"Caught {e}!!")
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

def get_dict(obj):
    return {
        k:getattr(obj,k)
        for k in dir(obj)
        if k and k[0]=='_'
        and not isinstance(getattr(obj,k), dict)
    }

@log.debug
def stashed_result(
    _func=None,
    stash: Optional["BaseHashStash"] = None,
    force=False,
    store_args=True,
    **stash_kwargs,
):
    @log.debug
    def decorator(func: Callable, *decorator_args, **decorator_kwargs) -> Callable:
        from ..engines.base import HashStash
        from .misc import ReusableGenerator
        from .addrs import is_method, get_object_from_method
        nonlocal stash, stash_kwargs

        if stash is None:
            stash = HashStash(**stash_kwargs)

        stash.attach_func(func)
        @log.debug
        @wraps(func)
        def wrapper(*args, **kwargs):
            from .misc import ReusableGenerator 
            nonlocal force, stash, store_args, func, stash_kwargs, decorator_kwargs
            if args and get_pytype(args[0]) in {'class','instance'}:
                self_obj = args[0]
                func = getattr(self_obj, func.__name__)
                args = args[1:]

            return stash.run(func, *args, **kwargs)
            
        return wrapper

    if _func is None:
        return decorator
    else:
        return decorator(_func)





# @log.debug
# def parallelized(
#     _func=None,
#     num_proc=None,
#     progress=True,
#     desc=None,
#     ordered=True,
#     stash=None,
#     stashed=False,
#     update_on_src_change=False,
#     *pmap_args,
#     **pmap_kwargs,
# ):
#     from .pmap import pmap

#     def decorator(func):
#         nonlocal stash
#         if stash is None and stashed:
#             from ..engines.base import HashStash
#             stash = HashStash()

#         if stashed:
#             if get_obj_module(func) == "__main__":
#                 update_on_src_change = True
#             func_stash = stash.sub_function_results(
#                 func, dbname="pmap_result", update_on_src_change=update_on_src_change
#             )
#             func.stash = func_stash
#         else:
#             func_stash = None

#         @wraps(func)
#         def wrapper(*args, **kwargs):
#             nonlocal func_stash

#             # Check if we're dealing with multiple sets of arguments
#             if args and isinstance(args[0], (list, tuple)):
#                 objects = args[0]
#                 if kwargs:
#                     options = [kwargs] * len(objects)
#                 else:
#                     options = [{} for _ in objects]

#                 # Use pmap to execute the function(s)
#                 results = pmap(
#                     func,
#                     objects=objects,
#                     options=options,
#                     num_proc=num_proc or os.cpu_count(),
#                     progress=progress,
#                     desc=desc or func.__name__,
#                     ordered=ordered,
#                     stash=func_stash,
#                     *pmap_args,
#                     **pmap_kwargs,
#                 )
#                 return list(results)  # Convert generator to list
#             else:
#                 # Single function call
#                 if func_stash is not None:
#                     res = func_stash.get(
#                         args, kwargs
#                     )  # Changed from func_stash.get(*args, **kwargs)
#                 else:
#                     res = None
#                 return func(*args, **kwargs) if res is None else res

#         return wrapper

#     if _func is None:
#         return decorator
#     else:
#         return decorator(_func)




stashed_dataframe = partial(stashed_result, engine='dataframe', _as_dataframe=True, _with_metadata=True, _all_results=True)
