from . import *
from concurrent.futures import ProcessPoolExecutor, as_completed, wait, FIRST_COMPLETED
import signal
from types import MethodType


def init_worker():
    signal.signal(signal.SIGINT, signal.SIG_IGN)


def pmap(
    func,
    objects=[],
    options=[],
    num_proc=1,
    total=None,
    desc=None,
    progress=True,
    ordered=True,
    stash=None,
    *common_args,
    **common_kwargs,
):
    from ..engines.base import HashStash
    from ..serializers.serializer import serialize, deserialize

    if not objects and not options:
        raise ValueError("At least one of objects or options must be non-empty")

    if (
        not isinstance(objects, (tuple, list))
        and isinstance(options, (tuple, list))
        and options
    ):
        objects = [objects] * len(options)

    objects = [list(x) if isinstance(x, (tuple, list)) else [x] for x in objects]
    if not objects:
        objects = [[]] * len(options)
    elif not options:
        options = [{}] * len(objects)

    if len(objects) != len(options):
        raise ValueError("objects and options must have the same length")

    if common_args:
        objects = [obj + list(common_args) for obj in objects]
    if common_kwargs:
        options = [{**common_kwargs, **opt} for opt in options]

    if progress:
        if total is None:
            total = len(objects)

    # Multi-process: continue with the existing logic
    # Check if func is a method and get the instance (self)
    if isinstance(func, MethodType):
        instance = func.__self__
        func = func.__func__
        objects = [(instance,) + tuple(obj) for obj in objects]


    # Serialize the function

    # Create a HashStash instance for caching
    if stash is not None:
        func_stash = stash.sub_function_results(func, dbname='pmap_result')
        func.stash = func_stash
    else:
        func_stash = None

    items = [(serialize(func) if num_proc>1 else func, obj, opt) for obj, opt in zip(objects, options)]

    desc = "" if not desc else desc + " "
    desc += f"[{num_proc}x]"

    iterator = range(len(items)) if total is None else range(total)

    def yield_results():
        if num_proc == 1:
            for _ in progress_bar(iterator, desc=desc, progress=progress):
                yield _pmap_item(items[_], func_stash)
            return
        else:
            with ProcessPoolExecutor(max_workers=num_proc, initializer=init_worker) as executor:
                futures = [executor.submit(_pmap_item, item, func_stash) for item in items]
                try:
                    for _ in progress_bar(iterator, desc=desc, progress=progress):
                        if ordered:
                            yield futures[_].result()
                        else:
                            done, _ = wait(futures, return_when=FIRST_COMPLETED)
                            for future in done:
                                yield future.result()
                                futures.remove(future)
                except KeyboardInterrupt as e:
                    log.error(f"Caught {e}, terminating workers")
                    executor.shutdown(wait=False, cancel_futures=True)
                    raise
                except Exception as e:
                    log.error(f"Caught {e}, terminating workers")
                    executor.shutdown(wait=False, cancel_futures=True)
                    raise
                finally:
                    for future in futures:
                        future.cancel()
    
    return iter(yield_results())


def _pmap_item(item, stash=None):
    from ..serializers.serializer import serialize, deserialize

    serialized_func, args, kwargs = item
    func = deserialize(serialized_func) if isinstance(serialized_func, (str, bytes)) else serialized_func

    if stash is not None:
        # Create a unique key for the function call
        # key = (args, kwargs)
        key = {"args": tuple(args), "kwargs": kwargs}

        # Check if the result is already stashed
        cached_result = stash.get(key)
        print('got cached result',cached_result)

    # If not cached or no cache is used, execute the function
    try:
        result = func(*args, **kwargs)
    except Exception as e:
        log.error(f"Caught {e}!")
        raise e

    if stash is not None:
        # Cache the result
        stash.set(key, result)

    return result


def progress_bar(iterr, progress=True, **kwargs):
    global current_depth

    if not progress:
        yield from iterr
    else:
        try:
            from tqdm import tqdm

            current_depth += 1

            class ColoredTqdm(tqdm):
                def __init__(self, *args, desc=None, **kwargs):
                    self.green = "\033[32m"
                    self.reset = "\033[32m"
                    desc = f"{self.green}{log_prefix_str(desc,reset=True)}{self.reset}"
                    super().__init__(*args, desc=desc, **kwargs)

            yield from ColoredTqdm(iterr, leave=True, **kwargs)
        except ImportError:
            yield from progress_bar(iterr, progress=False, **kwargs)
        finally:
            current_depth -= 1




pmap.progress_bar = progress_bar
