from .utils import *


def pmap(func, objects=[], options=[], num_proc=1, chunksize=1, total=None, desc=None, progress=True, ordered=True, *common_args, **common_kwargs):
    if not objects and not options:
        raise ValueError('At least one of objects or options must be non-empty')
    
    if not isinstance(objects, (tuple,list)) and isinstance(options, (tuple,list)) and options:
        objects = [objects] * len(options)

    objects = [
        list(x) if isinstance(x, (tuple,list)) else [x] for x in objects
    ]
    if not objects:
        objects = [[]] * len(options)
    elif not options:
        options = [{}] * len(objects)
    
    if len(objects) != len(options):
        raise ValueError('objects and options must have the same length')
    
    if common_args:
        objects = [obj+common_args for obj in objects]
    if common_kwargs:
        objects = [{**common_args, **obj} for obj in options]

    if progress:
        from tqdm import tqdm
        if total is None:
            total = len(objects)

    
    items = [(func, obj, opt) for obj,opt in zip(objects,options)]

    with tqdm(total=total, desc=desc) if progress else nullcontext() as pbar:
        if num_proc == 1:
            # Single-process: use a simple map
            for result in map(_pmap_item, items):
                yield result
                if progress:
                    pbar.update(1)
        else:
            # Multi-process: use ProcessPoolExecutor
            with ProcessPoolExecutor(max_workers=num_proc) as executor:
                if ordered:
                    for result in imap(executor, _pmap_item, items, chunksize=chunksize):
                        yield result
                        if progress:
                            pbar.update(1)
                else:
                    futures = [executor.submit(_pmap_item, item) for item in items]
                    for future in as_completed(futures):
                        yield future.result()
                        if progress:
                            pbar.update(1)

def _pmap_item(item):
    func,args,kwargs = item
    return func(*args,**kwargs)


def imap(executor, func, iterable, chunksize=1):
    """
    A generator that mimics the behavior of multiprocessing.Pool.imap()
    using ProcessPoolExecutor, yielding results in order as they become available.
    """
    # Create futures for each item
    futures = {}
    for i, item in enumerate(iterable):
        future = executor.submit(func, item)
        futures[future] = i

    # Yield results in order
    next_to_yield = 0
    results = {}
    for future in as_completed(futures):
        index = futures[future]
        try:
            result = future.result()
        except Exception as e:
            result = e  # or handle the exception as needed

        if index == next_to_yield:
            yield result
            next_to_yield += 1

            # Yield any subsequent results that are ready
            while next_to_yield in results:
                yield results.pop(next_to_yield)
                next_to_yield += 1
        else:
            results[index] = result


def f(x):
    print(x)
    if not x % 2:
        time.sleep(10)