from . import *
from concurrent.futures import ProcessPoolExecutor, as_completed, wait, FIRST_COMPLETED
import signal

def init_worker():
    signal.signal(signal.SIGINT, signal.SIG_IGN)

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
        objects = [obj + list(common_args) for obj in objects]
    if common_kwargs:
        options = [{**common_kwargs, **opt} for opt in options]

    if progress:
        if total is None:
            total = len(objects)

    
    items = [(func, obj, opt) for obj,opt in zip(objects,options)]

    desc = '' if not desc else desc+' '
    desc+=f'[{num_proc}x]'

    iterator = range(len(items)) if total is None else range(total)
    
    with ProcessPoolExecutor(max_workers=num_proc, initializer=init_worker) as executor:
        futures = [executor.submit(_pmap_item, item) for item in items]
        
        try:
            for _ in progress_bar(iterator, desc=desc, disable=not progress):
                if num_proc == 1:
                    # Single-process: use a simple map
                    yield _pmap_item(items[_])
                else:
                    # Multi-process: use ProcessPoolExecutor
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

def _pmap_item(item):
    func,args,kwargs = item
    return func(*args,**kwargs)



def progress_bar(iterr, progress=True,**kwargs):
    global current_depth

    if not progress:
        yield from iterr
    else:
        try:
            from tqdm import tqdm
            current_depth+=1
            
            class ColoredTqdm(tqdm):
                def __init__(self, *args, desc=None, **kwargs):
                    self.green = '\033[32m'
                    self.reset = '\033[32m'
                    desc = f"{self.green}{log_prefix_str(desc,reset=True)}{self.reset}"
                    super().__init__(*args, desc=desc, **kwargs)
                    

            yield from ColoredTqdm(iterr, position=0, leave=False, **kwargs)
        except ImportError:
            yield from progress_bar(iterr, progress=False, **kwargs)
        finally:
            current_depth-=1