# Explicit stdlib imports: this package's `from . import *` chains are
# circular, and whether a name has landed in the package namespace yet
# depends on import order (spawn workers + editable installs order imports
# differently). Never rely on the star-chain for stdlib names.
from functools import wraps
import multiprocessing as mp
import threading

from . import *
from collections import UserList
from threading import Thread, Event

from concurrent.futures import (
    ProcessPoolExecutor,
    as_completed,
    wait,
    FIRST_COMPLETED,
    ThreadPoolExecutor,
)
import signal
from types import MethodType
from functools import cached_property
from concurrent.futures import Future
from queue import Queue, Empty
from .misc import is_stash
import atexit
from contextlib import contextmanager
import os

# Global executors and lock
executors = {}
executor_lock = threading.Lock()

def get_global_executor(num_proc):
    # keyed by (pid, num_proc): a later num_proc used to silently reuse whatever
    # pool was created first; broken pools (worker OOM/segfault) are replaced
    # instead of poisoning every future pmap in the process
    global executors
    key = (os.getpid(), num_proc)
    with executor_lock:
        executor = executors.get(key)
        # A worker raising a NORMAL exception does not set _broken, but the
        # spawn pool can still be left in a state where a LATER pmap deadlocks
        # (observed as a flaky macOS-CI hang in test_pmap_pool_usable_after_
        # worker_exception). So we also replace a pool we tainted after a worker
        # error. Replacement happens here (between pmaps), never mid-pmap, so the
        # tainting pmap's remaining items still complete on the old pool.
        if (
            executor is None
            or getattr(executor, "_broken", False)
            or getattr(executor, "_hs_tainted", False)
        ):
            old = executor
            # explicit spawn context: on Linux <= 3.13 the default is fork, and
            # forking a worker while another thread (future callbacks, logging)
            # holds a lock deadlocks the child — pmap is inherently multi-threaded.
            # Workers rehydrate functions via stuff/unstuff, so nothing relies on
            # fork's memory inheritance.
            executor = executors[key] = ProcessPoolExecutor(
                max_workers=num_proc, mp_context=mp.get_context("spawn")
            )
            if old is not None:
                # don't wait: any still-in-flight futures on the old pool finish,
                # then its workers exit; we just stop handing it out.
                old.shutdown(wait=False)
        return executor


def taint_global_executor(num_proc):
    """Mark the cached pool for (pid, num_proc) so the NEXT get_global_executor
    replaces it. Called when a worker future raises — a normal task exception
    leaves _broken unset, but the pool may still deadlock a later pmap on spawn."""
    global executors
    key = (os.getpid(), num_proc)
    with executor_lock:
        executor = executors.get(key)
        if executor is not None:
            executor._hs_tainted = True

def shutdown_global_executors():
    global executors
    with executor_lock:
        for executor in executors.values():
            executor.shutdown()
        executors.clear()

atexit.register(shutdown_global_executors)

def get_num_proc(n=None, num_spare=2):
    num_avail = mp.cpu_count()
    return n if n and 1<=n<=num_avail else (num_avail-num_spare) if (num_avail-num_spare)>0 else 1

class StashMap(UserList):
    def __init__(
        self,
        func,
        objects=None,
        options=None,
        total=None,
        num_proc=None,
        desc=None,
        progress=True,
        ordered=True,
        stash=None,
        preload=True,
        precompute=True,
        _results=None,
        _stash_key=None,
        stash_runs=True,
        stash_map=True,
        _force=False,
        **common_kwargs,
    ):

        self.func = func
        self._objects = objects
        self._options = options
        self._total = total
        self.objects, self.options = self.process_input(
            objects, options, total=total, **common_kwargs
        )
        
        self.common_kwargs = common_kwargs
        self.total = len(self.objects)
        num_proc=get_num_proc(num_proc)
        self.num_proc = num_proc
        self.desc = (
            desc
            if desc is not None
            else f"Mapping {get_obj_addr(func)} across {self.total} objects"
        ) + (f" [{num_proc}x]" if num_proc > 1 else "")
        self.progress = progress
        self.ordered = ordered
        self.stash = stash
        self._stash_key = _stash_key
        self._preload = preload
        self._precompute = precompute
        self.stash_runs = stash_runs
        self.stash_map = stash_map
        self._force = _force
        self._needed_computing = None
        self._stashed = False
        self.progress_bar = None
        if self.progress:
            from .misc import progress_bar
            self.progress_bar = progress_bar(total=self.total, desc=self.desc)

        self._executor = get_global_executor(num_proc)
        # only threads in THIS process contend on it: a multiprocessing.Lock here
        # was pointless overhead and another fork-inheritance hazard
        self._executor_lock = threading.Lock() if num_proc > 1 else None

        if _results is None:
            self._results = [
                StashMapRun(
                    self.func,
                    self.objects[i],
                    self.options[i],
                    self,
                    _preload=preload,
                    _precompute=precompute,
                )
                for i in range(self.total)
            ]
        else:
            self._results = [
                StashMapRun.from_dict(
                    {
                        "func": self.func,
                        "_pmap_instance": self,
                        "_preload": preload,
                        "_precompute": precompute,
                        **res,
                    }
                )
                for res in _results
            ]

    @staticmethod
    def process_input(objects=None, options=None, total=None, **common_kwargs):
        if not objects and not options:
            raise ValueError("At least one of objects or options must be non-empty")
        if is_generator(objects):
            objects = list(objects)
        if is_generator(options):
            options = list(options)

        if isinstance(objects,list) and isinstance(options,list) and objects and options and len(objects)!=len(options):
            raise ValueError("objects and options must have the same length")

        if not total:
            if isinstance(objects,list) and objects:
                total = len(objects)
            elif isinstance(options,list) and options:
                total = len(options)
            else:
                total = 1

        if not isinstance(objects, list) or not len(objects):
            objects = [objects if objects is not None and objects != [] else ()] * total
        if not isinstance(options, list) or not len(options):
            options = [options if options is not None and options != [] else {}] * total
        
        objects = objects[:total]
        options = options[:total]

        objects = [tuple(x) if isinstance(x, (tuple, list)) else (x,) for x in objects]
        if common_kwargs:
            options = [{**common_kwargs, **opt} for opt in options]

        return objects, options

    @property
    def stash_key(self):
        return self._stash_key or self.get_stash_key(
            self.func, self._objects, self._options, self._total, **self.common_kwargs
        )

    @classmethod
    @log.debug
    def get_stash_key(cls, func, objects=None, options=None, total=None, **common_kwargs):
        return {"func": func, "objects": objects, "options": options, "total":total, **common_kwargs}

    @property
    def executor(self):
        return self._executor

    @property
    def finished(self):
        return self.num_done == self.total

    @property
    def num_done(self):
        return len([res for res in self._results if res._computed])

    def __len__(self):
        return self.total

    def _iter_runs(self):
        """Yield the underlying StashMapRun wrapper objects. Subclasses (e.g.
        StashMapSlice) override this to select a subset."""
        return iter(self._results)

    @property
    def runs(self):
        """The StashMapRun wrapper objects — args/kwargs, per-item cache status,
        lazy `.result`. Iterating or indexing the StashMap itself yields the
        computed *values* (like builtin `map` / `pmap`); use `.runs` when you
        want the wrappers."""
        return list(self._iter_runs())

    def compute(self):
        for res in self._iter_runs():
            res.compute()
            if res._needed_computing:
                self._needed_computing = True

    def __iter__(self):
        # yield the computed VALUES (like builtin map / pmap); `.runs` gives the
        # StashMapRun wrappers
        return self.results_iter()

    @property
    def data(self):
        # UserList backing store holds the run wrappers, so len()/repr stay lazy
        # and never force computation
        return list(self._iter_runs())

    @cached_property
    def results(self):
        return list(self.results_iter())

    def items(self):
        for res in self._iter_runs():
            yield (res.args, res.kwargs), res.result

    def keys(self):
        yield from (k for k,v in self.items())
    def values(self):
        yield from (v for k,v in self.items())

    def items_l(self):
        return list(self.items())
    def values_l(self):
        return list(self.values())
    def keys_l(self):
        return list(self.keys())

    def results_iter(self):
        self.compute()
        for res in self._iter_runs():
            yield res.result
            if res._needed_computing:
                self._needed_computing = True
        if self.progress_bar:
            self.progress_bar.close()
        # stash the whole map once, and only for a top-level StashMap (not slices)
        if (type(self) is StashMap and not self._stashed and self._needed_computing
                and self.stash_map and self.stash is not None):
            self._stashed = True
            log.info(f"Saving {self.total} results to stash")
            self.stash.set(self.stash_key, self)
            log.info(f"Saved {self.total} results to stash")
        


    def preload(self):
        for res in self._results:
            res.preload()

    def __del__(self):
        # Do not shutdown the global executor here; guard the attribute since
        # __del__ can run on instances whose __init__ raised early
        progress_bar = getattr(self, "progress_bar", None)
        if progress_bar:
            progress_bar.close()

    def __getitem__(self, key):
        if isinstance(key, slice):
            return StashMapSlice(self, key)
        else:
            return self._get_single_item(key)

    def _get_single_item(self, index):
        # returns the computed VALUE; use `.runs[index]` for the StashMapRun
        if index < 0:
            index += self.total
        if index < 0 or index >= self.total:
            raise IndexError("StashMap index out of range")
        res = self._results[index]
        if not res._computed:
            res.compute()
            if res._needed_computing:
                self._needed_computing = True
        return res.result

    def to_dict(self):
        results = [
            {
                k: v
                for k, v in res.to_dict().items()
                if k not in {"func", "_pmap_instance"}
            }
            for res in self._results
        ]
        return {
            "func": self.func,
            "objects": self.objects,
            "options": self.options,
            "num_proc": self.num_proc,
            "total": self.total,
            "desc": self.desc,
            "progress": self.progress,
            "ordered": self.ordered,
            "stash": self.stash,
            "preload": self._preload,
            "precompute": self._precompute,
            "_results": results,
        }

    @classmethod
    def from_dict(cls, data):
        # preload/precompute forced off: deserializing a stored map must not spawn
        # a process pool and submit lookups as a side effect of stash.get().
        # stash.map() re-preloads explicitly when the caller asks for it.
        pmap = cls(
            data["func"],
            objects=data["objects"],
            options=data["options"],
            num_proc=data["num_proc"],
            total=data["total"],
            desc=data["desc"],
            progress=data["progress"],
            ordered=data["ordered"],
            stash=data["stash"],
            preload=False,
            precompute=False,
            _results=data["_results"],
        )
        return pmap

    def __reduce__(self):
        return (self.__class__.from_dict, (self.to_dict(),))

    def _execute_task(self, stuffed_item):
        if self.num_proc > 1:
            return self.executor.submit(_pmap_item, stuffed_item)
        else:
            return _pmap_item(stuffed_item)


class StashMapSlice(StashMap):
    def __init__(self, pmap, slice_obj):
        self.pmap = pmap
        self.start, self.stop, self.step = slice_obj.indices(len(pmap))
        self.total = len(range(self.start, self.stop, self.step))
        # attrs the inherited compute()/results_iter() expect; a slice never
        # owns a progress bar and never re-stashes the parent map
        self.progress_bar = None
        self.stash = getattr(pmap, "stash", None)
        self.stash_map = False
        self._needed_computing = None
        self._stashed = True

    def _iter_runs(self):
        return (self.pmap._results[i] for i in range(self.start, self.stop, self.step))

    def __len__(self):
        return max(0, (self.stop - self.start + self.step - 1) // self.step)

    def __getitem__(self, key):
        if isinstance(key, slice):
            new_start = self.start + key.start * self.step
            new_stop = min(self.stop, self.start + key.stop * self.step)
            new_step = self.step * key.step
            return StashMapSlice(self.pmap, slice(new_start, new_stop, new_step))
        else:
            index = self.start + key * self.step
            if index < self.start or index >= self.stop:
                raise IndexError("StashMapSlice index out of range")
            return self.pmap._get_single_item(index)

    def to_dict(self):
        return {
            "pmap": self.pmap,
            "start": self.start,
            "stop": self.stop,
            "step": self.step,
            "total": self.total,
        }

    @classmethod
    def from_dict(cls, data):
        slice_obj = slice(data["start"], data["stop"], data["step"])
        result = cls(data["pmap"], slice_obj)
        result.total = data["total"]
        return result

    def __reduce__(self):
        return (self.from_dict, (self.to_dict(),))


def stash_mapped(_func=None, *stash_args, stash=None, _force=False, **stash_kwargs):
    if stash is None:
        from ..engines.base import HashStash
        stash = HashStash(*stash_args, **stash_kwargs)

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            return stash.map(func, *args, _force=_force, **{**stash_kwargs, **kwargs})
        return wrapper

    # Check if _func is a string (root_dir) or a function
    if isinstance(_func, str):
        stash_kwargs['root_dir'] = _func
        return decorator
    elif callable(_func):
        return decorator(_func)
    else:
        return decorator

parallelized = stash_mapped

parallelized = stash_mapped



class StashMapRun:
    def __init__(
        self,
        func,
        args,
        kwargs,
        _pmap_instance,
        _preload=True,
        _precompute=True,
        _result=None,
    ):
        self.func = func
        self.args = args
        self.kwargs = kwargs
        self._pmap_instance = _pmap_instance
        self._result = _result
        self._error = None
        self._future = None
        self._direct_result = None  # New attribute to store direct results
        self._computed = False
        self._processing_started = False
        self._preload = _preload
        self._preloaded = False
        self._preloading_started = False
        self._precompute = _precompute
        self._needed_computing = None
        if self._preload:
            self.preload()
        elif self._precompute:
            self.compute()

    @property
    def stash(self):
        return self._pmap_instance.stash

    @cached_property
    def stash_key(self):
        if self.stash is not None:
            return self.stash.new_function_key(*self.args, **self.kwargs)

    def stuff(self):
        from ..serializers import stuff

        return stuff(
            {
                "func": self.func,
                "args": self.args,
                "kwargs": self.kwargs,
                "stash": (
                    self._pmap_instance.stash
                    if self._pmap_instance.stash_runs
                    else None
                ),
                "_force": self._pmap_instance._force,
            }
        )

    def preload(self):
        if (
            not self._preloaded
            and not self._preloading_started
            and self._result is None
        ):
            self._start_preloading()

    def _start_preloading(self):
        stuffed_item = self.stuff()
        if self._pmap_instance.num_proc > 1:
            with self._pmap_instance._executor_lock:
                self._future = self._pmap_instance.executor.submit(
                    _pmap_lookup_item, stuffed_item
                )
                self._future.add_done_callback(self._set_preloaded)
        else:
            result = _pmap_lookup_item(stuffed_item)
            self._set_preloaded(result)
        self._preloading_started = True

    def _set_preloaded(self, future_or_result):
        self._preloaded = True
        if isinstance(future_or_result, Future):
            try:
                payload = future_or_result.result()
            except Exception as e:
                log.debug(f"preload lookup failed: {e}")
                payload = ("miss", None)
        else:
            payload = future_or_result
        # _pmap_lookup_item returns ('hit', value) / ('miss', None): a bare
        # None used to be ambiguous, so cached None results were recomputed
        if (
            isinstance(payload, tuple)
            and len(payload) == 2
            and payload[0] in ("hit", "miss")
        ):
            status, value = payload
        else:
            status, value = ("hit", payload)
        if status == "hit":
            self._needed_computing = False
            self._set_computed(value)
        else:
            self._needed_computing = True
            self.compute()

    def _start_processing(self):
        if self._result is not None:
            self._processing_started = True
            return
        stuffed_item = self.stuff()
        if self._pmap_instance.num_proc > 1:
            with self._pmap_instance._executor_lock:
                self._future = self._pmap_instance._execute_task(stuffed_item)
                self._future.add_done_callback(self._set_computed)
        else:
            self._direct_result = self._pmap_instance._execute_task(stuffed_item)
            self._set_computed(self._direct_result)
        self._processing_started = True

    def _set_computed(self, future_or_result):
        self._computed = True
        if self._result is None:
            if isinstance(future_or_result, Future):
                try:
                    self._result = future_or_result.result()
                except Exception as e:
                    # raising here would be swallowed by add_done_callback:
                    # remember the failure and re-raise when .result is read
                    self._error = e
                    # taint the shared pool so the next pmap gets a fresh one —
                    # a worker exception can otherwise leave the spawn pool in a
                    # state that deadlocks a later call
                    if self._pmap_instance.num_proc > 1:
                        taint_global_executor(self._pmap_instance.num_proc)
            else:
                self._result = future_or_result
        if self._pmap_instance.progress_bar:
            self._pmap_instance.progress_bar.update(1)

    @cached_property
    def result(self):
        # worker exceptions propagate to the caller — they used to be logged and
        # silently converted into a None result, indistinguishable from a real None
        if self._error is not None:
            raise self._error
        if self._result is not None:
            return self._result
        if not self._computed:
            if not self._processing_started:
                self._start_processing()
            if self._future:
                self._result = self._future.result()
            else:
                self._result = self._direct_result
            self._computed = True
        if self._error is not None:
            raise self._error
        return self._result

    def compute(self):
        if self._result is None and not self._processing_started and not self._computed:
            self._start_processing()

    def to_dict(self):
        return {
            "func": self.func,
            "args": self.args,
            "kwargs": self.kwargs,
            "_pmap_instance": self._pmap_instance,
            "_result": self._result,
        }

    @classmethod
    def from_dict(cls, data):
        return cls(
            data["func"],
            data["args"],
            data["kwargs"],
            data["_pmap_instance"],
            _preload=data.get("_preload", True),
            _result=data.get("_result", None),
        )

    def __reduce__(self):
        return (self.from_dict, (self.to_dict(),))

    def __repr__(self):
        funcstr = f"{get_function_call_str(self.func,*self.args,**self.kwargs)}"
        funcstr += f" >>> {_cleanstr(self._result)[:50].strip() if self._result is not None else '?'}"
        return f"StashMapRun({funcstr})"






def init_worker():
    signal.signal(signal.SIGINT, signal.SIG_IGN)


def pmap(func, *args, **kwargs):
    # StashMap now iterates values directly (use .runs for the wrappers).
    # pmap caches per-item results (stash_runs) but not the map object itself.
    kwargs.setdefault("stash_map", False)
    yield from StashMap(func, *args, **kwargs)


def pmap_l(*x, **y):
    return list(pmap(*x, **y))


def _pmap_item(stuffed_item):
    from ..serializers import unstuff

    unstuffed_item = unstuff(stuffed_item)  # if num_proc>1 else stuffed_item
    func, args, kwargs = (
        unstuffed_item["func"],
        unstuffed_item["args"],
        unstuffed_item["kwargs"],
    )
    stash = unstuffed_item.get("stash")
    _force = unstuffed_item.get("_force")
    if stash is not None:
        return stash.run(func, *args, **kwargs, _force=_force)
    else:
        return func(*args, **kwargs)


def _pmap_lookup_item(stuffed_item):
    from ..serializers import unstuff
    from ..engines.base import _MISSING

    unstuffed_item = unstuff(stuffed_item)  # if num_proc>1 else stuffed_item
    func, args, kwargs = (
        unstuffed_item["func"],
        unstuffed_item["args"],
        unstuffed_item["kwargs"],
    )
    stash = unstuffed_item["stash"]
    if stash is not None:
        result = stash.get_func(*args, func=func, default=_MISSING, **kwargs)
        # a status tuple, not a bare value: sentinel identity does not survive
        # pickling across the process boundary, and a cached None is a hit
        if result is not _MISSING:
            return ("hit", result)
    return ("miss", None)




def _cleanstr(x):
    x = str(x).replace("\n", " ")
    while "  " in x:
        x = x.replace("  ", " ")
    return x.strip()
