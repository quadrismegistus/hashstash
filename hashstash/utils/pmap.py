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
    global executors
    pid = os.getpid()
    with executor_lock:
        if pid not in executors:
            executors[pid] = ProcessPoolExecutor(max_workers=num_proc)
        return executors[pid]

def shutdown_global_executors():
    global executors
    with executor_lock:
        for executor in executors.values():
            executor.shutdown()
        executors.clear()

atexit.register(shutdown_global_executors)


class StashMap(UserList):
    def __init__(
        self,
        func,
        objects=None,
        options=None,
        total=None,
        num_proc=1,
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

        self.progress_bar = None
        if self.progress:
            from .misc import progress_bar

            self.progress_bar = progress_bar(total=self.total, desc=self.desc)

        self._executor = get_global_executor(num_proc)
        self._executor_lock = mp.Lock() if num_proc > 1 else None

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

    def compute(self):
        for res in self:
            res.compute()

    def __iter__(self):
        for res in self._results:
            yield res
            if not res._computed:
                res.compute()

    @property
    def data(self):
        return list(self)

    @cached_property
    def results(self):
        self.compute()
        resl = [res.result for res in self]
        if self.stash_map and type(self) is StashMap and self.stash is not None:
            self.stash.set(self.stash_key, self)
        if self.progress_bar:
            self.progress_bar.close()
        return resl
    
    def iter_results(self):
        self.compute()
        yield from (res.result for res in self)


    def __del__(self):
        # Do not shutdown the global executor here
        if self.progress_bar:
            self.progress_bar.close()

    def __getitem__(self, key):
        if isinstance(key, slice):
            return StashMapSlice(self, key)
        else:
            return self._get_single_item(key)

    def _get_single_item(self, index):
        if index < 0:
            index += len(self)
        if index < 0 or index >= len(self):
            raise IndexError("StashMap index out of range")

        for i, item in enumerate(self):
            if i == index:
                return item

        raise IndexError("StashMap index out of range")

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
            preload=data["preload"],
            precompute=data["precompute"],
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

    def __iter__(self):
        for i in range(self.start, self.stop, self.step):
            yield self.pmap._get_single_item(i)

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

    def __del__(self):
        self.pmap._executor.shutdown(wait=False)
        self.pmap._executor = None

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


def stash_mapped(_func=None, stash=None, **top_kwargs):
    if stash is None:
        from ..engines.base import HashStash
        stash = HashStash()

    def decorator(func):
        # stash.attach_func(func)

        @wraps(func)
        def wrapper(*args, **kwargs):
            results = stash.map(func, *args, **{**top_kwargs, **kwargs}).results
            if len(results)==1:
                return results[0]
            return results

        return wrapper

    if _func is None:
        return decorator
    else:
        return decorator(_func)

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
        self._future = None
        self._direct_result = None  # New attribute to store direct results
        self._computed = False
        self._processing_started = False
        self._preload = _preload
        self._preloaded = False
        self._preloading_started = False
        self._precompute = _precompute
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
            result = future_or_result.result()
        else:
            result = future_or_result
        if result is not None:
            self._set_computed(result)
        # if self._result is not None:
        #     self._computed = True
        # elif self._precompute:
        #     self.compute()

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
                    log.error(e)
            else:
                self._result = future_or_result
        if self._pmap_instance.progress_bar:
            self._pmap_instance.progress_bar.update(1)

    @cached_property
    def result(self):
        if self._result is not None:
            return self._result
        if not self._computed:
            if not self._processing_started:
                self._start_processing()
            if self._future:
                try:
                    self._result = self._future.result()
                except Exception as e:
                    log.error(e)
            else:
                self._result = self._direct_result
            self._computed = True
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
    for res in StashMap(func, *args, **kwargs):
        yield res.result


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

    unstuffed_item = unstuff(stuffed_item)  # if num_proc>1 else stuffed_item
    func, args, kwargs = (
        unstuffed_item["func"],
        unstuffed_item["args"],
        unstuffed_item["kwargs"],
    )
    stash = unstuffed_item["stash"]
    if stash is not None:
        return stash.get_func(*args, func=func,**kwargs)




def _cleanstr(x):
    x = str(x).replace("\n", " ")
    while "  " in x:
        x = x.replace("  ", " ")
    return x.strip()
