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
    BrokenExecutor,
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
    # Default to SERIAL (num_proc=1). Parallel maps use a spawn process pool,
    # which re-imports __main__ in each worker — so an unguarded top-level
    # stash.map() in a script would crash without `if __name__ == "__main__":`.
    # Serial-by-default needs no guard and works everywhere (script/notebook/
    # REPL); opt into parallelism explicitly with num_proc=N.
    num_avail = mp.cpu_count()
    if not n or n < 1:
        return 1
    if n > num_avail:
        return max(1, num_avail - num_spare)  # clamp an oversized request
    return n


def _ultradict_available():
    """True if UltraDict is importable (the 'memory' engine uses it for
    cross-process shared memory). Checked without importing/instantiating it."""
    try:
        import importlib.util
        return importlib.util.find_spec("UltraDict") is not None
    except Exception:
        return False


def _map_fallback_reason(func):
    """Read-only pre-flight check for spawn safety. Returns a short reason string
    if running ``func`` under a spawn worker pool is likely to fail cryptically
    (so the caller should fall back to num_proc=1), or None if parallel is fine.

    Only reads the function's addr/source; never spawns or mutates anything. The
    failures this guards against, all cryptic to an end user:
      * bare REPL / ``python -c`` / piped stdin -> BrokenProcessPool
      * unguarded module top-level        -> multiprocessing bootstrapping RuntimeError
      * source-unretrievable function      -> KeyError in recreate_function_from_src
    """
    import sys
    # A function importable from a real (non-__main__) module is rebuilt in the
    # worker by import, so parallelism is safe regardless of the calling context.
    try:
        if can_import_object(func):
            return None
    except Exception:
        pass
    # Otherwise the worker must rebuild it from source; empty source -> KeyError in
    # recreate_function_from_src (REPL/exec-defined functions).
    try:
        src = get_function_src(func) or ""
    except Exception:
        src = ""
    if not str(src).strip():
        return "function source unavailable"
    # Source is retrievable but the process itself is a spawn-unsafe bootstrap
    # context: a bare REPL, ``python -c ...``, or piped stdin re-imports __main__
    # in the child and dies (BrokenProcessPool / bootstrapping RuntimeError).
    main_mod = sys.modules.get("__main__")
    argv0 = sys.argv[0] if sys.argv else ""
    if getattr(main_mod, "__file__", None) is None or argv0 in ("-c", ""):
        return "interactive/unguarded __main__ context"
    return None


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
        num_proc = get_num_proc(num_proc)
        # Guard spawn-multiprocessing footguns BEFORE creating the pool. An
        # interactive/`-c`/stdin context, an unguarded module top-level, or a
        # function whose source can't be retrieved all make spawn workers fail
        # cryptically. Degrade to serial with ONE actionable warning instead;
        # num_proc=1 never serializes the function (see _run_item / _lookup_item),
        # so it works for any callable, including REPL/exec-defined ones.
        if num_proc > 1:
            reason = _map_fallback_reason(func)
            if reason:
                log.warning(
                    f"stash.map: falling back to num_proc=1 ({reason}); guard "
                    "module-level map() calls with if __name__=='__main__' for "
                    "real parallelism."
                )
                num_proc = 1
            elif (
                getattr(stash, "engine", None) == "memory"
                and not _ultradict_available()
            ):
                # process-local memory dict: worker results never reach the
                # parent, so incremental caching silently no-ops under num_proc>1
                log.warning(
                    "stash.map: engine='memory' without ultradict is process-local, "
                    "so results computed in num_proc>1 workers never reach the "
                    "parent and are not cached. Install ultradict, or use "
                    "num_proc=1 or a persistent engine."
                )
        self.num_proc = num_proc
        self._warned_spawn_fallback = False
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

    def _warn_spawn_fallback(self):
        """Emit ONE warning when a worker pool turns out to be unusable at
        runtime (e.g. an unguarded __main__ that only surfaces as a
        BrokenProcessPool once we submit) and we compute in-process instead."""
        if not getattr(self, "_warned_spawn_fallback", False):
            self._warned_spawn_fallback = True
            log.warning(
                "stash.map: worker pool unavailable (interactive/unguarded "
                "__main__ context); computing in-process. Guard module-level "
                "map() calls with if __name__=='__main__' for real parallelism."
            )

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

    def _item_dict(self):
        return {
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

    def stuff(self):
        from ..serializers import stuff

        return stuff(self._item_dict())

    def preload(self):
        if (
            not self._preloaded
            and not self._preloading_started
            and self._result is None
        ):
            self._start_preloading()

    def _start_preloading(self):
        if self._pmap_instance.num_proc > 1:
            try:
                with self._pmap_instance._executor_lock:
                    self._future = self._pmap_instance.executor.submit(
                        _pmap_lookup_item, self.stuff()
                    )
                    self._future.add_done_callback(self._set_preloaded)
            except BrokenExecutor:
                # pool died before it could accept the task (unguarded __main__):
                # degrade this item to an in-process cache lookup
                self._pmap_instance._warn_spawn_fallback()
                self._set_preloaded(_lookup_item(self._item_dict()))
        else:
            # num_proc=1: no serialize round-trip, so REPL/exec-defined functions
            # (whose source can't be recreated) still work
            self._set_preloaded(_lookup_item(self._item_dict()))
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
        if self._pmap_instance.num_proc > 1:
            try:
                with self._pmap_instance._executor_lock:
                    self._future = self._pmap_instance.executor.submit(
                        _pmap_item, self.stuff()
                    )
                    self._future.add_done_callback(self._set_computed)
            except BrokenExecutor:
                # pool died before accepting the task (unguarded __main__):
                # degrade this item to in-process computation
                self._pmap_instance._warn_spawn_fallback()
                self._set_computed(_run_item(self._item_dict()))
        else:
            # num_proc=1: run directly without a serialize round-trip
            self._direct_result = _run_item(self._item_dict())
            self._set_computed(self._direct_result)
        self._processing_started = True

    def _set_computed(self, future_or_result):
        self._computed = True
        if self._result is None:
            if isinstance(future_or_result, Future):
                try:
                    self._result = future_or_result.result()
                except BrokenExecutor:
                    # the pool broke (e.g. an unguarded __main__ that only fails
                    # once a worker starts): recompute this item in-process rather
                    # than surface a cryptic BrokenProcessPool to the caller
                    self._pmap_instance._warn_spawn_fallback()
                    try:
                        self._result = _run_item(self._item_dict())
                    except Exception as e:
                        self._error = e
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

    @property
    def was_cached(self):
        """True if this run's result came from the stash cache instead of being
        (re)computed in this process. Read-only. Forces computation if the run
        hasn't finished so the answer is meaningful even before the map is
        iterated; after iteration it just reflects the settled cache status."""
        # A result present before this instance ever looked it up or computed it
        # came pre-populated from a stashed/deserialized StashMap (the whole map
        # was a cache hit) — that's cached.
        if (
            self._result is not None
            and not self._preloading_started
            and not self._processing_started
        ):
            return True
        if not self._computed:
            # ensure the preload cache lookup / computation has resolved
            self.result
        # _needed_computing is set False only on a cache hit (see _set_preloaded);
        # a miss sets it True, so anything other than an explicit False was computed
        return self._needed_computing is False

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


def _run_item(item):
    """Compute one item from an already-deserialized item dict. Used directly on
    the num_proc=1 path (no serialize round-trip, so REPL/exec-defined functions
    whose source can't be recreated still work)."""
    func, args, kwargs = item["func"], item["args"], item["kwargs"]
    stash = item.get("stash")
    _force = item.get("_force")
    if stash is not None:
        return stash.run(func, *args, **kwargs, _force=_force)
    else:
        return func(*args, **kwargs)


def _lookup_item(item):
    """Cache-lookup one item from an already-deserialized item dict (num_proc=1
    path). Returns ('hit', value) / ('miss', None)."""
    from ..engines.base import _MISSING

    func, args, kwargs = item["func"], item["args"], item["kwargs"]
    stash = item.get("stash")
    if stash is not None:
        result = stash.get_func(*args, func=func, default=_MISSING, **kwargs)
        # a status tuple, not a bare value: sentinel identity does not survive
        # pickling across the process boundary, and a cached None is a hit
        if result is not _MISSING:
            return ("hit", result)
    return ("miss", None)


def _pmap_item(stuffed_item):
    # worker entrypoint (num_proc>1): deserialize then compute
    from ..serializers import unstuff

    return _run_item(unstuff(stuffed_item))


def _pmap_lookup_item(stuffed_item):
    # worker entrypoint (num_proc>1): deserialize then cache-lookup
    from ..serializers import unstuff

    return _lookup_item(unstuff(stuffed_item))




def _cleanstr(x):
    x = str(x).replace("\n", " ")
    while "  " in x:
        x = x.replace("  ", " ")
    return x.strip()
