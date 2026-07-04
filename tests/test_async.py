"""Async API: aget/aset/ahas/arun and @stashed_result over `async def`."""
import asyncio

import pytest

from hashstash import HashStash, stashed_result


def _run(coro):
    return asyncio.run(coro)


def test_aget_aset_roundtrip(tmp_path):
    stash = HashStash(root_dir=str(tmp_path / "c"))

    async def go():
        await stash.aset("k", {"a": [1, 2, 3]})
        assert await stash.ahas("k")
        return await stash.aget("k")

    assert _run(go()) == {"a": [1, 2, 3]}


# module-level (closure-free) so cache identity is stable across calls; the
# call log lives in a module global that isn't part of the function's identity
_compute_calls = []


def _compute(x):
    _compute_calls.append(x)
    return x * 2


def test_arun_plain_function_caches(tmp_path):
    stash = HashStash(root_dir=str(tmp_path / "c"))
    _compute_calls.clear()

    async def go():
        a = await stash.arun(_compute, 5)
        b = await stash.arun(_compute, 5)
        return a, b

    a, b = _run(go())
    assert a == b == 10
    assert len(_compute_calls) == 1


_fetch_calls = []


async def _fetch(x):
    _fetch_calls.append(x)
    await asyncio.sleep(0.01)
    return x * 3


def test_arun_awaits_coroutine_function(tmp_path):
    """arun must cache the awaited result, not the coroutine object."""
    stash = HashStash(root_dir=str(tmp_path / "c"))
    _fetch_calls.clear()

    async def go():
        a = await stash.arun(_fetch, 4)
        b = await stash.arun(_fetch, 4)  # cached, coroutine not re-run
        return a, b

    a, b = _run(go())
    assert a == b == 12
    assert len(_fetch_calls) == 1


_async_calls = []


@stashed_result
async def _decorated_async(x):
    _async_calls.append(x)
    await asyncio.sleep(0.01)
    return x + 100


def test_stashed_result_on_async_def(tmp_path):
    _async_calls.clear()

    async def go():
        a = await _decorated_async(1)
        b = await _decorated_async(1)  # cached
        c = await _decorated_async(2)  # new arg
        return a, b, c

    a, b, c = _run(go())
    assert (a, b, c) == (101, 101, 102)
    assert _async_calls == [1, 2]  # 1 computed once, 2 once


def test_stashed_result_async_force(tmp_path):
    calls = []

    @stashed_result
    async def f(x):
        calls.append(x)
        return x

    async def go():
        await f(7)
        await f(7, _force=True)

    _run(go())
    assert calls == [7, 7]


def test_sync_stashed_result_still_sync():
    """A plain function decorated with @stashed_result must remain a normal
    (non-coroutine) callable."""

    @stashed_result
    def plain(x):
        return x * 2

    assert not asyncio.iscoroutinefunction(plain)
    assert plain(3) == 6
