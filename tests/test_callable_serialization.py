"""Regression tests for BUG B4: closures, functools.partial, and lambdas used
to serialize WITHOUT error but deserialize into broken callables that raised only
at CALL time (silent corruption). The contract this suite enforces is:

    a callable either round-trips CORRECTLY, or fails LOUDLY at STORE (serialize)
    time -- it must never come back as a silently-broken callable.

These tests live in a real module on disk so inspect.getsource() can retrieve the
source of the locally-defined functions (it cannot for objects defined in a
`python -c`/stdin session).
"""
import logging
from functools import partial

import pytest

from hashstash import HashStash, serialize, deserialize
from hashstash.serializers.custom import SafeDeserializationError

logging.getLogger("hashstash").setLevel(logging.CRITICAL + 1)


def rt(obj):
    """Round-trip through the hashstash serializer."""
    return deserialize(serialize(obj))


# --- module-level factories (their inner functions/lambdas capture free vars) ---

def make_adder(n):
    def inner(x):
        return x + n
    return inner


def make_scaler_lambda(k):
    return lambda x: x * k


def make_multivar(a, b):
    def f(x):
        y = x + a
        return y * b
    return f


def make_str_list_capture(prefix, items):
    def show(x):
        return prefix + str(items) + str(x)
    return show


def make_factorial():
    def fact(n):
        return 1 if n <= 0 else n * fact(n - 1)
    return fact


def make_nested(a):
    def mid(b):
        def inner(c):
            return a + b + c
        return inner
    return mid


def make_no_capture():
    def c():
        return 42
    return c


def make_empty_cell_closure():
    # `later` is a cellvar (freevar of inner) because it is assigned in this
    # scope, but the assignment is unreachable, so the cell is EMPTY at the time
    # inner is returned -- an un-round-trippable closure that must fail at store.
    def inner():
        return later
    return inner
    later = 5  # noqa: F841  (unreachable on purpose)


def scale(x, factor):
    return x * factor


def three(a, b, c):
    return (a, b, c)


TOP_LEVEL_LAMBDA = lambda x: x * 2  # noqa: E731


# --------------------------------- closures ----------------------------------

class TestClosures:
    def test_closure_captures_int(self):
        f = rt(make_adder(10))
        assert f(5) == 15

    def test_closure_multivar_multiline(self):
        f = rt(make_multivar(2, 3))
        assert f(5) == 21  # (5+2)*3

    def test_closure_captures_str_and_list(self):
        f = rt(make_str_list_capture("P", [1, 2]))
        assert f(9) == "P[1, 2]9"

    def test_recursive_closure(self):
        fact = rt(make_factorial())
        assert fact(5) == 120

    def test_nested_closure(self):
        f = rt(make_nested(1)(2))
        assert f(3) == 6

    def test_lambda_closure(self):
        f = rt(make_scaler_lambda(7))
        assert f(3) == 21

    def test_closure_via_hashstash_memory_engine(self):
        s = HashStash(engine="memory")
        s.clear()
        s["f"] = make_adder(10)
        assert s["f"](5) == 15

    def test_empty_cell_closure_fails_loud_at_store(self):
        # cannot round-trip -> must raise at STORE time, not hand back a broken
        # callable that explodes later at call time.
        fn = make_empty_cell_closure()
        with pytest.raises(ValueError, match="empty cell"):
            serialize(fn)


# ------------------------------ functools.partial -----------------------------

class TestPartial:
    def test_partial_keyword(self):
        p = rt(partial(scale, factor=3))
        assert p(5) == 15

    def test_partial_positional_and_keyword(self):
        p = rt(partial(three, 1, c=3))
        assert p(2) == (1, 2, 3)

    def test_partial_over_lambda_closure(self):
        p = rt(partial(make_scaler_lambda(2)))
        assert p(10) == 20

    def test_partial_nested_in_container(self):
        d = rt({"p": partial(three, 9), "n": 5})
        assert d["p"](8, 7) == (9, 8, 7)
        assert d["n"] == 5

    def test_partial_preserves_type(self):
        p = rt(partial(scale, factor=4))
        assert isinstance(p, partial)
        assert p(3) == 12

    def test_partial_via_hashstash_memory_engine(self):
        s = HashStash(engine="memory")
        s.clear()
        s["p"] = partial(scale, factor=3)
        assert s["p"](5) == 15


# --------------- things that already worked must keep working -----------------

class TestStillWorks:
    def test_top_level_lambda(self):
        f = rt(TOP_LEVEL_LAMBDA)
        assert f(5) == 10

    def test_inline_lambda(self):
        f = rt(lambda x: x + 100)
        assert f(1) == 101

    def test_no_capture_nested_function(self):
        f = rt(make_no_capture())
        assert f() == 42


# ---------------------------------- safe mode ---------------------------------

class TestSafeMode:
    def test_safe_mode_refuses_partial(self):
        data = serialize(partial(scale, factor=3))
        with pytest.raises(SafeDeserializationError):
            deserialize(data, safe=True)

    def test_safe_mode_refuses_closure(self):
        data = serialize(make_adder(10))
        with pytest.raises(SafeDeserializationError):
            deserialize(data, safe=True)
