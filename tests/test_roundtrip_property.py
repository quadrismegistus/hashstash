"""Property-based round-trip law for the hashstash serializer:

    deserialize(serialize(x)) == x   for all x in a broad type space.

Unlike the fixed example matrix (test_type_matrix.py), this GENERATES values
across the stdlib + numpy + pandas type zoo, so it keeps probing combinations
nobody enumerated. Every serializer gap in this codebase's history (numpy
scalars, pandas Timestamp/NaT/Period, enums, nullable/categorical/tz columns)
would have been caught here.
"""
import datetime as dt
import decimal
import fractions
import zoneinfo
from collections import Counter, OrderedDict, deque
from enum import Enum, IntEnum

import pytest
from hypothesis import given, settings, strategies as st, HealthCheck

from hashstash import deserialize, serialize

np = pytest.importorskip("numpy")
pd = pytest.importorskip("pandas")
from hypothesis.extra import numpy as hnp  # noqa: E402
from hypothesis.extra import pandas as hpd  # noqa: E402

SETTINGS = settings(max_examples=60, deadline=None, suppress_health_check=list(HealthCheck))


def rt(v):
    return deserialize(serialize(v, serializer="hashstash"), serializer="hashstash")


def eq(a, b):
    """nan/NaT-aware structural equality across the type zoo."""
    if isinstance(a, np.ndarray):
        if not isinstance(b, np.ndarray) or a.dtype != b.dtype or a.shape != b.shape:
            return False
        if a.dtype.kind in "fc":
            return np.array_equal(a, b, equal_nan=True)
        if a.dtype.kind in "mM":  # datetime64/timedelta64: NaT != NaT
            return bool((np.isnat(a) == np.isnat(b)).all() and (a[~np.isnat(a)] == b[~np.isnat(b)]).all())
        if a.dtype.kind == "V":  # structured/void: nan fields defeat array_equal
            return a.tobytes() == b.tobytes()
        return np.array_equal(a, b)
    if isinstance(a, pd.Series):
        try: pd.testing.assert_series_equal(a, b); return True
        except Exception: return False
    if isinstance(a, pd.DataFrame):
        try: pd.testing.assert_frame_equal(a, b); return True
        except Exception: return False
    if isinstance(a, pd.Index):
        try: pd.testing.assert_index_equal(a, b); return True
        except Exception: return False
    if isinstance(a, pd.Categorical):
        return isinstance(b, pd.Categorical) and a.equals(b)
    if isinstance(a, float) and isinstance(b, float):
        return a == b or (a != a and b != b)
    if isinstance(a, np.generic):
        try:
            if np.isnat(a):
                return type(a) is type(b) and bool(np.isnat(b))
        except TypeError:
            pass
    # containers: recurse so nan/NaT nested inside still compares equal
    if isinstance(a, (list, tuple, deque)):
        return type(a) is type(b) and len(a) == len(b) and all(eq(x, y) for x, y in zip(a, b))
    if isinstance(a, dict):
        return type(a) is type(b) and list(a.keys()) == list(b.keys()) and all(eq(a[k], b[k]) for k in a)
    if isinstance(a, (set, frozenset)):
        return type(a) is type(b) and a == b
    try:
        if pd.isna(a) and pd.isna(b):
            return True
    except (TypeError, ValueError):
        pass
    return type(a) is type(b) and bool(a == b)


# --------------------------------------------------------------------------
# stdlib / builtin recursive space
# --------------------------------------------------------------------------

class _Color(Enum):
    RED = 1
    GREEN = "green"


class _Size(IntEnum):
    S = 1
    L = 3


_LEAVES = st.one_of(
    st.none(), st.booleans(), st.integers(),
    st.floats(allow_nan=True, allow_infinity=True),
    st.text(), st.binary(),
    st.decimals(allow_nan=False), st.fractions(), st.complex_numbers(allow_nan=False),
    st.datetimes(), st.dates(), st.times(), st.timedeltas(), st.uuids(),
    st.sampled_from(list(_Color)), st.sampled_from(list(_Size)),
    st.builds(zoneinfo.ZoneInfo, st.sampled_from(["UTC", "America/New_York"])),
)


@st.composite
def _hashable(draw):
    return draw(st.one_of(st.none(), st.booleans(), st.integers(), st.text(), st.binary(),
                          st.tuples(st.integers(), st.text())))


_NESTED = st.recursive(
    _LEAVES,
    lambda children: st.one_of(
        st.lists(children),
        st.tuples(children, children),
        st.dictionaries(_hashable(), children, max_size=5),
        st.sets(_hashable()),
        st.builds(OrderedDict, st.dictionaries(st.text(), children, max_size=4)),
        st.builds(deque, st.lists(children, max_size=4)),
    ),
    max_leaves=25,
)


@SETTINGS
@given(_NESTED)
def test_stdlib_nested_roundtrip(v):
    assert eq(rt(v), v)


# --------------------------------------------------------------------------
# numpy: arrays (all dtypes, 0-3d), scalars, structured
# --------------------------------------------------------------------------

_NP_DTYPES = st.one_of(
    hnp.integer_dtypes(), hnp.unsigned_integer_dtypes(),
    hnp.floating_dtypes(), hnp.complex_number_dtypes(), hnp.boolean_dtypes(),
    hnp.datetime64_dtypes(), hnp.timedelta64_dtypes(),
    hnp.unicode_string_dtypes(), hnp.byte_string_dtypes(),
)


@SETTINGS
@given(hnp.arrays(dtype=_NP_DTYPES, shape=hnp.array_shapes(min_dims=0, max_dims=3, max_side=4)))
def test_numpy_array_roundtrip(a):
    assert eq(rt(a), a)


@SETTINGS
@given(hnp.arrays(dtype=np.dtype([("x", "i4"), ("y", "f8"), ("z", "?")]),
                  shape=hnp.array_shapes(min_dims=1, max_dims=1, max_side=5)))
def test_numpy_structured_array_roundtrip(a):
    assert eq(rt(a), a)


@SETTINGS
@given(hnp.arrays(dtype=st.one_of(hnp.integer_dtypes(), hnp.floating_dtypes(),
                                  hnp.datetime64_dtypes(), hnp.boolean_dtypes()),
                  shape=()).map(lambda z: z[()]))
def test_numpy_scalar_roundtrip(s):
    out = rt(s)
    same = bool(out == s)
    if isinstance(s, np.floating) and np.isnan(s):
        same = bool(np.isnan(out))
    try:
        if np.isnat(s):
            same = bool(np.isnat(out))
    except TypeError:
        pass
    assert same
    # np.float64 (a float subclass) and np.str_ (a str subclass) intentionally
    # coerce to the Python type: value-preserving, and preserving the numpy type
    # would cost a check in the serialization hot path. All other scalar types
    # keep their exact numpy type.
    if not isinstance(s, (np.float64, np.str_)):
        assert type(out) is type(s)


# --------------------------------------------------------------------------
# pandas: scalars, Series (incl nullable/categorical), Index family, DataFrame
# --------------------------------------------------------------------------

@SETTINGS
@given(st.one_of(
    st.integers(0, 2**31).map(lambda n: pd.Timestamp(n, unit="s")),
    st.integers(0, 2**31).map(lambda n: pd.Timestamp(n, unit="s", tz="UTC")),
    st.integers(-(2**31), 2**31).map(lambda n: pd.Timedelta(n, unit="s")),
    st.integers(1980, 2050).map(lambda y: pd.Period(year=y, freq="Y")),
    st.integers(-100, 100).map(lambda a: pd.Interval(a, a + 5)),
    st.just(pd.NaT),
))
def test_pandas_scalar_roundtrip(v):
    assert eq(rt(v), v)


@SETTINGS
@given(st.one_of(
    hpd.series(dtype=int), hpd.series(dtype=float),
    hpd.series(dtype="datetime64[ns]"), hpd.series(dtype=bool),
    hpd.series(dtype=object, elements=st.text(max_size=5)),
    st.lists(st.integers(0, 9) | st.none(), max_size=6).map(lambda xs: pd.Series(pd.array(xs, dtype="Int64"))),
    st.lists(st.booleans() | st.none(), max_size=6).map(lambda xs: pd.Series(pd.array(xs, dtype="boolean"))),
    st.lists(st.text(max_size=4) | st.none(), max_size=6).map(lambda xs: pd.Series(pd.array(xs, dtype="string"))),
    st.lists(st.sampled_from(["a", "b", "c"]), max_size=6).map(lambda xs: pd.Series(pd.Categorical(xs))),
))
def test_pandas_series_roundtrip(s):
    assert eq(rt(s), s)


@SETTINGS
@given(st.one_of(
    hpd.indexes(dtype=int, max_size=5),
    hpd.indexes(dtype="datetime64[ns]", max_size=5),
    hpd.range_indexes(max_size=6),
    st.just(pd.MultiIndex.from_tuples([(1, "a"), (2, "b"), (3, "c")], names=["n", "l"])),
))
def test_pandas_index_roundtrip(idx):
    assert eq(rt(idx), idx)


@SETTINGS
@given(hpd.data_frames([
    hpd.column("i", dtype=int),
    hpd.column("f", dtype=float),
    hpd.column("s", dtype=object, elements=st.text(max_size=4)),
]))
def test_pandas_dataframe_roundtrip(df):
    assert eq(rt(df), df)


def test_fixed_types_deterministic():
    """Explicit regression cases for every type this work fixed — a fast guard
    that doesn't depend on hypothesis generating the right example."""
    cases = [
        _Color.RED, _Color.GREEN, _Size.L,                       # enum / IntEnum
        zoneinfo.ZoneInfo("America/New_York"),
        np.int64(-5), np.uint8(200), np.float32(1.5), np.complex128(1 + 2j), np.bool_(True),
        np.datetime64("2020-01-01", "ns"), np.timedelta64(5, "D"),
        np.float64("inf"), np.float64("-inf"),                    # non-finite numpy float
        np.bytes_(b"hi"),
        np.array([(1, 2.0)], dtype=[("a", "i4"), ("b", "f8")]),  # structured array
        pd.Timestamp("2020-01-01 00:00:00.123456789"),
        pd.Timestamp("2020-06-01", tz="US/Eastern"),
        pd.Timedelta(days=2, seconds=5), pd.Period("2020-03", freq="M"),
        pd.Interval(0, 5), pd.NaT,
        pd.Series([1, None, 3], dtype="Int64", name="s"),
        pd.Series(["a", None, "c"], dtype="string"),
        pd.Series(pd.to_datetime(["2020-01-01"]).tz_localize("UTC")),
        pd.Categorical(["a", "b", "a"], categories=["b", "a", "c"], ordered=True),
        pd.Index([1, 2, 3], name="ix"),
        pd.RangeIndex(2, 10, 2),
        pd.DatetimeIndex(["2020-01-01", "2021-06-01"]),
        pd.MultiIndex.from_tuples([(1, "a"), (2, "b")], names=["n", "l"]),
    ]
    for v in cases:
        assert eq(rt(v), v), f"{type(v).__name__}: {v!r}"


def test_pandas_dataframe_rich_dtypes_faithful():
    """The column-wise serializer must preserve per-column dtypes exactly:
    nullable Int64, categorical, and tz-aware datetime (the df.values approach
    collapsed these)."""
    df = pd.DataFrame({
        "i": pd.array([1, None, 3], dtype="Int64"),
        "cat": pd.Categorical(["a", "b", "a"], categories=["b", "a", "c"], ordered=True),
        "dt": pd.to_datetime(["2020-01-01", "2020-06-01", "2021-01-01"]).tz_localize("UTC"),
        "f": [1.5, 2.5, 3.5],
    })
    out = rt(df)
    pd.testing.assert_frame_equal(out, df)
    assert list(out.dtypes.astype(str)) == list(df.dtypes.astype(str))
