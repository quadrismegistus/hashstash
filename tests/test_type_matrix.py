"""Exhaustive round-trip matrix for the hashstash serializer. Locks down that a
wide zoo of Python/stdlib/numpy/pandas types survives serialize -> deserialize
with both value and (where meaningful) type preserved. This is the regression
net; several entries here were bugs found while writing it (numpy scalars and
pandas Timestamp/Timedelta failed to round-trip)."""
import datetime
import decimal
import fractions
from collections import Counter, OrderedDict, deque

import pytest

from hashstash import deserialize, serialize
from hashstash.serializers.custom import safe_deserialization


def rt(v):
    return deserialize(serialize(v, serializer="hashstash"), serializer="hashstash")


# --- stdlib / builtin types (always available) -------------------------------

STDLIB_CASES = [
    # primitives
    None, True, False, 0, -1, 2 ** 70, 3.14, -0.0, "", "unicode ☃ é",
    # non-finite floats
    float("inf"), float("-inf"),
    # numeric types
    decimal.Decimal("3.141592653589793"),
    decimal.Decimal("-0.0001"),
    fractions.Fraction(22, 7),
    complex(1, -2),
    # bytes
    b"", b"\x00\x01\x02bytes", bytearray(b"mutable"),
    # containers
    [], {}, set(), frozenset(),
    [1, "two", 3.0, None, True],
    {"a": 1, "b": [2, 3]},
    {1: "int-key", (2, 3): "tuple-key"},
    {1, 2, 3}, frozenset([4, 5]),
    (1, 2, 3), (),
    # datetime family
    datetime.datetime(2020, 1, 1, 12, 30, 15, 123456),
    datetime.datetime(2020, 6, 1, tzinfo=datetime.timezone.utc),
    datetime.date(1999, 12, 31),
    datetime.time(23, 59, 59),
    datetime.timedelta(days=3, hours=2, seconds=1),
    datetime.timezone.utc,
    # collections
    OrderedDict([("x", 1), ("y", 2)]),
    Counter({"a": 3, "b": 1}),
    deque([1, 2, 3]),
    # deeply nested combination
    {"list": [1, (2, {3, 4}), {"k": b"v"}], "dt": datetime.date(2020, 1, 1)},
]


@pytest.mark.parametrize("value", STDLIB_CASES, ids=lambda v: type(v).__name__ + repr(v)[:20])
def test_stdlib_roundtrip(value):
    out = rt(value)
    if isinstance(value, float) and value != value:  # nan
        assert out != out
    else:
        assert out == value
    assert type(out) is type(value)


# --- numpy -------------------------------------------------------------------

def test_numpy_scalars_roundtrip():
    np = pytest.importorskip("numpy")
    cases = [
        np.int8(-5), np.int16(-300), np.int32(-70000), np.int64(42),
        np.uint8(200), np.uint16(60000), np.uint32(4_000_000_000), np.uint64(10),
        np.float16(1.5), np.float32(3.25),
        np.complex64(1 + 2j), np.complex128(3 - 4j),
        np.bool_(True), np.bool_(False),
    ]
    for v in cases:
        out = rt(v)
        assert out == v, f"value mismatch for {v!r}"
        assert type(out) is type(v), f"type not preserved for {v!r}: got {type(out)}"


def test_numpy_float64_preserves_value():
    np = pytest.importorskip("numpy")
    # float64 subclasses float -> stored as plain float (value preserved, not type)
    out = rt(np.float64(3.14))
    assert out == 3.14


def test_numpy_array_roundtrip():
    np = pytest.importorskip("numpy")
    for arr in [np.array([1, 2, 3]), np.array([[1.5, 2.5], [3.5, 4.5]]), np.array([], dtype="int64")]:
        out = rt(arr)
        assert out.dtype == arr.dtype
        assert out.shape == arr.shape
        assert (out == arr).all()


# --- pandas ------------------------------------------------------------------

def test_pandas_timestamp_roundtrip():
    pd = pytest.importorskip("pandas")
    cases = [
        pd.Timestamp("2020-01-01"),
        pd.Timestamp("2020-01-01 12:00:00.123456789"),   # nanoseconds
        pd.Timestamp("2020-06-01", tz="UTC"),
        pd.Timestamp("2020-06-01 09:00", tz="US/Eastern"),
    ]
    for ts in cases:
        out = rt(ts)
        assert out == ts
        assert out.nanosecond == ts.nanosecond
        assert type(out) is type(ts)


def test_pandas_timedelta_roundtrip():
    pd = pytest.importorskip("pandas")
    td = pd.Timedelta(days=2, seconds=5, microseconds=7)
    out = rt(td)
    assert out == td
    assert type(out) is type(td)


def test_pandas_na_sentinels():
    pd = pytest.importorskip("pandas")
    # NaT is not == itself, so compare via isna; NA is a singleton
    assert pd.isna(rt(pd.NaT))
    assert rt(pd.NA) is pd.NA


# --- safe mode ---------------------------------------------------------------

def test_data_types_roundtrip_in_safe_mode():
    """Data types (including numpy/pandas via the vetted table) must round-trip
    even under safe mode."""
    np = pytest.importorskip("numpy")
    pd = pytest.importorskip("pandas")
    values = [
        {"a": 1, "b": [2, 3]}, {1, 2}, (1, 2), b"bytes",
        decimal.Decimal("1.5"), datetime.datetime(2020, 1, 1),
        np.int64(9), np.float32(1.5), pd.Timestamp("2021-01-01"),
    ]
    with safe_deserialization():
        for v in values:
            assert rt(v) == v
