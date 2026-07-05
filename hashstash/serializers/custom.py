# Explicit stdlib imports: this package's `from . import *` chains are
# circular, and whether a name has landed in the package namespace yet
# depends on import order (spawn workers + editable installs order imports
# differently). Never rely on the star-chain for stdlib names.
import enum
import importlib
import inspect
import os
import types
from contextlib import contextmanager

from . import *
from ..utils.logs import *
from pprint import pprint
import json
from typing import Any
from pathlib import Path
from ..utils.misc import ReusableGenerator

PANDAS_EXTENSION_ACTIVATED = True

# Plain dicts containing any of these keys must round-trip through the tagged
# '__pytype__: dict' form, or the deserializer would misread them as object markers.
RESERVED_DICT_KEYS = frozenset({'__py__', '__pytype__', '__data__'})


def _dict_has_simple_keys(d):
    """True if every key is a plain str and none collide with the reserved
    marker keys — i.e. the dict can serialize as a normal JSON object rather
    than the tagged '__pytype__: dict' form. Single pass with early exit
    (the old `all(isinstance...) and RESERVED & keys` scanned the keys twice)."""
    for k in d:
        if not isinstance(k, str) or k in RESERVED_DICT_KEYS:
            return False
    return True


# --- safe deserialization mode ------------------------------------------------
#
# Deserializing a hashstash payload can execute code: functions/classes are
# rebuilt with exec, reducers invoke an importable callable chosen by the
# payload ('os.system' qualifies), and instance state is applied blindly.
# Safe mode restricts round-trips to DATA: primitives, containers, bytes,
# paths, the vetted CUSTOM_DESERIALIZERS table (numpy/pandas), and a small
# allowlist of value-type constructors. Everything else raises
# SafeDeserializationError instead of running.
#
# Enable per stash with HashStash(safe=True), globally with HASHSTASH_SAFE=1,
# or around any deserialize call with the safe_deserialization() context.

import contextvars

_SAFE_CTX = contextvars.ContextVar("hashstash_safe", default=None)


class SafeDeserializationError(Exception):
    """Raised in safe mode when a payload would execute code on load."""


def _safe_mode_active():
    ctx = _SAFE_CTX.get()
    if ctx is not None:
        return ctx
    return bool(os.environ.get("HASHSTASH_SAFE"))


@contextmanager
def safe_deserialization(enabled=True):
    token = _SAFE_CTX.set(enabled)
    try:
        yield
    finally:
        _SAFE_CTX.reset(token)


def _refuse_unsafe(what):
    raise SafeDeserializationError(
        f"safe mode refused to deserialize {what}: it would execute code on "
        f"load. Open this stash with safe=False only if you trust its writer."
    )


# value-type constructors a reducer may invoke in safe mode: fixed-arity
# builtins/stdlib types whose construction runs no user code
SAFE_REDUCER_CONSTRUCTORS = frozenset({
    "datetime.datetime",
    "datetime.date",
    "datetime.time",
    "datetime.timedelta",
    "datetime.timezone",
    "builtins.complex",
    "complex",
    "decimal.Decimal",
    "fractions.Fraction",
    "uuid.UUID",
    "collections.OrderedDict",
    "collections.Counter",
    "collections.deque",
})


# orjson accelerates the VALUE serialization path (dumps/loads of the already
# JSON-safe structure _serialize_custom produces). It is NEVER used for cache
# KEYS: those must stay stdlib-json canonical (sort_keys=True) so a key hashes
# to the same bytes whether or not orjson happens to be installed. orjson also
# rejects a few things stdlib json accepts (ints beyond 64 bits), so value
# serialization falls back to stdlib json on any orjson error.
_orjson = None
_orjson_checked = False


def _get_orjson():
    global _orjson, _orjson_checked
    if not _orjson_checked:
        _orjson_checked = True
        try:
            import orjson
            _orjson = orjson
        except ImportError:
            _orjson = None
    return _orjson


def _dumps_value(structure):
    """Serialize a JSON-safe structure for a VALUE (non-canonical). orjson when
    available (returns bytes), else stdlib json (str); falls back to json for
    inputs orjson rejects."""
    oj = _get_orjson()
    if oj is not None:
        try:
            return oj.dumps(structure)
        except (TypeError, ValueError):
            pass  # e.g. int > 64-bit — stdlib json handles it
    return json.dumps(structure)


def _loads_any(data):
    """Parse JSON produced by either orjson or stdlib json (str or bytes).

    Deliberately uses stdlib json.loads, NOT orjson.loads: orjson.loads
    silently coerces integers beyond 64 bits to float (lossy), and stdlib
    json.loads is both correct and C-accelerated. orjson's speed win is on the
    write (dumps) side, where correctness is preserved by the big-int fallback."""
    if isinstance(data, (bytes, bytearray)):
        data = data.decode("utf-8")
    return json.loads(data)


def _is_json_native(obj):
    """True if a VALUE can be dumped directly by json/orjson and read back
    identically, letting serialize_custom skip the recursive _serialize_custom
    rebuild entirely — the biggest serialization cost for JSON-shaped payloads.

    EXACT types only (`type(obj) is X`, not isinstance): subclasses take the
    full path, so bool/IntEnum/OrderedDict/defaultdict/namedtuple and friends
    never slip through with different-from-full-path semantics.

    Rejected (they serialize DIFFERENTLY via _serialize_custom, so a direct dump
    would corrupt the round-trip):
      - tuple    -> full path tags it; a direct dump becomes a JSON array (list)
      - set/bytes/... -> need their custom serializers
      - dict with a non-str or reserved-marker key -> full path uses the tagged
        '__pytype__: dict' form; a direct dump would either coerce int keys to
        str or be misread as an object marker on load

    Big ints and non-finite floats are NOT rejected here: _dumps_value falls
    back to stdlib json.dumps(obj) for them, which for a native obj is byte-for-
    byte what the full path would produce."""
    t = type(obj)
    if obj is None or t is bool or t is int or t is str:
        return True
    if t is float:
        # finite floats dump fine; inf/nan need the tagged form (orjson emits
        # null for them), so route those through the full path
        return obj == obj and obj != float("inf") and obj != float("-inf")
    if t is list:
        return all(_is_json_native(v) for v in obj)
    if t is dict:
        for k, v in obj.items():
            if type(k) is not str or k in RESERVED_DICT_KEYS:
                return False
            if not _is_json_native(v):
                return False
        return True
    return False


# NOTE: serialize_custom / _serialize_custom are intentionally NOT decorated
# with @log.debug. _serialize_custom recurses once per node of the value tree
# (millions of calls on a large payload), and the log-wrapper's per-call
# overhead dominated ~20% of serialization time even with logging disabled
# (measured 1.5x speedup from removing it). Tracing lives at the serialize()/
# deserialize() boundary in serializer.py, which fires once per operation.
def serialize_custom(obj: Any, sort_keys: bool = False) -> str:
    # VALUE fast-path: a purely JSON-native value dumps identically without the
    # recursive _serialize_custom rebuild — the dominant serialization cost. Only
    # for values (not the canonical key path): keys stay on the audited full path.
    if not sort_keys and _is_json_native(obj):
        return _dumps_value(obj)
    serialized = _serialize_custom(obj)
    if sort_keys:
        # canonical KEY path: stdlib json with sorted keys so equal keys hash
        # identically regardless of insertion order OR whether orjson is present
        return json.dumps(serialized, sort_keys=True)
    # VALUE path: orjson-accelerated when available
    return _dumps_value(serialized)

def stuff(obj, data=None):
    return _serialize_custom(obj, data=data)
def unstuff(obj):
    return _deserialize_custom(obj)

def _serialize_custom(obj: Any, data:Any=None) -> Any:
    if obj is None:
        return None

    if data is not None:
        return {
            '__py__': get_obj_addr(obj),
            '__data__': _serialize_custom(data)
        }

    # Enum BEFORE the primitive check: IntEnum/IntFlag are int subclasses, so the
    # primitive branch would coerce them to a bare int and lose the type; a plain
    # Enum otherwise fell to the instance path and recursed forever (serializing
    # the class re-serializes its members). Stored as class-address + member name.
    if isinstance(obj, enum.Enum):
        return {
            '__py__': get_obj_addr(type(obj)),
            '__pytype__': 'enum',
            '__data__': obj.name,
        }

    if isinstance(obj, float):
        # non-finite floats round-trip through a tagged form: orjson emits null
        # for inf/nan (silent data loss) and stdlib json emits non-standard
        # Infinity/NaN. Store a CANONICAL token, not repr(obj): a float subclass
        # like np.float64 reprs as 'np.float64(inf)', which float() can't parse.
        if obj != obj:
            return {'__pytype__': 'float', '__val__': 'nan'}
        if obj == float("inf"):
            return {'__pytype__': 'float', '__val__': 'inf'}
        if obj == float("-inf"):
            return {'__pytype__': 'float', '__val__': '-inf'}
        return obj

    if isinstance(obj, (str, int, bool)):
        return obj

    if isinstance(obj, dict):
        if type(obj) is not dict:
            # a dict SUBCLASS (OrderedDict, Counter, defaultdict, ...): coercing
            # it to a plain dict would lose its type and behavior. Its reducer
            # rebuilds the exact type — and these types are on the safe-mode
            # allowlist, so this round-trips under safe=True too.
            return ReducerSerializer.serialize(obj)
        if _dict_has_simple_keys(obj):
            return {k: _serialize_custom(v) for k, v in obj.items()}
        # Non-string keys (JSON would coerce them to strings) or reserved marker keys:
        # keep keys as a list of [key, value] pairs so their types survive the round-trip.
        return {
            '__pytype__': 'dict',
            '__items__': [
                [_serialize_custom(k), _serialize_custom(v)] for k, v in obj.items()
            ],
        }

    if isinstance(obj, list):
        return [_serialize_custom(v) for v in obj]

    addr = get_obj_addr(obj)
    if addr in CUSTOM_SERIALIZERS:
        return CUSTOM_SERIALIZERS[addr](obj)

    if hasattr(obj, 'to_serialized') and callable(obj.to_serialized) and not inspect.isclass(obj):
        return {
            '__py__': addr,
            '__data__': _serialize_custom(obj.to_serialized())
        }
    
    if hasattr(obj, 'to_dict') and callable(obj.to_dict) and not inspect.isclass(obj):
        return {
            '__py__': addr,
            '__data__': _serialize_custom(obj.to_dict())
        }
    
    if isinstance(obj, type):
        return ClassSerializer.serialize(obj)

    if inspect.ismodule(obj):
        # serialize modules by reference — recursing into __dict__ would pull in
        # the world (and blow the recursion limit)
        return {'__py__': obj.__name__, '__pytype__': 'module'}

    if inspect.isgenerator(obj):
        return GeneratorSerializer.serialize(obj)

    if is_function(obj):
        return FunctionSerializer.serialize(obj)
    
    # Handle class instances
    if hasattr(obj, '__dict__'):
        return InstanceSerializer.serialize(obj)

    if hasattr(obj, '__reduce__'):
        return ReducerSerializer.serialize(obj)
    
    log.debug(f"Unsupported object type: {type(obj)}")
    return obj


### Deserializing

def deserialize_custom(serialized_str: str) -> Any:
    parsed = _loads_any(serialized_str)
    # deserialize fast-path (symmetric to the serialize fast-path): json.loads
    # already produced the final Python structure for a purely JSON-native value,
    # so if nothing in it is a hashstash marker dict, return it directly and skip
    # _deserialize_custom's recursive, allocating rebuild — the dominant read cost.
    if _parsed_is_native(parsed):
        return parsed
    return _deserialize_custom(parsed)


def _parsed_is_native(obj):
    """True if a json.loads result contains no hashstash marker dict — i.e. it is
    already the deserialized value. A marker is a dict carrying '__py__' or
    '__pytype__'; the serializer routes any user dict that happens to hold a
    reserved key through the tagged '__pytype__: dict' form, so a plain dict here
    never legitimately holds one. Native data carries no code, so this is safe
    under safe mode too."""
    t = type(obj)
    if t is dict:
        if '__py__' in obj or '__pytype__' in obj:
            return False
        for v in obj.values():
            if not _parsed_is_native(v):
                return False
        return True
    if t is list:
        for v in obj:
            if not _parsed_is_native(v):
                return False
        return True
    return True

def _deserialize_object_data(obj, obj_data: Any) -> Any:
    if _safe_mode_active():
        # from_serialized/from_dict/__setstate__ on a payload-chosen importable
        # class is arbitrary code execution
        _refuse_unsafe(f"object reconstruction via {get_obj_addr(obj)!r}")
    if hasattr(obj, 'from_serialized') and callable(obj.from_serialized):
        return obj.from_serialized(obj_data)

    if hasattr(obj, 'from_dict') and callable(obj.from_dict):
        return obj.from_dict(obj_data)
    # If obj is a class, instantiate a bare object first (skipping __init__), then apply state
    # to the instance. Pandas 3.0 removed Series.from_dict and made NDFrame.__setstate__ strict,
    # so the class-passed-to-__setstate__ pattern breaks.
    if isinstance(obj, type):
        inst = obj.__new__(obj)
        if hasattr(inst, '__setstate__'):
            _invoke_setstate(inst, obj_data)
        else:
            inst.__dict__.update(obj_data)
        return inst
    if hasattr(obj, '__setstate__'):
        _invoke_setstate(obj, obj_data)
    else:
        obj.__dict__.update(obj_data)
    return obj


def _invoke_setstate(obj, state):
    # pandas 3.x made NDFrame.__setstate__ require the `state` arg by keyword.
    # Older versions accept positional. Try positional first, fall back to kw.
    try:
        obj.__setstate__(state)
    except TypeError:
        obj.__setstate__(state=state)
            


def _deserialize_custom(data: Any) -> Any:
    if isinstance(data, (str, int, float, bool, type(None))):
        return data
    
    if isinstance(data, list):
        return [_deserialize_custom(v) for v in data]
    
    if isinstance(data, dict):
        pytype = data.get('__pytype__')
        addr = data.get('__py__')

        if pytype == 'dict':
            return {
                _deserialize_custom(k): _deserialize_custom(v)
                for k, v in data['__items__']
            }

        if pytype == 'float':
            return float(data['__val__'])

        if pytype == 'enum':
            # reconstructing an enum member requires importing its class, which
            # (like any import reference) can execute module-level code
            if _safe_mode_active():
                _refuse_unsafe(f"an enum ({addr!r})")
            return flexible_import(addr)[data['__data__']]

        if pytype == 'instance':
            if _safe_mode_active():
                _refuse_unsafe(f"an instance of {addr!r}")
            return InstanceSerializer.deserialize(data)

        if addr and addr in CUSTOM_DESERIALIZERS:
            # fixed, vetted dispatch table (numpy/pandas/containers): the code
            # that runs is ours, not payload-chosen — allowed in safe mode
            return CUSTOM_DESERIALIZERS[addr](data)

        if pytype == 'reducer':
            return ReducerSerializer.deserialize(data)

        if pytype in {'function', 'classmethod', 'instancemethod'}:
            if _safe_mode_active():
                _refuse_unsafe(f"a function ({addr!r})")
            return FunctionSerializer.deserialize(data)

        if pytype == 'class':
            if _safe_mode_active():
                # a bare reference to an allowlisted value type (no exec-based
                # reconstruction) is fine — e.g. `complex` inside its own reducer
                # args. Reconstructing a class from stored bases/methods is not.
                is_reference = '__bases__' not in data
                if not (is_reference and addr in SAFE_REDUCER_CONSTRUCTORS):
                    _refuse_unsafe(f"a class ({addr!r})")
            return ClassSerializer.deserialize(data)

        if pytype == 'generator':
            return GeneratorSerializer.deserialize(data)

        if pytype == 'module':
            if _safe_mode_active():
                _refuse_unsafe(f"a module import ({addr!r})")
            return importlib.import_module(addr)

        obj_data = data.get('__data__')
        # 'is not None': an empty-but-valid payload ({}, [], 0, '') must still be
        # reconstructed — the old truthiness check returned the class itself instead
        if obj_data is not None and addr and can_import_object(addr):
            return _deserialize_object_data(flexible_import(addr), _deserialize_custom(obj_data))

        if addr:
            if _safe_mode_active():
                _refuse_unsafe(f"an import reference ({addr!r})")
            return flexible_import(addr)

        return {_deserialize_custom(k): _deserialize_custom(v) for k, v in data.items()}
    
    return data


## custom object de/serializers

class CustomSerializer:
    @staticmethod
    def serialize(obj: Any) -> dict:
        raise NotImplementedError

    @staticmethod
    def deserialize(data: dict) -> Any:
        raise NotImplementedError

class IterableSerializer(CustomSerializer):
    @staticmethod
    def serialize(obj):
        items = [_serialize_custom(x) for x in obj]
        if isinstance(obj, (set, frozenset)):
            # set iteration order depends on PYTHONHASHSEED: sort the serialized
            # forms so equal sets always produce identical (cache-key-stable) bytes
            items.sort(key=lambda x: json.dumps(x, sort_keys=True, default=str))
        return {
            '__py__': get_obj_addr(obj),
            '__data__': items
        }

    @staticmethod
    def deserialize(data):
        obj = flexible_import(data['__py__'])
        return obj(_deserialize_custom(data['__data__']))

def deactivate_pandas_extension():
    global PANDAS_EXTENSION_ACTIVATED
    PANDAS_EXTENSION_ACTIVATED = False

def activate_pandas_extension():
    global PANDAS_EXTENSION_ACTIVATED
    PANDAS_EXTENSION_ACTIVATED = True

def pandas_extension_activated():
    global PANDAS_EXTENSION_ACTIVATED
    return PANDAS_EXTENSION_ACTIVATED

@fcache
def pandas_installed():
    try:
        import pandas
        return True
    except ImportError:
        return False


class PandasDataFrameSerializer(CustomSerializer):
    """Column-wise: each column is serialized as its own Series (preserving that
    column's exact dtype — nullable/categorical/tz/object all survive), alongside
    the row index and the columns Index. This avoids df.values, which collapses
    every column to one common dtype and can't be recovered faithfully."""

    @staticmethod
    def serialize(obj):
        assert pandas_installed(), "Pandas is required for this serializer."
        return {
            '__py__': get_obj_addr(obj),
            '__pytype__': 'pd_dataframe',
            '__data__': {
                'columns': PandasIndexSerializer.serialize(obj.columns),
                'index': PandasIndexSerializer.serialize(obj.index),
                # positional column-Series (handles duplicate/ non-str column labels)
                'series': [
                    PandasSeriesSerializer.serialize(obj.iloc[:, i])
                    for i in range(obj.shape[1])
                ],
            },
        }

    @staticmethod
    def deserialize(data):
        assert pandas_installed(), "Pandas is required for this serializer."
        import pandas as pd

        d = data['__data__']
        columns = PandasIndexSerializer.deserialize(d['columns'])
        index = PandasIndexSerializer.deserialize(d['index'])
        cols = [PandasSeriesSerializer.deserialize(s) for s in d['series']]
        if cols:
            df = pd.concat(cols, axis=1, keys=range(len(cols)))
            df.columns = columns          # restore real labels (incl. duplicates / dtype)
        else:
            df = pd.DataFrame(index=index, columns=columns)
        df.index = index
        return df

def _dtype_to_data(dtype):
    # structured dtypes have field names; str(dtype) of a structured dtype is not
    # reparseable by np.dtype, so store the descr (a list of field tuples). Plain
    # dtypes round-trip fine as their string.
    return dtype.descr if dtype.names else str(dtype)


def _data_to_dtype(spec):
    import numpy as np
    if isinstance(spec, list):
        # JSON turned each field tuple (and any subarray shape) into a list
        fields = []
        for f in spec:
            if len(f) >= 3 and isinstance(f[2], list):
                fields.append((f[0], f[1], tuple(f[2])))
            else:
                fields.append(tuple(f))
        return np.dtype(fields)
    return np.dtype(spec)


class NumpySerializer(CustomSerializer):
    @staticmethod
    def serialize(obj):
        outd = {
            '__py__': get_obj_addr(obj),
            '__data__': {
                'dtype': _dtype_to_data(obj.dtype),
                'shape': obj.shape
            }
        }
        if obj.dtype.kind == 'O':
            outd['__data__']['values'] = [_serialize_custom(item) for item in obj.flatten()]
        else:
            outd['__data__']['bytes'] = encode(obj.tobytes(), compress=False, b64=True, as_string=True)
        return outd

    @staticmethod
    def deserialize(data):
        try:
            import numpy as np
        except ImportError:
            raise ImportError("NumPy is required for this deserializer.")
        dtype = _data_to_dtype(data['__data__']['dtype'])
        shape = tuple(data['__data__']['shape'])
        if 'bytes' in data['__data__']:
            arr_bytes = decode(data['__data__']['bytes'], compress=False, b64=True)
            return np.frombuffer(arr_bytes, dtype=dtype).reshape(shape)
        else:
            return np.array([_deserialize_custom(item) for item in data['__data__']['values']], dtype=dtype).reshape(shape)


class NumpyScalarSerializer(CustomSerializer):
    """numpy scalar types (np.int64, np.float32, np.bool_, np.complex128, ...).

    These are NOT plain-int/float subclasses in modern numpy, so without this
    they fell through to the generic reducer, whose constructor
    (numpy's `scalar`) resolves to the wrong address and fails to reconstruct.
    Stored as dtype + the Python-native value; rebuilt with dtype.type(value)."""

    @staticmethod
    def serialize(obj):
        return {
            '__py__': get_obj_addr(obj),
            '__pytype__': 'npscalar',
            '__data__': {'dtype': str(obj.dtype), 'value': _serialize_custom(obj.item())},
        }

    @staticmethod
    def deserialize(data):
        try:
            import numpy as np
        except ImportError:
            raise ImportError("NumPy is required for this deserializer.")
        d = data['__data__']
        return np.dtype(d['dtype']).type(_deserialize_custom(d['value']))


class PandasTimestampSerializer(CustomSerializer):
    """pandas.Timestamp — took the generic instance path (rebuild-from-state),
    which fails on the C-backed type. Stored as ISO 8601 (preserves timezone
    and nanoseconds) and rebuilt with pd.Timestamp()."""

    @staticmethod
    def serialize(obj):
        return {
            '__py__': get_obj_addr(obj),
            '__pytype__': 'pd_timestamp',
            '__data__': {'iso': obj.isoformat()},
        }

    @staticmethod
    def deserialize(data):
        try:
            import pandas as pd
        except ImportError:
            raise ImportError("pandas is required for this deserializer.")
        return pd.Timestamp(data['__data__']['iso'])


class PandasTimedeltaSerializer(CustomSerializer):
    """pandas.Timedelta — same generic-instance failure as Timestamp. Stored as
    ISO 8601 duration and rebuilt with pd.Timedelta()."""

    @staticmethod
    def serialize(obj):
        return {
            '__py__': get_obj_addr(obj),
            '__pytype__': 'pd_timedelta',
            '__data__': {'iso': obj.isoformat()},
        }

    @staticmethod
    def deserialize(data):
        try:
            import pandas as pd
        except ImportError:
            raise ImportError("pandas is required for this deserializer.")
        return pd.Timedelta(data['__data__']['iso'])


class PandasNaTSerializer(CustomSerializer):
    """pandas.NaT — the generic instance path built a broken pseudo-NaT (repr'd
    as NaT but pd.isna() returned False on it). NaT is a singleton, so store a
    bare marker and return the real pd.NaT on load."""

    @staticmethod
    def serialize(obj):
        return {
            '__py__': get_obj_addr(obj),
            '__pytype__': 'pd_nat',
            '__data__': {},
        }

    @staticmethod
    def deserialize(data):
        try:
            import pandas as pd
        except ImportError:
            raise ImportError("pandas is required for this deserializer.")
        return pd.NaT


class NumpyDatetime64Serializer(CustomSerializer):
    """numpy.datetime64 / numpy.timedelta64 — like the other numpy scalars they
    fell to the generic reducer and failed to reconstruct. Stored as the raw
    int64 view plus the dtype (which carries the time unit); rebuilt by viewing
    the int back as that dtype, preserving unit and value (incl. NaT)."""

    @staticmethod
    def serialize(obj):
        return {
            '__py__': get_obj_addr(obj),
            '__pytype__': 'np_datetime64',
            '__data__': {'dtype': str(obj.dtype), 'value': int(obj.view('int64'))},
        }

    @staticmethod
    def deserialize(data):
        try:
            import numpy as np
        except ImportError:
            raise ImportError("NumPy is required for this deserializer.")
        d = data['__data__']
        return np.int64(d['value']).view(d['dtype'])


class PandasPeriodSerializer(CustomSerializer):
    """pandas.Period — took the generic instance path and silently round-tripped
    to NaT. Stored as its ordinal + frequency string and rebuilt exactly."""

    @staticmethod
    def serialize(obj):
        return {
            '__py__': get_obj_addr(obj),
            '__pytype__': 'pd_period',
            '__data__': {'ordinal': obj.ordinal, 'freq': obj.freqstr},
        }

    @staticmethod
    def deserialize(data):
        try:
            import pandas as pd
        except ImportError:
            raise ImportError("pandas is required for this deserializer.")
        d = data['__data__']
        return pd.Period(ordinal=d['ordinal'], freq=d['freq'])


class ZoneInfoSerializer(CustomSerializer):
    """zoneinfo.ZoneInfo — the reducer couldn't reconstruct it. Stored as its IANA
    key and rebuilt with ZoneInfo(key)."""

    @staticmethod
    def serialize(obj):
        return {
            '__py__': get_obj_addr(obj),
            '__pytype__': 'zoneinfo',
            '__data__': {'key': obj.key},
        }

    @staticmethod
    def deserialize(data):
        from zoneinfo import ZoneInfo
        return ZoneInfo(data['__data__']['key'])


class PandasSeriesSerializer(CustomSerializer):
    """Preserves the Series' exact dtype (incl. nullable Int64/boolean/string,
    categorical, datetime-with-tz), its name, and its index. numpy-backed
    numeric data takes the fast bytes path; ExtensionArray-backed data is stored
    as elements + dtype (categoricals keep their category order via the
    Categorical serializer)."""

    @staticmethod
    def serialize(obj):
        import numpy as np
        import pandas as pd
        values = obj.values
        if isinstance(values, pd.Categorical):
            vdata = {'kind': 'categorical', 'data': PandasCategoricalSerializer.serialize(values)}
        elif isinstance(obj.dtype, np.dtype) and obj.dtype.kind != 'O':
            # a genuine numpy dtype (int/float/bool/naive-datetime): fast bytes path.
            # Checked on obj.dtype, NOT values.dtype: a tz-aware series is an
            # ExtensionDtype whose .values is a NAIVE numpy array — routing it here
            # would silently drop the timezone.
            vdata = {'kind': 'numpy', 'data': NumpySerializer.serialize(values)}
        else:
            # object ndarray, or an ExtensionArray (nullable Int64/boolean/string,
            # tz-aware datetime, period, interval): a plain element list round-trips
            # every element; the exact dtype is re-applied on load.
            vdata = {'kind': 'list', 'data': _serialize_custom(obj.tolist())}
        return {
            '__py__': get_obj_addr(obj),
            '__pytype__': 'pd_series',
            '__data__': {
                'values': vdata,
                'dtype': str(obj.dtype),
                'name': _serialize_custom(obj.name),
                'index': PandasIndexSerializer.serialize(obj.index),
            },
        }

    @staticmethod
    def deserialize(data):
        try:
            import pandas as pd
        except ImportError:
            raise ImportError("Pandas is required for this deserializer.")
        d = data['__data__']
        vkind = d['values']['kind']
        if vkind == 'categorical':
            values = PandasCategoricalSerializer.deserialize(d['values']['data'])
        elif vkind == 'numpy':
            values = NumpySerializer.deserialize(d['values']['data'])
        else:
            values = _deserialize_custom(d['values']['data'])
        index = PandasIndexSerializer.deserialize(d['index'])
        s = pd.Series(values, index=index, name=_deserialize_custom(d['name']))
        # re-apply the exact dtype for the list path (nullable Int64/boolean/string)
        if vkind == 'list' and str(s.dtype) != d['dtype']:
            s = s.astype(d['dtype'])
        return s


class PandasIndexSerializer(CustomSerializer):
    """All pandas Index flavours: MultiIndex (from tuples), RangeIndex (start/
    stop/step), and everything else via its element list + dtype (which covers
    Int/Float/Datetime/Period/Interval/Categorical indexes)."""

    @staticmethod
    def serialize(obj):
        import pandas as pd
        if isinstance(obj, pd.MultiIndex):
            payload = {
                'kind': 'multi',
                'tuples': _serialize_custom([tuple(t) for t in obj]),
                'names': _serialize_custom(list(obj.names)),
            }
        elif isinstance(obj, pd.RangeIndex):
            payload = {
                'kind': 'range',
                'start': int(obj.start), 'stop': int(obj.stop), 'step': int(obj.step),
                'name': _serialize_custom(obj.name),
            }
        else:
            payload = {
                'kind': 'index',
                'values': _serialize_custom(obj.tolist()),
                'dtype': str(obj.dtype),
                'name': _serialize_custom(obj.name),
            }
        return {'__py__': get_obj_addr(obj), '__pytype__': 'pd_index', '__data__': payload}

    @staticmethod
    def deserialize(data):
        import pandas as pd
        d = data['__data__']
        kind = d['kind']
        if kind == 'multi':
            return pd.MultiIndex.from_tuples(
                _deserialize_custom(d['tuples']), names=_deserialize_custom(d['names'])
            )
        if kind == 'range':
            return pd.RangeIndex(
                d['start'], d['stop'], d['step'], name=_deserialize_custom(d['name'])
            )
        return pd.Index(
            _deserialize_custom(d['values']), dtype=d['dtype'],
            name=_deserialize_custom(d['name']),
        )


class PandasCategoricalSerializer(CustomSerializer):
    """pandas.Categorical — stored as its categories + integer codes + ordered
    flag, preserving category identity and order exactly (unlike inferring
    categories from the values, which would re-sort them)."""

    @staticmethod
    def serialize(obj):
        return {
            '__py__': get_obj_addr(obj),
            '__pytype__': 'pd_categorical',
            '__data__': {
                'categories': _serialize_custom(obj.categories.tolist()),
                'codes': obj.codes.tolist(),
                'ordered': bool(obj.ordered),
            },
        }

    @staticmethod
    def deserialize(data):
        try:
            import pandas as pd
        except ImportError:
            raise ImportError("Pandas is required for this deserializer.")
        d = data['__data__']
        return pd.Categorical.from_codes(
            d['codes'], categories=_deserialize_custom(d['categories']),
            ordered=d['ordered'],
        )

class ReducerSerializer(CustomSerializer):
    @staticmethod
    def serialize(obj):
        # __reduce_ex__(2), not bare __reduce__(): protocol 2 works for C-level types
        # (complex, datetime, array, ...) and still dispatches to a custom __reduce__.
        # No fallback — where protocol 2 refuses (locks, sockets), bare __reduce__()
        # "succeeds" with a copyreg._reconstructor form that cannot deserialize.
        reduced = obj.__reduce_ex__(2)

        if isinstance(reduced, str):
            # pickle protocol: a string means "look this name up in the module"
            return {
                '__pytype__': 'reducer',
                '__global__': f'{get_obj_module(obj)}.{reduced}',
            }
        if not isinstance(reduced, tuple) or len(reduced) < 2:
            raise ValueError(f"Invalid __reduce__ output for {type(obj)}: {reduced!r}")

        # Every component must recurse through _serialize_custom: reduce output
        # routinely contains bytes/tuples/numpy that raw json.dumps rejects
        result = {
            '__py__': get_obj_addr(reduced[0]),
            '__pytype__': 'reducer',
            '__args__': [_serialize_custom(a) for a in reduced[1]] if reduced[1] else []
        }
        if len(reduced) > 2 and reduced[2] is not None:
            result['__state__'] = _serialize_custom(reduced[2])
        if len(reduced) > 3 and reduced[3] is not None:
            result['__listitems__'] = [_serialize_custom(x) for x in reduced[3]]
        if len(reduced) > 4 and reduced[4] is not None:
            result['__dictitems__'] = [
                [_serialize_custom(k), _serialize_custom(v)] for k, v in reduced[4]
            ]
        if len(reduced) > 5 and reduced[5] is not None:
            result['__state_setter__'] = get_obj_addr(reduced[5])
        return result

    @staticmethod
    def deserialize(data):
        if _safe_mode_active():
            ReducerSerializer._check_safe(data)
        if data.get('__global__'):
            return flexible_import(data['__global__'])

        constructor = flexible_import(data['__py__'])
        args = _deserialize_custom(data['__args__'])
        state = _deserialize_custom(data.get('__state__'))
        listitems = _deserialize_custom(data.get('__listitems__'))
        dictitems = _deserialize_custom(data.get('__dictitems__'))
        state_setter = flexible_import(data.get('__state_setter__')) if data.get('__state_setter__') else None

        obj = constructor(*args)

        if state is not None:
            if state_setter:
                state_setter(obj, state)
            elif hasattr(obj, '__setstate__'):
                _invoke_setstate(obj, state)
            else:
                obj.__dict__.update(state)

        if listitems is not None:
            obj.extend(listitems)

        if dictitems is not None:
            obj.update(dictitems)

        return obj

    @staticmethod
    def _check_safe(data):
        """In safe mode, a reducer may only invoke allowlisted value-type
        constructors ('os.system' is an importable constructor too)."""
        if data.get('__global__'):
            _refuse_unsafe(f"a reducer global reference ({data['__global__']!r})")
        addr = data.get('__py__')
        if addr == 'copyreg.__newobj__':
            # cls.__new__(cls, *args): the class is the first arg — it must
            # itself be an allowlisted value type
            args = data.get('__args__') or []
            first = args[0] if args else None
            cls_addr = first.get('__py__') if isinstance(first, dict) else None
            if cls_addr not in SAFE_REDUCER_CONSTRUCTORS:
                _refuse_unsafe(f"construction of {cls_addr!r} via __newobj__")
        elif addr not in SAFE_REDUCER_CONSTRUCTORS:
            _refuse_unsafe(f"a reducer invoking {addr!r}")
        if data.get('__state_setter__'):
            _refuse_unsafe(f"a reducer state setter ({data['__state_setter__']!r})")

class BytesSerializer(CustomSerializer):
    @staticmethod
    def serialize(obj):
        return {
            '__py__': get_obj_addr(obj),
            '__pytype__': 'bytes',
            '__data__': encode(obj, compress=False, b64=True, as_string=True)
        }

    @staticmethod
    def deserialize(data):
        return decode(data['__data__'], compress=False, b64=True)


class FunctionSerializer(CustomSerializer):
    # @log.info
    @staticmethod
    def serialize(obj, incl_class=True):
        func = unwrap_func(obj)
        obj_d = {}
        obj_d['__py__'] = full_name = get_obj_addr(obj)
        pytype = obj_d['__pytype__'] = get_pytype(obj)
        if pytype == 'classmethod':
            if incl_class:
                obj_d['__cls__'] = ClassSerializer.serialize(get_class_from_method(obj))
                return obj_d
        
        elif pytype == 'instancemethod':
            obj_d['__self__'] = InstanceSerializer.serialize(obj.__self__)
        
        if not can_import_object(full_name) or get_obj_module(obj) == '__main__':
            obj_d['__source__'] =  get_function_src(obj)

        return obj_d
        

    @staticmethod
    @log.debug
    def deserialize(data):
        log.debug(f"Deserializing function: {data['__py__']}")
        
        # If only __py__ is present, try to import the function
        # if len(data) == 1 and '__py__' in data:
        #     obj = flexible_import(data['__py__'])
        #     return classmethod(obj) if data['__pytype__'] == 'classmethod' else obj
        
        pytype = data.get('__pytype__')
        pyaddr = data.get('__py__')
        #pprint(data)
        pyname = pyaddr.split('.')[-1]
        if pytype == 'classmethod':
            cls = ClassSerializer.deserialize(data['__cls__'])
            return getattr(cls, pyname)
        
        if pytype == 'instancemethod':
            obj = InstanceSerializer.deserialize(data['__self__'])
            return getattr(obj, pyname)

        elif can_import_object(pyaddr):
            return flexible_import(pyaddr)

        elif '__source__' in data:
            source = data['__source__']
            func_name = data['__py__'].split('.')[-1]
            return recreate_function_from_src(source, func_name)
        
        else:
            #pprint(data)
            raise Exception('what happened?')
            
        
        return func


def recreate_function_from_src(source, func_name):
    # Handle lambda functions
    if source.startswith('lambda'):
        lambda_expr = source.split(':')[0] + ':' + source.split(':')[1].split(',')[0]
        code = compile(lambda_expr, '<string>', 'eval')
        func = eval(code)
        func.__source__ = source
        return func
    
    code = compile(source, '<string>', 'exec')
    
    try:
        namespace = globals()
        exec(code, namespace)
        func = namespace[func_name]
        func.__source__ = source
        return func

        # closure = get_function_closure(func)    
        # if closure:
        #     func.__closure__ = tuple(cell(v) for v in closure.values())
    except Exception as e:
        log.error(f"Error creating function: {e}")
        raise


class ClassSerializer(CustomSerializer):
    @staticmethod
    def serialize(obj):
        if can_import_object(obj):
            return {'__py__': get_obj_addr(obj), '__pytype__': 'class'}
        
        return {
            '__py__': get_obj_addr(obj),
            '__pytype__': 'class',
            '__bases__': [get_obj_addr(base) for base in obj.__bases__],
            '__methods__': {
                key: FunctionSerializer.serialize(func, incl_class=False) for key,func in obj.__dict__.items()
                if is_function(unwrap_func(func))
            },
            '__attrs__': {
                key: _serialize_custom(attr) for key,attr in obj.__dict__.items()
                if not is_function(unwrap_func(attr)) and not key.startswith('__')
            }

            
            # '__dict__': {
            #     k: _serialize_custom(v) for k, v in obj.__dict__.items()
            #     if not k.startswith('__') or k in {'__init__'}
            # },
            # '__methods__': {
            #     name: get_obj_addr(method) if can_import_object(method) else FunctionSerializer.serialize(method)
            #     for name, method in obj.__dict__.items()
            #     if callable(method) and not name.startswith('__')
            # }
        }

    @staticmethod
    def deserialize(data):
        if can_import_object(data['__py__']):
            return flexible_import(data['__py__'])
        
        bases = tuple(flexible_import(base) for base in data['__bases__'])
        
        # Create a new namespace for the class
        namespace = globals().copy()
        
        # Deserialize and add methods to the namespace
        for name, func_d in data['__methods__'].items():
            namespace[name] = FunctionSerializer.deserialize(func_d)


        # Create the class
        module_path, class_name = data['__py__'].rsplit('.', 1)
        cls = type(class_name, bases, namespace)
        cls.__module__ = module_path
        cls.__qualname__ = class_name
        
        return cls

    #     return cls

class InstanceSerializer(CustomSerializer):
    @staticmethod
    def serialize(obj):
        addr = get_obj_addr(obj)
        cls = obj.__class__
        return {
            '__py__': addr,
            '__pytype__': 'instance',
            '__cls__': ClassSerializer.serialize(cls),
            '__state__': _serialize_custom(obj.__dict__)
        }

    @staticmethod
    def deserialize(data):
        if isinstance(data['__cls__'], dict):
            cls = ClassSerializer.deserialize(data['__cls__'])
        else:
            cls = flexible_import(data['__cls__'])
        
        instance = cls.__new__(cls)
        instance.__dict__.update(_deserialize_custom(data['__state__']))
        return instance

class GeneratorSerializer(CustomSerializer):
    @staticmethod
    def serialize(obj):
        # We'll serialize the generator function and its current state
        return {
            '__py__': get_obj_addr(obj),
            '__pytype__': 'generator',
            '__state__': _serialize_custom(list(obj))  # This consumes the generator
        }

    @staticmethod
    def deserialize(data):
        state = _deserialize_custom(data['__state__'])
        
        def reconstructed_gen():
            yield from state
        
        return reconstructed_gen()

class PathSerializer(CustomSerializer):
    @staticmethod
    def serialize(obj):
        return {
            '__py__': get_obj_addr(obj),
            '__data__': str(obj)
        }

    @staticmethod
    def deserialize(data):
        return Path(data['__data__'])

class ReusableGeneratorSerializer(CustomSerializer):
    @staticmethod
    def serialize(obj):
        return {
            '__py__': get_obj_addr(obj),
            '__func__': FunctionSerializer.serialize(obj.func),
            '__args__': _serialize_custom(obj.args),
            '__kwargs__': _serialize_custom(obj.kwargs)
        }

    @staticmethod
    def deserialize(data):
        func = FunctionSerializer.deserialize(data['__func__'])
        args = _deserialize_custom(data['__args__'])
        kwargs = _deserialize_custom(data['__kwargs__'])
        return ReusableGenerator(func, *args, **kwargs)


def get_function_closure(func):
    if not hasattr(func, '__closure__') or not hasattr(func, '__code__'):
        return None
    obj = func
    closure_dict = {}
    if obj.__closure__:
        for name, cell in zip(obj.__code__.co_freevars, obj.__closure__):
            try:
                if isinstance(cell.cell_contents, types.FunctionType):
                    closure_dict[name] = f"<function {cell.cell_contents.__name__}>"
                elif isinstance(cell.cell_contents, type(obj)):
                    closure_dict[name] = "<self>"
                else:
                    closure_dict[name] = _serialize_custom(cell.cell_contents)
            except ValueError:
                log.debug(f"Empty cell encountered for {name} in function {obj.__name__}")
                closure_dict[name] = None
    return closure_dict if closure_dict else None


CUSTOM_SERIALIZERS = {
    'pandas.core.frame.DataFrame': PandasDataFrameSerializer.serialize,
    'pandas.core.series.Series': PandasSeriesSerializer.serialize,
    # pandas 3.x reports __module__ as 'pandas' (top-level) instead of internal paths
    'pandas.DataFrame': PandasDataFrameSerializer.serialize,
    'pandas.Series': PandasSeriesSerializer.serialize,
    'numpy.ndarray': NumpySerializer.serialize,
    'builtins.set': IterableSerializer.serialize,
    'builtins.tuple': IterableSerializer.serialize,
    'builtins.frozenset': IterableSerializer.serialize,
    'builtins.bytes': BytesSerializer.serialize,
    'types.FunctionType': FunctionSerializer.serialize,
    'types.LambdaType': FunctionSerializer.serialize,
    'type': ClassSerializer.serialize,
    'object': InstanceSerializer.serialize,
    'types.GeneratorType': GeneratorSerializer.serialize,
    'pathlib.PosixPath': PathSerializer.serialize,
    'pathlib.WindowsPath': PathSerializer.serialize,
    # Python 3.13 moved pathlib internals to pathlib._local, so get_obj_addr
    # reports these names; without them Path falls through to the reducer
    # (which safe mode blocks). Data-only PathSerializer works on every version.
    'pathlib._local.PosixPath': PathSerializer.serialize,
    'pathlib._local.WindowsPath': PathSerializer.serialize,
    'hashstash.utils.misc.ReusableGenerator': ReusableGeneratorSerializer.serialize,
}

CUSTOM_DESERIALIZERS = {
    'pandas.core.frame.DataFrame': PandasDataFrameSerializer.deserialize,
    'pandas.core.series.Series': PandasSeriesSerializer.deserialize,
    # pandas 3.x reports __module__ as 'pandas' (top-level) instead of internal paths
    'pandas.DataFrame': PandasDataFrameSerializer.deserialize,
    'pandas.Series': PandasSeriesSerializer.deserialize,
    'numpy.ndarray': NumpySerializer.deserialize,
    'builtins.set': IterableSerializer.deserialize,
    'builtins.tuple': IterableSerializer.deserialize,
    'builtins.frozenset': IterableSerializer.deserialize,
    'builtins.bytes': BytesSerializer.deserialize,
    'function': FunctionSerializer.deserialize,
    'type': ClassSerializer.deserialize,
    'object': InstanceSerializer.deserialize,
    'types.GeneratorType': GeneratorSerializer.deserialize,
    'pathlib.PosixPath': PathSerializer.deserialize,
    'pathlib.WindowsPath': PathSerializer.deserialize,
    'pathlib._local.PosixPath': PathSerializer.deserialize,
    'pathlib._local.WindowsPath': PathSerializer.deserialize,
    'hashstash.utils.misc.ReusableGenerator': ReusableGeneratorSerializer.deserialize,
}

# pandas scalar types (both the internal and pandas-3.x top-level module paths)
for _addr in (
    'pandas._libs.tslibs.timestamps.Timestamp',
    'pandas.Timestamp',
):
    CUSTOM_SERIALIZERS[_addr] = PandasTimestampSerializer.serialize
    CUSTOM_DESERIALIZERS[_addr] = PandasTimestampSerializer.deserialize
for _addr in (
    'pandas._libs.tslibs.timedeltas.Timedelta',
    'pandas.Timedelta',
):
    CUSTOM_SERIALIZERS[_addr] = PandasTimedeltaSerializer.serialize
    CUSTOM_DESERIALIZERS[_addr] = PandasTimedeltaSerializer.deserialize
for _addr in (
    'pandas._libs.tslibs.nattype.NaTType',
    'pandas.NaTType',  # pandas 3.x reports this top-level path
):
    CUSTOM_SERIALIZERS[_addr] = PandasNaTSerializer.serialize
    CUSTOM_DESERIALIZERS[_addr] = PandasNaTSerializer.deserialize
for _addr in (
    'pandas._libs.tslibs.period.Period',
    'pandas.Period',  # pandas 3.x top-level path
):
    CUSTOM_SERIALIZERS[_addr] = PandasPeriodSerializer.serialize
    CUSTOM_DESERIALIZERS[_addr] = PandasPeriodSerializer.deserialize
CUSTOM_SERIALIZERS['zoneinfo.ZoneInfo'] = ZoneInfoSerializer.serialize
CUSTOM_DESERIALIZERS['zoneinfo.ZoneInfo'] = ZoneInfoSerializer.deserialize
for _addr in ('numpy.datetime64', 'numpy.timedelta64'):
    CUSTOM_SERIALIZERS[_addr] = NumpyDatetime64Serializer.serialize
    CUSTOM_DESERIALIZERS[_addr] = NumpyDatetime64Serializer.deserialize
# numpy string/bytes scalars (str_ is a str subclass caught by the primitive
# branch, so its registration is only reachable for bytes_, but both are listed)
for _addr in ('numpy.bytes_', 'numpy.str_'):
    CUSTOM_SERIALIZERS[_addr] = NumpyScalarSerializer.serialize
    CUSTOM_DESERIALIZERS[_addr] = NumpyScalarSerializer.deserialize
# pandas Index family -> one serializer (internal + pandas-3.x top-level paths)
for _addr in (
    'pandas.core.indexes.base.Index', 'pandas.Index',
    'pandas.core.indexes.range.RangeIndex', 'pandas.RangeIndex',
    'pandas.core.indexes.datetimes.DatetimeIndex', 'pandas.DatetimeIndex',
    'pandas.core.indexes.timedeltas.TimedeltaIndex', 'pandas.TimedeltaIndex',
    'pandas.core.indexes.period.PeriodIndex', 'pandas.PeriodIndex',
    'pandas.core.indexes.interval.IntervalIndex', 'pandas.IntervalIndex',
    'pandas.core.indexes.category.CategoricalIndex', 'pandas.CategoricalIndex',
    'pandas.core.indexes.multi.MultiIndex', 'pandas.MultiIndex',
):
    CUSTOM_SERIALIZERS[_addr] = PandasIndexSerializer.serialize
    CUSTOM_DESERIALIZERS[_addr] = PandasIndexSerializer.deserialize
for _addr in ('pandas.core.arrays.categorical.Categorical', 'pandas.Categorical'):
    CUSTOM_SERIALIZERS[_addr] = PandasCategoricalSerializer.serialize
    CUSTOM_DESERIALIZERS[_addr] = PandasCategoricalSerializer.deserialize


# numpy scalar types registered by ADDRESS STRING so that `import hashstash`
# never imports numpy (it stays a zero-dependency import); numpy is imported
# lazily only when a numpy scalar is actually deserialized. Both the old
# ('numpy.bool_') and new ('numpy.bool') names are covered across versions.
# numpy.float64 is intentionally excluded: it subclasses float, so it is stored
# as a plain float (value-preserving) before any custom dispatch sees it.
for _addr in (
    'numpy.int8', 'numpy.int16', 'numpy.int32', 'numpy.int64',
    'numpy.uint8', 'numpy.uint16', 'numpy.uint32', 'numpy.uint64',
    'numpy.longlong', 'numpy.ulonglong', 'numpy.intc', 'numpy.uintc',
    'numpy.intp', 'numpy.uintp',
    'numpy.float16', 'numpy.float32',
    'numpy.complex64', 'numpy.complex128',
    'numpy.bool_', 'numpy.bool',
):
    CUSTOM_SERIALIZERS[_addr] = NumpyScalarSerializer.serialize
    CUSTOM_DESERIALIZERS[_addr] = NumpyScalarSerializer.deserialize
