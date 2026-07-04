# Explicit stdlib imports: this package's `from . import *` chains are
# circular, and whether a name has landed in the package namespace yet
# depends on import order (spawn workers + editable installs order imports
# differently). Never rely on the star-chain for stdlib names.
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


# NOTE: serialize_custom / _serialize_custom are intentionally NOT decorated
# with @log.debug. _serialize_custom recurses once per node of the value tree
# (millions of calls on a large payload), and the log-wrapper's per-call
# overhead dominated ~20% of serialization time even with logging disabled
# (measured 1.5x speedup from removing it). Tracing lives at the serialize()/
# deserialize() boundary in serializer.py, which fires once per operation.
def serialize_custom(obj: Any, sort_keys: bool = False) -> str:
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


    if isinstance(obj, (str, int, float, bool)):
        return obj

    if isinstance(obj, dict):
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
    return _deserialize_custom(_loads_any(serialized_str))

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

class MetaDataFrameSerializer(CustomSerializer):
    @staticmethod
    def serialize(obj):
        return {
            '__py__': get_obj_addr(obj),
            '__data__': obj.stuff()
        }
        # data = obj.to_dict()
        # data['data'] = PandasDataFrameSerializer.serialize(data['data'])
        # return {
        #     '__py__': get_obj_addr(obj),
        #     '__data__': data
        # }
    
    @staticmethod
    def deserialize(data):
        data = data['__data__']
        return MetaDataFrame.unstuff(data)
        # data['data'] = PandasDataFrameSerializer.deserialize(data['data'])
        # return MetaDataFrame(**data)

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
    @staticmethod
    def serialize(obj):
        assert pandas_installed(), "Pandas is required for this serializer."
        if pandas_extension_activated():
            # log.debug("serializing MetaDataFrame")
            mdf = MetaDataFrame(obj)
            return {
                '__py__': get_obj_addr(obj),
                '__data__': MetaDataFrameSerializer.serialize(mdf)
            }
        else:
            df, index = reset_index_misc(obj, _index=True)
            return {
                '__py__': get_obj_addr(obj),
                '__data__': {
                    'values': NumpySerializer.serialize(df.values),
                    'columns': NumpySerializer.serialize(df.columns.values),
                    'index_columns': _serialize_custom(index),
                    'dtypes': _serialize_custom({k:str(v) for k,v in df.dtypes.to_dict().items()})
                }
            }

    @staticmethod
    def deserialize(data):
        assert pandas_installed(), "Pandas is required for this serializer."

        if pandas_extension_activated():
            # log.info("deserializing MetaDataFrame")
            mdf = MetaDataFrameSerializer.deserialize(data['__data__'])
            # log.debug(f"deserialized MetaDataFrame with shape {mdf.data.shape}")
            return mdf.data
        else:
            import pandas as pd
            values = NumpySerializer.deserialize(data['__data__']['values'])
            columns = NumpySerializer.deserialize(data['__data__']['columns'])
            index_columns = _deserialize_custom(data['__data__'].get('index_columns'))
            dtypes = _deserialize_custom(data['__data__']['dtypes'])
            
            df = pd.DataFrame(values, columns=columns)
            for col, dtype in dtypes.items():
                df[col] = df[col].astype(dtype)
            
            if index_columns:
                df = df.set_index(index_columns)
            if index_columns == ['_index']:
                df = df.rename_axis(None)
            return df

class NumpySerializer(CustomSerializer):
    @staticmethod
    def serialize(obj):
        outd = {
            '__py__': get_obj_addr(obj),
            '__data__': {
                'dtype': str(obj.dtype),
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
        dtype = data['__data__']['dtype']
        shape = data['__data__']['shape']
        if 'bytes' in data['__data__']:
            arr_bytes = decode(data['__data__']['bytes'], compress=False, b64=True)
            return np.frombuffer(arr_bytes, dtype=dtype).reshape(shape)
        else:
            return np.array([_deserialize_custom(item) for item in data['__data__']['values']], dtype=dtype).reshape(shape)

class PandasSeriesSerializer(CustomSerializer):
    @staticmethod
    def serialize(obj):
        return {
            '__py__': get_obj_addr(obj),
            '__data__': {
                'values': NumpySerializer.serialize(obj.values),
                'index': NumpySerializer.serialize(obj.index.values)
            }
        }

    @staticmethod
    def deserialize(data):
        try:
            import pandas as pd
        except ImportError:
            raise ImportError("Pandas is required for this deserializer.")
        return pd.Series(
            NumpySerializer.deserialize(data['__data__']['values']),
            index=NumpySerializer.deserialize(data['__data__']['index'])
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
    'hashstash.utils.dataframes.MetaDataFrame': MetaDataFrameSerializer.serialize,
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
    'hashstash.utils.dataframes.MetaDataFrame': MetaDataFrameSerializer.deserialize,
}
