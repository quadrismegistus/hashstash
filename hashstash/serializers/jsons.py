# Explicit stdlib imports: this package's `from . import *` chains are
# circular, and whether a name has landed in the package namespace yet
# depends on import order (spawn workers + editable installs order imports
# differently). Never rely on the star-chain for stdlib names.
import datetime
import json
import pickle

from . import *

# try:
#     import jsonpickle
#     jsonpickle.set_encoder_options('json', sort_keys=True)
# except ImportError:
#     pass


_jsonpickle_handlers_registered = False

def serialize_orjson(obj):
    import orjson
    return orjson.dumps(obj, option=orjson.OPT_SORT_KEYS | orjson.OPT_SERIALIZE_NUMPY | orjson.OPT_SERIALIZE_DATACLASS)

def serialize_json(obj):
    return json.dumps(obj,sort_keys=True)

def serialize_json_fast(obj):
    try:
        return serialize_orjson(obj)
    except ImportError as e:
        return serialize_json(obj)

def deserialize_json(obj):
    return json.loads(obj)


def deserialize_orjson(obj):
    import orjson
    return orjson.loads(obj)

def remove_mdfs(obj):
    if isinstance(obj,MetaDataFrame): return obj.df
    if isinstance(obj,list): return [remove_mdfs(i) for i in obj]
    if isinstance(obj,dict): return {remove_mdfs(k):remove_mdfs(v) for k,v in obj.items()}
    return obj

def serialize_pickle(obj):
    return pickle.dumps(obj, protocol=pickle.HIGHEST_PROTOCOL)

def deserialize_pickle(data):
    return pickle.loads(data)


def serialize_msgpack(obj):
    import msgpack
    # msgpack is DATA-only — it cannot encode functions/classes, which is
    # exactly why it is safe to deserialize. datetime=True keeps native
    # timestamps; use_bin_type distinguishes bytes from str.
    return msgpack.packb(obj, datetime=True, use_bin_type=True)

def deserialize_msgpack(data):
    import msgpack
    if isinstance(data, str):
        data = data.encode("utf-8")
    return msgpack.unpackb(data, timestamp=3, raw=False, strict_map_key=False)


def serialize_cbor2(obj):
    import cbor2
    # cbor2 is DATA-only — it cannot encode functions/classes, which is
    # exactly why it is safe to deserialize. timezone=... / datetime encoding
    # is native in cbor2 (datetimes become CBOR tag 0/1).
    return cbor2.dumps(obj, datetime_as_timestamp=True, timezone=datetime.timezone.utc)

def deserialize_cbor2(data):
    import cbor2
    if isinstance(data, str):
        data = data.encode("utf-8")
    return cbor2.loads(data)


def serialize_jsonpickle(obj):
    import jsonpickle
    _register_jsonpickle_handlers()
    # obj = remove_mdfs(obj)
    return jsonpickle.dumps(obj)

def deserialize_jsonpickle(obj):
    import jsonpickle
    _register_jsonpickle_handlers()
    return jsonpickle.loads(obj)


def _register_jsonpickle_handlers():
    import jsonpickle
    from ..utils.dataframes import MetaDataFrame

    jsonpickle.set_encoder_options('json', sort_keys=True)
    global _jsonpickle_handlers_registered

    class MetaDataFrameJSONHandler(jsonpickle.handlers.BaseHandler):
        pickler = jsonpickle.Pickler()
        unpickler = jsonpickle.Unpickler()
        
        
        def flatten(self, obj, data):
            data = {**data, **obj.to_dict()}
            data['data'] = self.pickler.flatten(obj.data)
            return data

        def restore(self, data):
            data['data'] = self.unpickler.restore(data['data'])
            return MetaDataFrame.from_dict(data)

    if not _jsonpickle_handlers_registered:
        jsonpickle.handlers.register(MetaDataFrame, MetaDataFrameJSONHandler)
        import jsonpickle.ext.numpy as jsonpickle_numpy
        import jsonpickle.ext.pandas as jsonpickle_pandas
        jsonpickle_numpy.register_handlers()
        jsonpickle_pandas.register_handlers()
        _jsonpickle_handlers_registered = True

def _unregister_jsonpickle_handlers():
    import jsonpickle
    jsonpickle.set_encoder_options('json', sort_keys=True)
    global _jsonpickle_handlers_registered
    if _jsonpickle_handlers_registered:
        try:
            import jsonpickle.ext.numpy as jsonpickle_numpy
            jsonpickle_numpy.unregister_handlers()
        except ImportError:
            pass

        try:
            import jsonpickle.ext.pandas as jsonpickle_pandas
            jsonpickle_pandas.unregister_handlers()
        except ImportError:
            pass

        _jsonpickle_handlers_registered = False