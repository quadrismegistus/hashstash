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

def deserialize_json(obj):
    return json.loads(obj)

def deserialize_orjson(obj):
    import orjson
    return orjson.loads(obj)

def serialize_jsonpickle(obj):
    import jsonpickle
    _unregister_jsonpickle_handlers()
    return jsonpickle.dumps(obj)


def serialize_jsonpickle_ext(obj):
    import jsonpickle
    _register_jsonpickle_handlers()
    return jsonpickle.dumps(obj)

def deserialize_jsonpickle(obj):
    import jsonpickle
    _unregister_jsonpickle_handlers()
    return jsonpickle.loads(obj)

def deserialize_jsonpickle_ext(obj):
    import jsonpickle
    _register_jsonpickle_handlers()
    return jsonpickle.loads(obj)



def _register_jsonpickle_handlers():
    import jsonpickle
    jsonpickle.set_encoder_options('json', sort_keys=True)
    global _jsonpickle_handlers_registered

    if not _jsonpickle_handlers_registered:
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