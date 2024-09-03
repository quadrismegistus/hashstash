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