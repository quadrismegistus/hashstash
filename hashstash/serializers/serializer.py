from . import *
import functools

def get_serializer(serializer: SERIALIZER_TYPES = DEFAULT_SERIALIZER):
    serializer_dict = {
        "hashstash": serialize_custom,
        "jsonpickle": serialize_jsonpickle,
        "jsonpickle_ext": serialize_jsonpickle_ext,
        "orjson": serialize_orjson,
        "pickle": serialize_pickle,
        "json": serialize_json,
    }
    
    return serializer_dict.get(serializer)

def get_deserializer(serializer: SERIALIZER_TYPES = DEFAULT_SERIALIZER):
    deserializer_dict = {
        "hashstash": deserialize_custom,
        "jsonpickle": deserialize_jsonpickle,
        "jsonpickle_ext": deserialize_jsonpickle_ext,
        "orjson": deserialize_orjson,
        "pickle": deserialize_pickle,
        "json": deserialize_json,
    }
    
    return deserializer_dict.get(serializer)

# @log.debug
def serialize(obj, serializer: SERIALIZER_TYPES = None, as_string=False):
    if serializer is None:
        serializer = config.serializer
    serializer_func = get_serializer(serializer)
    if serializer_func is None:
        raise ValueError(f"Invalid serializer: {serializer}")
    
    # log.debug(f"Attempting to serialize with {serializer_func.__name__}")
    try:
        data = serializer_func(obj)
        assert isinstance(data, (bytes, str)), "data should be bytes or string"
        # log.debug(f"Serialized with {serializer_func.__name__}")
        return data.decode() if isinstance(data, bytes) and as_string else data
    except Exception as e:
        log.warning(f"Serialization failed with {serializer_func.__name__}: {str(e)}")
        raise e

# @log.debug
def deserialize(data, serializer: SERIALIZER_TYPES = None):
    if serializer is None:
        serializer = config.serializer
    deserializer_func = get_deserializer(serializer)
    if deserializer_func is None:
        raise ValueError(f"Invalid deserializer: {serializer}")
    
    log.debug(f"Attempting to deserialize with {deserializer_func.__name__}")
    try:
        odata = deserializer_func(data)
        log.debug(f"Deserialized with {deserializer_func.__name__}")
        return odata
    except Exception as e:
        log.warning(f"Deserialization failed with {deserializer_func.__name__}: {str(e)}")
        raise e

@fcache
def get_working_serializers():
    working_serializers = []
    for serializer in SERIALIZER_TYPES.__args__:
        try:
            serializer_func = get_serializer(serializer)
            data = serializer_func('test')
            assert isinstance(data, (bytes, str)), "data should be bytes or string"
            working_serializers.append(serializer)
        except Exception as e:
            log.warning(f"Serialization failed with {serializer}: {str(e)}")
    
    return working_serializers

def serialize_pickle(obj):
    return pickle.dumps(obj)

def deserialize_pickle(data):
    return pickle.loads(data)

def bytesize(obj):
    return len(serialize(obj).encode())