from . import *
import functools



def get_serializer(serializer: Union[SERIALIZER_TYPES, List[SERIALIZER_TYPES]] = DEFAULT_SERIALIZER):
    serializer_dict = {
        "custom": serialize_custom,
        "jsonpickle": serialize_jsonpickle,
        "jsonpickle_ext": serialize_jsonpickle_ext,
        "orjson": serialize_orjson,
        "pickle": serialize_pickle,
        "json": serialize_json,
    }
    
    if isinstance(serializer, str):
        serializer = [serializer]
    
    return [serializer_dict[s] for s in serializer if s in serializer_dict]

def get_deserializer(serializer: Union[SERIALIZER_TYPES, List[SERIALIZER_TYPES]] = SERIALIZER_TYPES.__args__):
    deserializer_dict = {
        "custom": deserialize_custom,
        "jsonpickle": deserialize_jsonpickle,
        "jsonpickle_ext": deserialize_jsonpickle_ext,
        "orjson": deserialize_orjson,
        "pickle": deserialize_pickle,
        "json": deserialize_json,
    }
    
    if isinstance(serializer, str):
        serializer = [serializer]
    
    return [deserializer_dict[s] for s in serializer if s in deserializer_dict]


@log.debug
def serialize(obj, serializer: Union[SERIALIZER_TYPES, List[SERIALIZER_TYPES]] = None, as_string=False):
    if serializer is None:
        serializer = config.serializer
    serializer_funcs = get_serializer(serializer)
    for serializer_func in serializer_funcs:
        log.debug(f"Attempting to serialize with {serializer_func.__name__}")
        try:
            data = serializer_func(obj)
            assert isinstance(data, (bytes, str)), "data should be bytes or string"
            log.debug(f"Serialized with {serializer_func.__name__}")
            return (
                data.decode() if isinstance(data, bytes) and as_string else data
            )
        except Exception as e:
            log.warning(f"Serialization failed with {serializer_func.__name__}: {str(e)}")
    
    raise ValueError("All serialization attempts failed")


@log.debug
def deserialize(data, serializer: Union[SERIALIZER_TYPES, List[SERIALIZER_TYPES]] = None):
    if serializer is None:
        serializer = config.serializer
    deserializer_funcs = get_deserializer(serializer)
    for deserializer_func in deserializer_funcs:
        log.debug(f"Attempting to deserialize with {deserializer_func.__name__}")
        try:
            odata = deserializer_func(data)
            if type(odata) is dict and '__py__' in odata and deserializer_func.__name__ != 'serialize_custom':
                continue
            log.debug(f"Deserialized with {deserializer_func.__name__}")
            return odata
        except Exception as e:
            log.warning(f"Deserialization failed with {deserializer_func.__name__}: {str(e)}")
    
    raise ValueError("All deserialization attempts failed")

@fcache
def get_working_serializers_set():
    working_serializers = []
    serializer_funcs = get_serializer(list(SERIALIZER_TYPES.__args__))
    
    for serializer_func in serializer_funcs:
        try:
            data = serializer_func('test')
            assert isinstance(data, (bytes, str)), "data should be bytes or string"
            working_serializers.append(serializer_func.__name__.replace('serialize_', ''))
        except Exception as e:
            log.warning(f"Serialization failed with {serializer_func.__name__}: {str(e)}")
            pass
    
    return set(working_serializers)

def get_working_serializers(serializer: Union[SERIALIZER_TYPES, List[SERIALIZER_TYPES]] = SERIALIZER_TYPES.__args__):
    return [x for x in ([serializer] if isinstance(serializer,str) else serializer) if x in get_working_serializers_set()]

def serialize_pickle(obj):
    return pickle.dumps(obj)

def deserialize_pickle(data):
    return pickle.loads(data)

def bytesize(obj, serializer='custom'):
    return len(serialize(obj, serializer).encode())