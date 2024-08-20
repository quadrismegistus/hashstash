from . import *


def get_serializer(serializer: SERIALIZER_TYPES = DEFAULT_SERIALIZER):
    serializers = {
        "custom": serialize_custom,
        "jsonpickle": serialize_jsonpickle,
        "jsonpickle_ext": serialize_jsonpickle_ext,
        "orjson": serialize_orjson,
        "pickle": pickle.dumps,
        "json": json.dumps,
    }
    assert (
        serializer in serializers
    ), f'serializer not one of: {", ".join(serializers.keys())}'
    return serializers.get(serializer)


def get_deserializer(serializer: SERIALIZER_TYPES = DEFAULT_SERIALIZER):
    serializers = {
        "custom": deserialize_custom,
        "jsonpickle": deserialize_jsonpickle,
        "jsonpickle_ext": deserialize_jsonpickle_ext,
        "orjson": deserialize_orjson,
        "pickle": pickle.loads,
        "json": deserialize_json,
    }
    assert (
        serializer in serializers
    ), f'serializer not one of: {", ".join(serializers.keys())}'
    return serializers.get(serializer)


@log.debug
def serialize(obj, serializer: SERIALIZER_TYPES = DEFAULT_SERIALIZER, as_string=False):
    serializer_func = get_serializer(serializer)
    data = serializer_func(obj)
    assert isinstance(data, (bytes, str)), "data should be bytes or string"
    return (
        data.decode("utf-8")
        if as_string and serializer != "pickle" and isinstance(data, bytes)
        else data
    )


@log.debug
def deserialize(data, serializer: SERIALIZER_TYPES = DEFAULT_SERIALIZER):
    deserializer_func = get_deserializer(serializer)
    return deserializer_func(data)