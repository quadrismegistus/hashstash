from ..utils import *
from .serialize import _serialize
from .deserialize import _deserialize
from ..utils.encodings import decode, encode

@debug
def serialize_numpy(obj):
    return [obj.tolist()]

def deserialize_numpy(obj):
    import numpy as np
    return np.array(obj)



@debug
def serialize_pandas_df(obj):
    index = [x for x in obj.index.names if x is not None]
    if index:
        obj = obj.reset_index()
    return {
        "values": obj.values.tolist(),
        "columns": obj.columns.tolist(),
        "index_columns": index,
    }


@debug
def deserialize_pandas_df(**obj):
    import pandas as pd
    logger.debug("Deserializing pandas DataFrame")

    values = obj["values"]
    columns = obj["columns"]
    index = obj["index_columns"]
    df = pd.DataFrame(values, columns=columns)
    if index:
        df = df.set_index(index)
    return df




# @debug
# def serialize_pandas_series(obj):
#     return {
#         "values": obj.values.tolist(),
#         "index": obj.index.tolist(),
#         "name": obj.name,
#         "dtype": str(obj.dtype),
#     }

@debug
def serialize_pandas_series(obj, attrs=['name']):
    kwargs = {k:getattr(obj,k) for k in attrs if hasattr(obj,k)}
    return {
        OBJ_ARGS_KEY: [obj.tolist()],
        OBJ_KWARGS_KEY: kwargs
    }



@debug
def deserialize_pandas_series(obj):
    logger.debug("Deserializing pandas Series")
    import pandas as pd

    values = obj["values"]
    index = obj["index"]
    name = obj["name"]
    dtype = obj["dtype"]
    return pd.Series(values, index=index, name=name, dtype=dtype)

@debug
def serialize_bytes(obj):
    return encode(obj, compress=False, b64=True, as_string=True)

@debug
def deserialize_bytes(obj):
    return decode(obj, compress=False, b64=True)

def serialize_set(obj):
    return [_serialize(v) for v in sorted(obj, key=lambda x: str(x))] # ensure pseudo sorted for deterministic output

def serialize_tuple(obj):
    return [_serialize(v) for v in obj]

    
CUSTOM_OBJECT_SERIALIZERS = {
    'pandas.core.frame.DataFrame':serialize_pandas_df,
    'pandas.core.series.Series':serialize_pandas_series,
    'numpy.ndarray':serialize_numpy,
    'builtins.tuple':serialize_tuple,
    'builtins.set':serialize_set,
    'builtins.bytes':serialize_bytes,
}

CUSTOM_OBJECT_DESERIALIZERS = {
    'pandas.core.frame.DataFrame':deserialize_pandas_df,
    # 'pandas.core.series.Series':deserialize_pandas_series,
    'numpy.ndarray':deserialize_numpy,
    # 'builtins.tuple':deserialize_tuple,
    # 'builtins.set':deserialize_set,
    'builtins.bytes':deserialize_bytes,
}

