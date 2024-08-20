from . import *
from .serialize import _serialize

@log.debug
def serialize_numpy(obj):
    out=encode(obj.tobytes(), compress=False, b64=True, as_string=True)
    return {
        OBJ_ARGS_KEY:[out],
        OBJ_KWARGS_KEY:{
            'dtype':str(obj.dtype)
        }
    }

def deserialize_numpy(obj_b, dtype=None):
    import numpy as np
    arr_bytes = decode(obj_b, compress=False, b64=True)
    dtype = np.dtype(dtype) if dtype else None
    return np.frombuffer(arr_bytes, dtype=dtype)



# @log.debug
# def serialize_pandas_df(obj):
#     index = [x for x in obj.index.names if x is not None]
#     if index:
#         obj = obj.reset_index()
#     return {
#         "values": obj.values.tolist(),
#         "columns": obj.columns.tolist(),
#         "index_columns": index,
#     }

@log.debug
def serialize_pandas_df(obj):
    index = [x for x in obj.index.names if x is not None]
    if index:
        obj = obj.reset_index()
    return {
        OBJ_ARGS_KEY: [obj.values.tolist()],
        OBJ_KWARGS_KEY: {
            'columns': obj.columns.tolist(),
            'index_columns': index
        }
    }

@log.debug
def deserialize_pandas_df(data, *args, columns=None, index_columns=None, **kwargs):
    import pandas as pd
    log.debug("Deserializing pandas DataFrame")
    df = pd.DataFrame(data)
    if columns:
        df.columns = columns
    if index_columns:
        df = df.set_index(index_columns)
    return df




@log.debug
# def serialize_pandas_series(obj):
#     return {
#         "values": obj.values.tolist(),
#         "index": obj.index.tolist(),
#         "name": obj.name,
#         "dtype": str(obj.dtype),
#     }

@log.debug
def serialize_pandas_series(obj, attrs=['name']):
    kwargs = {k:getattr(obj,k) for k in attrs if hasattr(obj,k)}
    return {
        OBJ_ARGS_KEY: [obj.tolist()],
        OBJ_KWARGS_KEY: kwargs
    }



@log.debug
def deserialize_pandas_series(obj):
    log.debug("Deserializing pandas Series")
    import pandas as pd

    values = obj["values"]
    index = obj["index"]
    name = obj["name"]
    dtype = obj["dtype"]
    return pd.Series(values, index=index, name=name, dtype=dtype)

@log.debug
def serialize_bytes(obj):
    return _encode(obj, compress=False, b64=True, as_string=True)

@log.debug
def deserialize_bytes(obj):
    return _decode(obj, compress=False, b64=True)

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

