from .profiler import *
import orjson
import json
import jsonpickle
import pickle
import numpy as np
import pandas as pd
import jsonpickle.ext.numpy as jsonpickle_numpy
import jsonpickle.ext.pandas as jsonpickle_pandas
from ..serialize import serialize, deserialize

def register_handlers():
    jsonpickle_numpy.register_handlers()
    jsonpickle_pandas.register_handlers()

def unregister_handlers():
    jsonpickle_numpy.unregister_handlers()
    jsonpickle_pandas.unregister_handlers()

SERIALIZERS = {
    'serialize': serialize,
    'jsonpickle': jsonpickle.encode,
    'jsonpickle_ext': lambda obj: jsonpickle.encode(obj),
    'pickle': pickle.dumps,
    'json': json.dumps,
    'orjson': partial(orjson.dumps, option=orjson.OPT_SORT_KEYS | orjson.OPT_SERIALIZE_NUMPY | orjson.OPT_SERIALIZE_DATACLASS),
}

DESERIALIZERS = {
    'serialize': deserialize,
    'jsonpickle': jsonpickle.decode,
    'jsonpickle_ext': jsonpickle.decode,
    'pickle': pickle.loads,
    'json': json.dumps,
    'orjson': orjson.loads
}

def time_function(func, *args, **kwargs):
    start_time = time.time()
    result = func(*args, **kwargs)
    end_time = time.time()
    return result, end_time - start_time

def get_data_type(obj):
    addr = get_obj_addr(obj)
    return addr
    # return 'builtins' if addr.split('.')[0]=='builtins' else addr#.split('.')[-1]

@log.debug
def compare_serializers(obj, recurse=True):
    results = []
    input_size_mb = len(serialize(obj).encode()) / 1024 / 1024
    for name, func in SERIALIZERS.items():
        if name == 'jsonpickle_ext':
            register_handlers()
        elif name == 'jsonpickle':
            unregister_handlers()
        
        try:
            serialized, serialize_time = time_function(func, obj)
            serialized_size = len(serialized) if isinstance(serialized, bytes) else len(serialized.encode('utf-8'))
            
            deserialized, deserialize_time = time_function(DESERIALIZERS[name], serialized)
        except Exception as e:
            # logger.warning(e)
            continue

        results.append({
            'serializer_name': name,
            # 'data_type': data_type,
            'data_type': get_data_type(obj),# if not recurse else 'complex',
            "serialize_speed": input_size_mb / serialize_time if serialize_time else np.nan,
            "deserialize_speed": input_size_mb / deserialize_time if deserialize_time else np.nan,
            'serialize_time': serialize_time,
            'deserialize_time': deserialize_time,
            'size_mb': serialized_size / 1024 / 1024,
            "input_size_mb":input_size_mb
        })
        
        # if recurse:
        #     if type(obj) is list:
        #         for subobj in obj:
        #             results.extend(compare_serializers(subobj,recurse=False))
        #     elif type(obj) is dict:
        #         for key, value in obj.items():
        #             results.extend(compare_serializers(value, recurse=False))
                    
    return results

@cached_result
def run_comparisons(iterations=100, **y):
    results = []
    pbar=tqdm(range(iterations), desc=f"Iterations", leave=False)
    for _ in pbar:
        size = 1_000_000 #random.randint(1_00, 10_000)
        data = generate_data_simple(size)
        realsize = len(serialize(data).encode())
        pbar.set_description(f'comparing to data of {realsize/1024:,.2f} KB')
        results.extend(compare_serializers(data))
    
    return pd.DataFrame(results)

def run(iterations=1000, **kwargs):
    with temporary_log_level(logging.WARN):
        results_df = run_comparisons(iterations=iterations, **kwargs)
    
    # Group by serializer_name and data_type
    grouped_results = results_df.groupby(['data_type', 'serializer_name']).agg({
    # grouped_results = results_df.groupby(['serializer_name']).agg({
        'serialize_speed': 'median',
        'deserialize_speed': 'median',
        'serialize_time': 'sum',
        'deserialize_time': 'sum',
        'input_size_mb': 'sum',
        'size_mb': 'sum',
    }).reset_index()
    
    # Sort by serialize_time
    return grouped_results.sort_values(['data_type', 'serialize_speed'], ascending=[True,False])
    # return grouped_results.sort_values(['serialize_speed'], ascending=[False])

if __name__ == "__main__":
    run()