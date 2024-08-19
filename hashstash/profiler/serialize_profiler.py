from .profiler import *
import json
import jsonpickle
import pickle
import numpy as np
import pandas as pd
import jsonpickle.ext.numpy as jsonpickle_numpy
import jsonpickle.ext.pandas as jsonpickle_pandas
from ..serialize import serialize, deserialize

# Register the NumPy extension
jsonpickle_numpy.register_handlers()

# Register the Pandas extension
jsonpickle_pandas.register_handlers()

def time_function(func, *args, **kwargs):
    start_time = time.time()
    result = func(*args, **kwargs)
    end_time = time.time()
    return result, end_time - start_time

SERIALIZERS = {
    'serialize': serialize,
    'jsonpickle': jsonpickle.encode,
    'pickle': pickle.dumps,
    # 'json':json.dumps
}

DESERIALIZERS = {
    'serialize': deserialize,
    'jsonpickle': jsonpickle.decode,
    'pickle': pickle.loads,
    # 'json':json.loads
}

def compare_serializers(obj):
    results = []
    for name, func in SERIALIZERS.items():
        serialized, serialize_time = time_function(func, obj)
        serialized_size = len(serialized) if isinstance(serialized, bytes) else len(serialized.encode('utf-8'))
        
        deserialized, deserialize_time = time_function(DESERIALIZERS[name], serialized)
        
        results.append({
            'serializer_name': name,
            'serialize_time': serialize_time,
            'deserialize_time': deserialize_time,
            'size_mb': serialized_size / 1024 / 1024
        })
    
    return results

@cached_result
def run_comparisons(iterations=100, **y):
    results = []
    for _ in tqdm(range(iterations), desc=f"Iterations", leave=False):
        size = random.randint(10, 1_000_000)
        data = generate_data(size)
        for res in compare_serializers(data):
            comparison = {'input_size_mb': size / 1024 / 1024, **res}
            results.append(comparison)
    
    return pd.DataFrame(results)

def run(iterations=1000):
    with temporary_log_level(logging.WARN):
        results_df = run_comparisons(iterations=iterations)
    analyze_results(results_df)
    return results_df

if __name__ == "__main__":
    run()