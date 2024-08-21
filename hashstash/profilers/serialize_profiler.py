from . import *
from ..utils.encodings import encode, decode

SERIALIZERS = SERIALIZER_TYPES.__args__

def time_function(func, *args, **kwargs):
    start_time = time.time()
    result = func(*args, **kwargs)
    end_time = time.time()
    return result, end_time - start_time

def get_data_type(obj):
    addr = get_obj_addr(obj)
    return addr

@stashed_result(store_args=False)
def compare_serializers(obj, recurse=True):
    results = []
    input_size_mb = len(serialize(obj).encode()) / 1024 / 1024
    for name in progress_bar(SERIALIZERS, desc='iterating serializers', progress=False):
        try:
            serialized, serialize_time = time_function(serialize, obj, serializer=name)
            serialized_size = len(serialized) if isinstance(serialized, bytes) else len(serialized.encode('utf-8'))
            
            deserialized, deserialize_time = time_function(deserialize, serialized, serializer=name)

            # Create results for each encoding combination
            for compress in [True, False]:
                for b64 in [True, False]:
                    encoded, encode_time = time_function(encode, serialized, compress=compress, b64=b64)
                    decoded, decode_time = time_function(decode, encoded, compress=compress, b64=b64)
                    encoded_size = len(encoded) if isinstance(encoded, bytes) else len(encoded.encode('utf-8'))

                    # Create the encoding type string
                    encoding_type = []
                    if compress:
                        encoding_type.append("compressed")
                    if b64:
                        encoding_type.append("b64")
                    encoding_type = ".".join(encoding_type) if encoding_type else "raw"

                    results.append({
                        'serializer_name': name,
                        'data_type': get_data_type(obj),
                        'encoding': encoding_type,
                        "serialize_speed": input_size_mb / serialize_time if serialize_time else np.nan,
                        "deserialize_speed": input_size_mb / deserialize_time if deserialize_time else np.nan,
                        'serialize_time': serialize_time,
                        'deserialize_time': deserialize_time,
                        'encode_time': encode_time,
                        'decode_time': decode_time,
                        "encode_serialize_time": encode_time + serialize_time,
                        "decode_deserialize_time": decode_time + deserialize_time,
                        'size_mb': encoded_size / 1024 / 1024,
                        "input_size_mb": input_size_mb,
                    })

        except Exception as e:
            continue
                    
    return results

def run_comparison(size):
    data = generate_data(size)
    return compare_serializers(data)

# @stashed_result
def run_comparisons(iterations=100, num_proc=4, **y):
    results = []
    sizes = [1_000_000 for _ in range(iterations)]
    for res_l in pmap(run_comparison, sizes, num_proc=num_proc):
        results.extend(res_l)
    
    # # pbar=tqdm(range(iterations), desc=f"Iterations", leave=False)
    # for _ in pbar:
    #     size = 1_000_000 #random.randint(1_00, 10_000)
    #     data = generate_data((size)
    #     realsize = len(serialize(data).encode())
    #     # pbar.set_description(f'comparing to data of {realsize/1024:,.2f} KB')
    #     results.extend(compare_serializers(data))
    
    return pd.DataFrame(results)

def run(iterations=1000, **kwargs):
    with temporary_log_level(logging.WARN):
        results_df = run_comparisons(iterations=iterations, **kwargs)
    
    # Group by serializer_name, data_type, and encoding
    grouped_results = results_df.groupby(['data_type', 'serializer_name', 'encoding']).agg({
        'serialize_speed': 'median',
        'deserialize_speed': 'median',
        'serialize_time': 'sum',
        'deserialize_time': 'sum',
        'encode_time': 'sum',
        'decode_time': 'sum',
        'input_size_mb': 'sum',
        'size_mb': 'sum',
    }).reset_index()
    
    # Sort by data_type, encoding, and serialize_speed
    return grouped_results.sort_values(['data_type', 'encoding', 'serialize_speed'], ascending=[True, True, False])

if __name__ == "__main__":
    run()