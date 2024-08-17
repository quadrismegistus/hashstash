from ..hashstash import *
from tqdm import tqdm
import pandas as pd
import threading

RAW_SIZE_KEY = "Raw Size (MB)"


class HashStashProfiler:
    def __init__(self, stash):
        self.stash = stash

    # def to_dict(self):
    #     return {"stash": self.stash.to_dict()}

    # def __reduce__(self):
    #     return (self.__class__.from_dict, (self.to_dict(),))

    # @classmethod
    # def from_dict(cls, data: dict):
    #     stash = HashStash(**data["stash"])
    #     return cls(stash)

    
    def profile(
        self,
        size: list = PROFILE_SIZES,
        iterations: list = DEFAULT_ITERATIONS,
        num_proc: int = DEFAULT_NUM_PROC,
        verbose: bool = False,
        progress: bool = True
    ):
        tasks = [{"size": random.choice(size)} for _ in range(iterations)]

        def stream_results(stash):
            write_num = 0
            write_time = 0
            write_size = 0
            timenow = time.time()

            iterr = pmap(
                profile_stash_transaction,
                objects=stash,
                options=tasks,
                num_proc=num_proc,
                ordered=False,
                progress=progress,
                desc=f'Profiling {stash}'
            )
            for result in iterr:
                if not result:
                    continue

                write_num += 1
                timenownow = time.time()
                write_time += timenownow - timenow
                timenow = timenownow

                try:
                    if isinstance(result, Exception):
                        raise result
                    sizenow = result[0][RAW_SIZE_KEY]
                    write_size += sizenow

                    for d in result:
                        d["Num Processes"] = num_proc
                        d["Iteration"] = write_num
                        d["Cumulative Time (s)"] = write_time
                        d["Cumulative Size (MB)"] = write_size
                        yield d
                except Exception as e:
                    print(f"Error processing result: {e}")
                    print(f"Result type: {type(result)}")
                    print(f"Result content: {result}")
                    continue

        with self.stash.tmp() as tmpstash:
            return pd.DataFrame(stream_results(tmpstash))

    @staticmethod
    def _profile_one(args):
        profiler, task = args
        return profiler.profile_one(**task)

    
    def profile_df(
        self,
        *args,
        group_by=GROUPBY,
        sort_by=SORTBY,
        operations={"Read", "Write"},
        df=None,
        **kwargs,
    ):
        df = self.profile(*args, **kwargs) if df is None else df
        if operations:
            df = df[df.Operation.isin(operations)]
        df = pd.concat(
            gdf.sort_values("write_num").assign(
                **{
                    "Cumulative Time (s)": gdf["Time (s)"].cumsum(),
                    "Cumulative Size (MB)": gdf["Size (B)"].cumsum() / 1024 / 1024,
                }
            )
            for g, gdf in df.groupby(
                [x for x in group_by if not x.startswith("write_num")]
            )
        )
        if group_by:
            df = df.groupby(group_by).mean(numeric_only=True).round(4).sort_index()
        if sort_by:
            df = df.sort_values(sort_by, ascending=False)
        return df

def generate_data(size):
    return {
        "string": "".join(
            random.choices("abcdefghijklmnopqrstuvwxyz", k=size // 2)
        ),
        "number": random.randint(1, 1000000),
        "list": [random.randint(1, 1000) for _ in range(size // 20)],
        "nested": {
            f"key_{i}": {"value": random.random()} for i in range(size // 200)
        },
    }


def profile_stash_transaction(
    stash,
    size: int = DEFAULT_DATA_SIZE,
    verbose: bool = False,
):
    cache = stash
    if cache is None:
        raise Exception('Profiler must be used as context manager')
    data = generate_data(size)
    raw_size = len(json.dumps(data).encode())
    cache_key = f"test_data_{size}_{random.random()}"

    # Encode value to get cached size
    encoded_value = cache.encode(data)
    cached_size = len(encoded_value)

    results = []
    common_data = {
        "Engine": cache.engine,
        "Compress": cache.compress,
        "Base64": cache.b64,
        "Size (MB)": int(size) / 1024 / 1024,
        "Raw Size (MB)": raw_size / 1024 / 1024,
        "Cached Size (MB)": cached_size / 1024 / 1024,
        "Compression Ratio (%)": (cached_size / raw_size * 100) if raw_size else 0,
    }

    def add_result(operation, time_taken, additional_data=None):
        result = {
            **common_data,
            "Operation": operation,
            "Time (s)": time_taken,
            "Rate (it/s)": (1 / time_taken) if time_taken else 0,
            "Speed (MB/s)": ((raw_size / time_taken) if time_taken else 0)
            / 1024
            / 1024,
        }
        if additional_data:
            result.update(additional_data)
        results.append(result)

    # Measure key encoding speed
    start_time = time.time()
    encoded_key = cache.encode(cache_key)
    key_encode_time = time.time() - start_time
    add_result("Encode Key", key_encode_time)

    # Measure key decoding speed
    start_time = time.time()
    _ = cache.decode(encoded_key)
    key_decode_time = time.time() - start_time
    add_result("Decode Key", key_decode_time)

    # Measure value encoding speed
    start_time = time.time()
    encoded_value = cache.encode(data)
    value_encode_time = time.time() - start_time
    add_result("Encode Value", value_encode_time)

    # Measure value decoding speed
    start_time = time.time()
    _ = cache.decode(encoded_value)
    value_decode_time = time.time() - start_time
    add_result("Decode Value", value_decode_time)

    # Measure write speed
    start_time = time.time()
    cache[cache_key] = data
    write_time = time.time() - start_time
    add_result("Write", write_time)

    # Add compression data
    # add_result("Compress", value_encode_time)

    # Measure read speed
    start_time = time.time()
    _ = cache[cache_key]
    read_time = time.time() - start_time
    add_result("Read", read_time)

    # Calculate and add raw write and read times
    # raw_write_time = write_time - (key_encode_time + value_encode_time)
    # raw_read_time = read_time - (key_decode_time + value_decode_time)
    # add_result("Raw Write", raw_write_time)
    # add_result("Raw Read", raw_read_time)

    if verbose:
        print(results)
    return results