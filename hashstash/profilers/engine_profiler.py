from . import *

RAW_SIZE_KEY = "Raw Size (MB)"

def time_function(func, *args, **kwargs):
    start_time = time.time()
    result = func(*args, **kwargs)
    end_time = time.time()
    return result, end_time - start_time

class HashStashProfiler:
    def __init__(self, stash):
        self.stash = stash

    @stashed_result(name="profile_cache")
    def profile(
        self,
        size: list = PROFILE_SIZES,
        iterations: int = DEFAULT_ITERATIONS,
        num_proc: int = DEFAULT_NUM_PROC,
        verbose: bool = False,
        progress: bool = True
    ):
        tasks = [{"size": random.choice(size)} for _ in range(iterations)]
        results = pmap(
            self.profile_stash_transaction,
            objects=self.stash,
            options=tasks,
            num_proc=num_proc,
            ordered=False,
            progress=progress,
            desc=f'Profiling {self.stash}'
        )
        return pd.DataFrame([{'Iteration':i, **r} for i,result in enumerate(results) if result for r in result])

    def profile_stash_transaction(self, stash, size: int = DEFAULT_DATA_SIZE):
        data = generate_data(size)
        raw_size = bytesize(data)
        cache_key = f"test_data_{size}_{random.random()}"

        results = []
        common_data = {
            "Engine": stash.engine,
            "Compress": stash.compress,
            "Base64": stash.b64,
            "Size (MB)": int(size) / 1024 / 1024,
            "Raw Size (MB)": raw_size / 1024 / 1024,
        }

        def add_result(operation, time_taken, additional_data=None):
            result = {
                **common_data,
                "Operation": operation,
                "Time (s)": time_taken,
                "Rate (it/s)": (1 / time_taken) if time_taken else 0,
                "Speed (MB/s)": ((raw_size / time_taken) if time_taken else 0) / 1024 / 1024,
            }
            if additional_data:
                result.update(additional_data)
            results.append(result)

        operations = [
            ("Encode Key", lambda: stash.encode_key(cache_key)),
            ("Decode Key", lambda: stash.decode_key(stash.encode_key(cache_key))),
            ("Encode Value", lambda: stash.encode_value(data)),
            ("Decode Value", lambda: stash.decode_value(stash.encode_value(data))),
            ("Write", lambda: stash.set(cache_key, data)),
            ("Read", lambda: stash.get(cache_key)),
        ]

        for operation, func in operations:
            _, time_taken = time_function(func)
            add_result(operation, time_taken)

        # Add cached size and compression ratio after write operation
        encoded_value = stash.encode_value(data)
        for d in results:
            d.update({
                "Cached Size (MB)": len(encoded_value) / 1024 / 1024,
                "Compression Ratio (%)": (len(encoded_value) / raw_size * 100) if raw_size else 0,
            })

        return results

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
            gdf.sort_values("Iteration").assign(
                **{
                    "Cumulative Time (s)": gdf["Time (s)"].cumsum(),
                    "Cumulative Size (MB)": gdf["Raw Size (MB)"].cumsum(),
                }
            )
            for g, gdf in df.groupby(
                [x for x in group_by if x != "Iteration"]
            )
        )
        if group_by:
            df = df.groupby(group_by).mean(numeric_only=True).round(4).sort_index()
        if sort_by:
            df = df.sort_values(sort_by, ascending=False)
        return df

def run_engine_profile(iterations=1000, **kwargs):
    with temporary_log_level(logging.WARN):
        results_df = HashStashProfiler(HashStash()).profile(iterations=iterations, **kwargs)
    
    grouped_results = results_df.groupby(['Engine', 'Compress', 'Base64', 'Operation']).agg({
        'Speed (MB/s)': 'median',
        'Time (s)': 'sum',
        'Raw Size (MB)': 'sum',
        'Cached Size (MB)': 'mean',
        'Compression Ratio (%)': 'mean',
    }).reset_index()
    
    return grouped_results.sort_values(['Engine', 'Compress', 'Base64', 'Speed (MB/s)'], ascending=[True, True, True, False])

if __name__ == "__main__":
    run_engine_profile()