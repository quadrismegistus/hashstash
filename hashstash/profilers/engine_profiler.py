from . import *
from ..utils.encodings import encode, decode

RAW_SIZE_KEY = "Raw Size (MB)"
SERIALIZERS = SERIALIZER_TYPES.__args__


def time_function(func, *args, **kwargs):
    start_time = time.time()
    result = func(*args, **kwargs)
    end_time = time.time()
    return result, end_time - start_time


def get_data_type(obj):
    return get_obj_name(obj)


class HashStashProfiler:
    def __init__(self, stash):
        self.stash = stash

    def to_dict(self):
        return {'stash':self.stash.to_dict()}
    
    @classmethod
    def from_dict(cls, d):
        from hashstash import HashStash
        stash = HashStash(**d['stash'])
        return cls(stash)
    
    def __eq__(self, other):
        if not isinstance(other, HashStashProfiler):
            return False
        return self.to_dict() == other.to_dict()
    
    def __repr__(self):
        return f'{self.__class__.__name__}({self.stash})'

    
    # @stashed_dataframe
    def profile(
        self,
        size: list = DEFAULT_DATA_SIZE,
        iterations: int = DEFAULT_ITERATIONS,
        num_proc: int = DEFAULT_NUM_PROC,
        verbose: bool = False,
        progress: bool = True,
        **kwargs,
    ):
        import pandas as pd
        tasks = [{"size": size} for _ in range(iterations)]
        with self.stash.tmp() as tmp_stash:
            results = pmap(
                self.profile_one,
                objects=tmp_stash,
                options=tasks,
                num_proc=num_proc,
                ordered=False,
                progress=progress,
                desc=f"Profiling {tmp_stash}",
            )
            odf = pd.DataFrame(
                [
                    {
                        "Iteration": i,
                        "Num Proc": num_proc,
                        **r,
                    }
                    for i, result in enumerate(results)
                    if result
                    for r in result
                ]
            )
            # gby = [
            #     c
            #     for c in odf.columns
            #     if odf[c].dtype == "object" or odf[c].dtype == "bool"
            # ] + ['Num Proc']
            # odf = odf.groupby(gby).mean(numeric_only=True)  # .reset_index()
            return odf

    def profile_one(self, stash=None, size: int = DEFAULT_DATA_SIZE, data=None):
        stash = self.stash if stash is None else stash
        data = generate_data(size) if data is None else data
        raw_size = bytesize(data)

        def cache_key():
            return f"test_data_{size}_{uuid.uuid4().hex}"

        encoded_key = stash.encode_key(cache_key())
        serialized_data = stash.serialize(data)
        encoded_value = stash.encode_value(serialized_data)

        results = []
        common_data = {
            "Engine": stash.engine,
            "Serializer": stash.serializer,
            "Encoding": get_encoding_str(stash.compress, stash.b64),
            "Data Type": get_data_type(data),
            "Size (MB)": int(size) / 1024 / 1024,
            "Raw Size (MB)": raw_size / 1024 / 1024,
            "Cached Size (MB)": len(encoded_value) / 1024 / 1024,
            "Compression Ratio (%)": (
                (len(encoded_value) / raw_size * 100) if raw_size else 0
            ),
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

        def totalop(stash):
            cachekey = cache_key()
            return stash.set(cachekey, data) and stash.get(cachekey)

        operations = [
            ("Serialize", lambda: stash.serialize(data)),
            ("Deserialize", lambda: stash.deserialize(serialized_data)),
            ("Encode", lambda: stash.encode(serialized_data)),
            ("Decode", lambda: stash.decode(encoded_value)),
            ("Set", lambda: stash.set(cache_key(), data)),
            ("Get", lambda: stash.get(cache_key())),
            ("Total", lambda: totalop(stash)),
        ]

        for operation, func in operations:
            _, time_taken = time_function(func)
            add_result(operation, time_taken)

        return results

    def profile_df(
        self,
        *args,
        group_by=GROUPBY,
        sort_by=SORTBY,
        operations=None,
        df=None,
        **kwargs,
    ):
        import pandas as pd
        df = self.profile(*args, **kwargs) if df is None else df
        if operations:
            df = df[df.Operation.isin(operations)]
        group_by=[x for x in group_by if x in df.columns]
        df = pd.concat(
            gdf.sort_values("Iteration").assign(
                **{
                    "Cumulative Time (s)": gdf["Time (s)"].cumsum(),
                    "Cumulative Size (MB)": gdf["Raw Size (MB)"].cumsum(),
                }
            )
            for g, gdf in df.groupby([x for x in group_by if x != "Iteration"])
        )
        if group_by:
            df = df.groupby(group_by).mean(numeric_only=True).round(4).sort_index()
        if sort_by:
            df = df.sort_values(sort_by, ascending=False)
        return df

    @classmethod
    # @stashed_result(name="profile_results")
    def run_profiles(
        cls,
        iterations=1000,
        size=DEFAULT_DATA_SIZE,
        engines=ENGINES,
        serializers=SERIALIZERS,
        num_procs=[1],
        num_proc=5,
        force=False,
        progress=True,
        progress_inner=False,
        **kwargs,
    ):
        import pandas as pd
        if 'redis' in engines:
            start_redis_server()
        if 'mongo' in engines:
            start_mongo_server()
        opts = []
        for engine in engines:
            for serializer in serializers:
                for compress in [True, False]:
                    for b64 in [True, False]:
                        for numproc in num_procs:
                            opt = {
                                "size": size,
                                "engine": engine,
                                "serializer": serializer,
                                "compress": compress,
                                "b64": b64,
                                "iterations": iterations,
                                "num_proc": numproc,
                                "append_mode": force,
                                "_force": force,
                                "progress": progress_inner,
                                **kwargs,
                            }
                            opts.append(opt)
        random.shuffle(opts)

        @parallelized(num_proc=num_proc, progress=progress)
        def process(opt):
            from hashstash import HashStash

            stash = HashStash(**opt)
            writes = stash.profiler.profile(**opt)
            return writes[0] if isinstance(writes, (list, tuple)) else writes

        out = process(opts)
        return pd.concat(out) if out else pd.DataFrame()


if __name__ == "__main__":
    run_profile()
