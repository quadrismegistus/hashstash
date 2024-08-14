import json
import random
from pprint import pprint
import time
import pandas as pd
import tempfile
from filehashcache import Cache
from typing import Literal

DEFAULT_DATA_SIZE = 1_000_000
ENGINE_TYPES = Literal["memory", "file", "sqlite"]
DEFAULT_ENGINE_TYPE = "file"
PROFILE_SIZES = [
    1000,
    10000,
    100000,
    # 1000000,
    # 10000000,
]


class FileHashCacheProfiler:
    root_dir = ".cache_profile"
    engines = ["file", "sqlite"]
    encodings = ["zlib+b64", "zlib", "b64", "raw"]

    @staticmethod
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

    @staticmethod
    def get_encoding_str(compress: bool = True, b64: bool = True):
        encodings = []
        if compress:
            encodings.append("zlib")
        if b64:
            encodings.append("b64")
        return "+".join(encodings) if encodings else "raw"

    @classmethod
    def get_method_str(
        self,
        engine: ENGINE_TYPES = DEFAULT_ENGINE_TYPE,
        compress: bool = True,
        b64: bool = True,
        **kwargs,
    ):
        return f"{engine} ({self.get_encoding_str(compress, b64)})"

    @classmethod
    def profile_cache(
        self,
        engine: ENGINE_TYPES = DEFAULT_ENGINE_TYPE,
        compress: bool = True,
        b64: bool = True,
        size: int = DEFAULT_DATA_SIZE,
        verbose: bool = False,
    ):
        cache = Cache(self.root_dir, engine=engine, compress=compress, b64=b64)
        data = self.generate_data(size)
        raw_size = len(json.dumps(data).encode())

        # Measure write speed
        start_time = time.time()
        cache_key = f"test_data_{size}_{random.random()}"
        cache[cache_key] = data
        write_time = time.time() - start_time

        cached_size = len(cache._encode_cache(data))

        # Measure read speed
        start_time = time.time()
        _ = cache[cache_key]
        read_time = time.time() - start_time

        outd = {
            "Size": size,
            "Method": f"{self.get_method_str(engine,compress,b64)}",
            "Engine": engine,
            "Encoding": self.get_encoding_str(compress, b64),
            "Write Time (s)": write_time,
            "Read Time (s)": read_time,
            "Write Speed (MB/s)": raw_size / write_time / 1024 / 1024,
            "Read Speed (MB/s)": raw_size / read_time / 1024 / 1024,
            "Cached Size (MB)": cached_size / 1024 / 1024,
            "Space Saved (MB)": (raw_size - cached_size) / 1024 / 1024,
            "Space Saved (MB/GB)": (raw_size - cached_size)
            / 1024
            / 1024
            / (raw_size / 1024 / 1024 / 1024),
            "Raw Size (MB)": raw_size / 1024 / 1024,
            "Compression Ratio (%)": cached_size / raw_size * 100,
        }

        if verbose:
            for k in ["Size", "Method", "Read Speed (MB/s)", "Write Speed (MB/s)"]:
                v = outd[k]
                if type(v) is str:
                    print(f"{k}: {v}")
                else:
                    print(f"{k}: {round(v,2)}")
            print()
        return outd

    @classmethod
    def profile(
        self,
        engine=["memory", "file", "sqlite"],
        compress=[True, False],
        b64=[True, False],
        size=PROFILE_SIZES,
        iterations=1,
        verbose: bool = False,
    ):
        results = []
        for sizex in size:
            for compressx in compress:
                for b64x in b64:
                    for enginex in engine:
                        for i in range(iterations):
                            result = self.profile_cache(
                                engine=enginex,
                                compress=compressx,
                                b64=b64x,
                                size=sizex,
                                verbose=verbose,
                            )
                            if iterations > 1:
                                result["run"] = i
                            results.append(result)
        return pd.DataFrame(results)

    @classmethod
    def run_performance_tests(self, iterations=10, **kwargs):
        results = self.profile(verbose=True, iterations=iterations, **kwargs)
        return self.summarize_results(results)

    @staticmethod
    def summarize_results(df):
        print("\nPerformance Statistics:")
        df = df.groupby("Method").mean(numeric_only=True).round(2)
        df = df[
            [
                c
                for c in df
                if c not in {"Size","run"} and not c.endswith("(s)") and not c.endswith("(MB)")
            ]
        ]
        df = df.sort_values(["Write Speed (MB/s)"], ascending=[True])
        return df


if __name__ == "__main__":
    results = FileHashCacheProfiler.run_performance_tests()
    print(results)
