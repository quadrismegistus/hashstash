import json
import random
from pprint import pprint
import time
import pandas as pd
import tempfile
from filehashcache import Cache
from typing import Literal
import multiprocessing as mp
from concurrent.futures import ProcessPoolExecutor

DEFAULT_DATA_SIZE = 1_000_000
ENGINES = ["memory", "file", "sqlite", "shelve"]
ENGINE_TYPES = Literal["memory", "file", "sqlite", "shelve"]
DEFAULT_ENGINE_TYPE = "file"
INITIAL_SIZE=1024
PROFILE_SIZES = []
for n in range(5):
    PROFILE_SIZES.append(
        INITIAL_SIZE if not PROFILE_SIZES else PROFILE_SIZES[-1]*2
    )
    
DEFAULT_ITERATIONS = 10
GROUPBY=["Engine","Encoding","Method"]
SORTBY='Write Speed (MB/s)'
DEFAULT_INDEX=['Encoding','Engine']

class FileHashCacheProfiler:
    root_dir = ".cache_profile"
    
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
        root_dir: str = '.cache_profile',
        engine: ENGINE_TYPES = DEFAULT_ENGINE_TYPE,
        compress: bool = True,
        b64: bool = True,
        size: int = DEFAULT_DATA_SIZE,
        verbose: bool = False,
    ):
        cache = Cache(root_dir, engine=engine, compress=compress, b64=b64)
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
            "Size (KB)": int(size / 1000),
            "Encoding": self.get_encoding_str(compress, b64),
            "Engine": engine,
            "Method": f"{self.get_method_str(engine,compress,b64)}",
            "Write Speed (MB/s)": raw_size / write_time / 1024 / 1024,
            "Read Speed (MB/s)": raw_size / read_time / 1024 / 1024,
            "Space Saved (MB/GB)": (raw_size - cached_size)
            / 1024
            / 1024
            / (raw_size / 1024 / 1024 / 1024),
            "Write Time (s)": write_time,
            "Read Time (s)": read_time,
            "Raw Size (MB)": raw_size / 1024 / 1024,
            "Cached Size (MB)": cached_size / 1024 / 1024,
            "Space Saved (MB)": (raw_size - cached_size) / 1024 / 1024,
            "Compression Ratio (%)": cached_size / raw_size * 100,
        }

        if verbose:
            print(outd)
        return outd

    @classmethod
    def profile_cache_parallel(cls, args):
        return cls.profile_cache(**args)

    @classmethod
    def profile(
        self,
        engine=ENGINES,
        compress=[True, False],
        b64=[True, False],
        size=PROFILE_SIZES,
        iterations=DEFAULT_ITERATIONS,
        verbose: bool = False,
        num_proc: int = None,
        group_by=GROUPBY,
        sort_by=SORTBY
    ):
        with tempfile.TemporaryDirectory() as root_dir:
            results = []
            tasks = []
            for sizex in size:
                for compressx in compress:
                    for b64x in b64:
                        for enginex in engine:
                            for i in range(iterations):
                                tasks.append(
                                    {
                                        "root_dir": root_dir,
                                        "engine": enginex,
                                        "compress": compressx,
                                        "b64": b64x,
                                        "size": sizex,
                                        "verbose": verbose,
                                    }
                                )

            if num_proc == None: num_proc = mp.cpu_count() - 1 if mp.cpu_count()>1 else 1
            if num_proc > 1:
                results=[]
                for numproc in range(1,num_proc-1,2):
                    print(numproc)
                    with ProcessPoolExecutor(max_workers=numproc) as executor:
                        proc = executor.map(self.profile_cache_parallel, tasks)
                    for result in proc:
                        result['Num Processes']=numproc
                        results.append(result)
            else:
                results = [self.profile_cache(**task) for task in tasks]

            df = pd.DataFrame(results).set_index(DEFAULT_INDEX)
            if group_by:
                df = df.groupby(group_by).mean(numeric_only=True).round(2).sort_index()
            if sort_by:
                df = df.sort_values(sort_by, ascending=False)
            if group_by == 'Method' or group_by == ["Method"]:
                df = df[[c for c in df if '/' in c]]
            
            
            
            
            return df

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Profile FileHashCache performance")
    parser.add_argument("--engine", nargs="+", default=ENGINES, 
                        help="Engine types to profile (default: memory file sqlite)")
    parser.add_argument("--compress", nargs="+", type=lambda x: x.lower() == 'true', default=[True, False], 
                        help="Compression options (default: True False)")
    parser.add_argument("--b64", nargs="+", type=lambda x: x.lower() == 'true', default=[True, False], 
                        help="Base64 encoding options (default: True False)")
    parser.add_argument("--size", nargs="+", type=int, default=PROFILE_SIZES, 
                        help=f"Data sizes to profile (default: {PROFILE_SIZES})")
    parser.add_argument("--iterations", type=int, default=DEFAULT_ITERATIONS, 
                        help=f"Number of iterations for each configuration (default: {DEFAULT_ITERATIONS})")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose output")
    parser.add_argument("--num-proc", type=int, default=None, 
                        help="Number of processes to use (default: CPU count - 1)")
    parser.add_argument("--group-by", nargs="+", default=GROUPBY, 
                        help=f"Columns to group results by (default: {GROUPBY})")
    parser.add_argument("--sort-by", default=SORTBY, 
                        help=f"Column to sort results by (default: {SORTBY})")

    args = parser.parse_args()

    results = FileHashCacheProfiler.profile(
        engine=args.engine,
        compress=args.compress,
        b64=args.b64,
        size=args.size,
        iterations=args.iterations,
        verbose=args.verbose,
        num_proc=args.num_proc,
        group_by=args.group_by,
        sort_by=args.sort_by
    )
    print(results)