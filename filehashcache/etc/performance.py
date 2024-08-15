import warnings
warnings.filterwarnings('ignore')
import plotnine as p9
from concurrent.futures import ProcessPoolExecutor, as_completed
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
from tqdm import tqdm
from functools import lru_cache
from itertools import groupby
import shutil

# cache = lru_cache(maxsize=None)
cache = Cache(engine='file', root_dir='.cache_profile')

DEFAULT_NUM_PROC = (1,2, mp.cpu_count())
DEFAULT_DATA_SIZE = 1_000_000
ENGINES = ("memory", "file", "sqlite", "shelve", "redis")
ENGINE_TYPES = Literal["memory", "file", "sqlite", "shelve", "redis"]
DEFAULT_ENGINE_TYPE = "file"
INITIAL_SIZE = 1024

DEFAULT_ITERATIONS = 1
GROUPBY = ["Engine", "Encoding", "Method"]
SORTBY = "MB/s"
DEFAULT_INDEX = ["Encoding", "Engine"]

def generate_profile_sizes(num_sizes: int = 5, multiplier: int = 4, initial_size: int = INITIAL_SIZE) -> tuple:
    profile_sizes = []
    for n in range(num_sizes):
        if not profile_sizes:
            profile_sizes.append(initial_size)
        else:
            profile_sizes.append(profile_sizes[-1] * multiplier)
    return tuple(profile_sizes)

# Example usage:
PROFILE_SIZES = generate_profile_sizes(num_sizes=6, multiplier=10, initial_size=10)


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
    def get_encoding_str(compress: bool = None, b64: bool = None):
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
        compress: bool = None,
        b64: bool = None,
        **kwargs,
    ):
        return f"{engine} ({self.get_encoding_str(compress, b64)})"

    @classmethod
    # @cache.decorator
    def profile_cache(
        self,
        root_dir: str = ".cache_profile",
        engine: ENGINE_TYPES = DEFAULT_ENGINE_TYPE,
        compress: bool = None,
        b64: bool = None,
        size: int = DEFAULT_DATA_SIZE,
        verbose: bool = False,
        shared_cache=None,
        iter=None,
    ):
        if shared_cache is not None:
            cache = Cache(root_dir, engine=engine, compress=compress, b64=b64, shared_cache=shared_cache)
        else:
            cache = Cache(root_dir, engine=engine, compress=compress, b64=b64)
        data = self.generate_data(size)
        raw_size = len(json.dumps(data).encode())

        # Measure write speed
        start_time = time.time()
        cache_key = f"test_data_{size}_{random.random()}"
        cache[cache_key] = data
        write_time = time.time() - start_time

        cached_size = len(cache._encode_value(data))

        # Measure read speed
        start_time = time.time()
        _ = cache[cache_key]
        read_time = time.time() - start_time

        outd = {
            "Encoding": self.get_encoding_str(compress, b64),
            "Engine": engine,
            "Method": f"{self.get_method_str(engine,compress,b64)}",
            "Write Speed (MB/s)": raw_size / write_time / 1024 / 1024,
            "Read Speed (MB/s)": raw_size / read_time / 1024 / 1024,
            "Write Rate (it/s)": 1 / write_time,  # New metric
            "Read Rate (it/s)": 1 / read_time,  # New metric
            "Space Saved (MB/GB)": (raw_size - cached_size)
            / 1024
            / 1024
            / (raw_size / 1024 / 1024 / 1024),
            "Size (B)": int(size),
            "Write Time (s)": write_time,
            "Read Time (s)": read_time,
            "Raw Size (B)": raw_size,#
            "Raw Size (MB)": raw_size / 1024 / 1024,
            "Cached Size (MB)": cached_size / 1024 / 1024,
            "Space Saved (MB)": (raw_size - cached_size) / 1024 / 1024,
            "Compression Ratio (%)": cached_size / raw_size * 100,
            "Iteration": iter if iter else 0,
        }

        if verbose:
            print(outd)
        return outd

    @classmethod
    def profile_cache_parallel(cls, args):
        return cls.profile_cache(**args)

    @classmethod
    @cache.decorator
    def profile(
        self,
        engine=ENGINES,
        compress=(True, False),
        b64=(True, False),
        size=tuple(PROFILE_SIZES),
        iterations=DEFAULT_ITERATIONS,
        verbose: bool = False,
        num_proc: tuple = DEFAULT_NUM_PROC,
        group_by=('engine',),
    ):
        
        
        results = []
        tasks = [
            {
                'engine':random.choice(engine),
                'compress': random.choice(compress),
                'b64': random.choice(b64),
                'size': random.choice(size),
                'verbose': verbose,
                'iter':i,
            } for i in range(iterations)
        ]

        # Group tasks by specified parameters
        tasks.sort(key=lambda x: tuple(x[param] for param in group_by))
        grouped_tasks = groupby(tasks, key=lambda x: tuple(x[param] for param in group_by))

        # num_proc = num_proc + 1 if num_proc else mp.cpu_count()
        # num_proc_l = list(range(1, num_proc))

        for group_key, group_tasks in grouped_tasks:
            group_tasks = [t for t in group_tasks]
            print(f'\n{group_key}: {len(group_tasks)}')
            random.shuffle(group_tasks)
            for nproc in tqdm(num_proc, desc=f"Group {group_key}, Num Processors", position=1):
                writenum=0
                sizenow=0
                timestart=time.time()
                for res in self.profile_cache_nproc(group_tasks, nproc):
                    if res is not None:
                        writenum+=1
                        sizenow+=res.get('Raw Size (MB)',0)
                        results.append({**res, 'write_num':writenum, 'write_total_size':sizenow, 'write_total_time':time.time() - timestart})

        return results
    
    @classmethod
    def profile_cache_nproc(self, tasks, nproc):
        def return_iter(proc, numproc):
            for result in tqdm(proc, desc=f'Processing {numproc}x', total=len(tasks), position=0):
                if result is not None:
                    result["Num Processes"] = numproc
                    yield result

        with tempfile.TemporaryDirectory() as root_dir:
            tasks = [{'root_dir':root_dir, **d} for d in tasks]

            if nproc > 1:
                with ProcessPoolExecutor(max_workers=nproc) as executor:
                    proc = imap(executor, self.profile_cache_parallel, tasks)
                    yield from return_iter(proc, nproc)
            else:
                proc = (self.profile_cache(**task) for task in tasks)
                yield from return_iter(proc, nproc)

            shutil.rmtree(root_dir, ignore_errors=True)
            


    @classmethod
    def profile_speed(self, *args, **kwargs):
        df = pd.DataFrame(self.profile(*args, **kwargs))
        
        # Identify columns to melt
        speed_columns = [col for col in df.columns if any(x in col for x in ['Speed', 'Rate', 'Time'])]
        id_vars = [col for col in df.columns if col not in speed_columns]
        
        # Melt the dataframe
        melted_df = pd.melt(df, 
                            id_vars=id_vars,
                            value_vars=speed_columns,
                            var_name='Metric', value_name='Value')
        
        # Create 'Operation' and 'Measure' columns
        melted_df['Operation'] = melted_df['Metric'].apply(lambda x: x.split()[0])
        melted_df['Measure'] = melted_df['Metric'].apply(lambda x: x.split('(')[1].split(')')[0])
        
        # Pivot the dataframe
        reshaped_df = melted_df.pivot_table(values='Value', 
                                            index=id_vars + ['Operation'],
                                            columns='Measure')
        
        # Reset index and sort
        result_df = reshaped_df.reset_index()
        if 'MB/s' in result_df.columns:
            result_df = result_df.sort_values('MB/s', ascending=False)
        
        return result_df

    @classmethod
    def profile_df(self, *args, group_by=GROUPBY, sort_by=SORTBY, by_speed=False, **kwargs):
        df = pd.DataFrame(self.profile(*args, **kwargs)) if not by_speed else self.profile_speed(*args,**kwargs)
        df = df.copy()
        if group_by:
            df = df.groupby(group_by).mean(numeric_only=True).round(4).sort_index()
        if sort_by:
            df = df.sort_values(sort_by, ascending=False)
        # if group_by == "Method" or group_by == ["Method"]:
            # df = df[[c for c in df.columns if "/" in c]]
        return df
    
    def plot_sizes(self, x='write_num', y='write_total_time', shape='Operation', 
            color='Engine', facet='Num Processes', **profile_kwargs):
        p9.options.figure_size = 9, 6
        gby = [
            'Engine', 
            # 'Encoding', 
            # 'Method', 
            'Num Processes', 
            'Operation', 
            'Size (B)',
            'write_num'
        ]
        figdf = self.profile_df(
            group_by=gby, 
            by_speed=True,
            **profile_kwargs
            # num_proc=2
        ).sort_index().reset_index()
        # figdf=figdf.query(f'write_total_size>=1000')
        figdf['Num Processes']=pd.Categorical(figdf['Num Processes'], categories=figdf['Num Processes'].unique())
        # figdf=figdf[figdf['MB/s']>0]

        fig = p9.ggplot(
            figdf, 
            p9.aes(
                x=x, 
                y=y,
                shape=shape, 
                color=color,
                size='write_total_size'
            )
        )
        # fig += p9.geom_line(alpha=.5, size=1)
        fig += p9.geom_point(alpha=.5)
        # fig += p9.geom_smooth()
        engine_order = figdf.groupby('Engine')[y].mean().sort_values(ascending=False).index
        figdf['Engine'] = pd.Categorical(figdf['Engine'], categories=engine_order)
        if facet: fig += p9.facet_wrap(facet,nrow=1)
        # fig += p9.facet_grid('Operation~Num Processes', scales='free_y')
        fig += p9.scale_x_log10()
        fig += p9.scale_y_log10()
        fig += p9.theme_classic()
        fig += p9.scale_size_continuous(range=(.5,2))
        return fig

    



def imap(executor, func, *iterables, chunksize=1):
    """
    A generator that mimics the behavior of multiprocessing.Pool.imap()
    using ProcessPoolExecutor, yielding results in order as they become available.
    """
    # Create futures for each chunk
    futures = {}
    for i, args in enumerate(zip(*iterables)):
        if len(args) == 1:
            args = args[0]
        future = executor.submit(func, args)
        futures[future] = i

    # Yield results in order
    next_to_yield = 0
    results = {}
    for future in as_completed(futures):
        index = futures[future]
        result = future.result()
        
        if index == next_to_yield:
            yield result
            next_to_yield += 1
            
            # Yield any subsequent results that are ready
            while next_to_yield in results:
                yield results.pop(next_to_yield)
                next_to_yield += 1
        else:
            results[index] = result



if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Profile FileHashCache performance")
    parser.add_argument(
        "--engine",
        nargs="+",
        default=ENGINES,
        help="Engine types to profile (default: memory file sqlite)",
    )
    parser.add_argument(
        "--compress",
        nargs="+",
        type=lambda x: x.lower() == "true",
        default=[True, False],
        help="Compression options (default: True False)",
    )
    parser.add_argument(
        "--b64",
        nargs="+",
        type=lambda x: x.lower() == "true",
        default=[True, False],
        help="Base64 encoding options (default: True False)",
    )
    parser.add_argument(
        "--size",
        nargs="+",
        type=int,
        default=PROFILE_SIZES,
        help=f"Data sizes to profile (default: {PROFILE_SIZES})",
    )
    parser.add_argument(
        "--iterations",
        type=int,
        default=DEFAULT_ITERATIONS,
        help=f"Number of iterations for each configuration (default: {DEFAULT_ITERATIONS})",
    )
    parser.add_argument("--verbose", action="store_true", help="Enable verbose output")
    parser.add_argument(
        "--num-proc",
        nargs="+",
        type=int,
        default=DEFAULT_NUM_PROC,
        help=f"List of process counts to use (default: {DEFAULT_NUM_PROC})",
    )
    parser.add_argument(
        "--group-by",
        nargs="+",
        default=GROUPBY,
        help=f"Columns to group results by (default: {GROUPBY})",
    )
    parser.add_argument(
        "--sort-by",
        default=SORTBY,
        help=f"Column to sort results by (default: {SORTBY})",
    )

    args = parser.parse_args()

    results = FileHashCacheProfiler.profile(
        engine=tuple(args.engine),
        compress=tuple(args.compress),
        b64=tuple(args.b64),
        size=tuple(args.size),
        iterations=args.iterations,
        verbose=args.verbose,
        num_proc=tuple(args.num_proc)
    )
    print(pd.DataFrame(results))