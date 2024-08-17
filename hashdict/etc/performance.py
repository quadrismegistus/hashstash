from hashdict import *
from hashdict.engines.redis import *
import pandas as pd
import plotnine as p9
import itertools
from tqdm import tqdm
import tempfile
import shutil
from concurrent.futures import ProcessPoolExecutor, as_completed


def generate_profile_sizes(num_sizes: int = NUM_PROFILE_SIZES, multiplier: int = PROFILE_SIZE_MULTIPLIER, initial_size: int = INITIAL_PROFILE_SIZE) -> tuple:
    profile_sizes = []
    for n in range(num_sizes):
        if not profile_sizes:
            profile_sizes.append(initial_size)
        else:
            profile_sizes.append(profile_sizes[-1] * multiplier)
    return tuple(profile_sizes)



class HashDictProfiler:
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
    def profile_cache(
        self,
        root_dir: str = DEFAULT_ROOT_DIR,
        engine: ENGINE_TYPES = DEFAULT_ENGINE_TYPE,
        compress: bool = DEFAULT_COMPRESS,
        b64: bool = DEFAULT_B64,
        size: int = DEFAULT_DATA_SIZE,
        verbose: bool = False,
        iter=None,
        name = 'performance_cache',
    ):
        cache = HashDict(name=name, engine=engine, root_dir=root_dir, compress=compress, b64=b64)
        data = self.generate_data(size)
        raw_size = len(json.dumps(data).encode())
        cache_key = f"test_data_{size}_{random.random()}"

        # Encode value to get cached size
        encoded_value = cache.encode(data)
        cached_size = len(encoded_value)

        results = []
        common_data = {
            "Encoding": self.get_encoding_str(compress, b64),
            "Engine": engine,
            "Size (B)": int(size),
            "Raw Size (B)": raw_size,
            "Cached Size (B)": cached_size,
            "Compression Ratio (%)": (cached_size / (raw_size * 100)) if raw_size else 0,
            "Iteration": iter if iter else 0,
        }

        def add_result(operation, time_taken, additional_data=None):
            result = {
                "Operation": operation,
                "Time (s)": time_taken,
                "Rate (it/s)": (1 / time_taken) if time_taken else 0,
                "Speed (MB/s)": ((raw_size / time_taken) if time_taken else 0) / 1024 / 1024,
                **common_data
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
        return results

    @classmethod
    def profile_cache_parallel(cls, args):
        return cls.profile_cache(**args)

    @classmethod
    @cached_result
    def profile(
        self,
        engine=ENGINES,
        compress=[True, False],
        b64=[True, False],
        size=PROFILE_SIZES,
        iterations=DEFAULT_ITERATIONS,
        verbose: bool = False,
        num_proc: tuple = DEFAULT_NUM_PROC,
        profile_by=('size','engine'),
    ):
        if not size:
            size = generate_profile_sizes()
        
        results = []
        
        # Generate all possible group combinations
        group_combinations = [
            dict(zip(profile_by, values))
            for values in sorted(itertools.product(*(
                engine if param == 'engine' else
                compress if param == 'compress' else
                b64 if param == 'b64' else
                size if param == 'size' else
                [True, False] if param == 'verbose' else
                [None]  # Default for any other parameter
                for param in profile_by
            )))
        ]
        pbar = tqdm(group_combinations, desc='Group combinations', position=0, leave=True)
        for group in pbar:
            pbar.set_description(str(group))
            tasks = []
            for _ in range(iterations):
                task = {
                    'engine': group.get('engine', random.choice(engine)),
                    'compress': group.get('compress', random.choice(compress)),
                    'b64': group.get('b64', random.choice(b64)),
                    'size': group.get('size', random.choice(size)),
                    'verbose': group.get('verbose', verbose),
                    'iter': _,
                }
                tasks.append(task)

            random.shuffle(tasks)

            for nproc in num_proc:
                writenum = 0
                sizenow = 0
                timestart = time.time()
                for res in self.profile_cache_nproc(tasks, nproc, group):
                    if res is not None:
                        if res.get('Operation') == 'Write':
                            writenum += 1
                            sizenow += res.get('Raw Size (B)', 0)
                        results.append({**res, 'write_num': writenum})

        return pd.DataFrame(results)
    
    @classmethod
    def profile_cache_nproc(self, tasks, nproc, group=None):
        def return_iter(proc, numproc):
            for result in tqdm(proc, desc=f'Processing {numproc}x ({group})', total=len(tasks), position=0, leave=True):
            # for result in proc:
                if result is not None:
                    for d in result:
                        d["Num Processes"] = numproc
                        yield d

        with tempfile.TemporaryDirectory() as root_dir:
            tasks = [{'root_dir':root_dir, **d} for d in tasks]

            if nproc > 1:
                with ProcessPoolExecutor(max_workers=nproc) as executor:
                    proc = imap(executor, self.profile_cache_parallel, tasks)
                    yield from return_iter(proc, nproc)
            else:
                proc = (self.profile_cache(**task) for task in tasks)
                yield from return_iter(proc, nproc)

            cache_obj = HashDict(engine=group['engine'], root_dir=root_dir)
            cache_obj.clear()

            shutil.rmtree(root_dir, ignore_errors=True)
            


    # @classmethod
    # def profile_speed(self, *args, **kwargs):
    #     df = pd.DataFrame(self.profile(*args, **kwargs))
        
    #     # Identify columns to melt
    #     speed_columns = [col for col in df.columns if any(x in col for x in ['Speed', 'Rate', 'Time'])]
    #     id_vars = [col for col in df.columns if col not in speed_columns]
        
    #     # Melt the dataframe
    #     melted_df = pd.melt(df, 
    #                         id_vars=id_vars,
    #                         value_vars=speed_columns,
    #                         var_name='Metric', value_name='Value')
        
    #     # Create 'Operation' and 'Measure' columns
    #     melted_df['Operation'] = melted_df['Metric'].apply(lambda x: x.split()[0])
    #     melted_df['Measure'] = melted_df['Metric'].apply(lambda x: x.split('(')[1].split(')')[0])
        
    #     # Pivot the dataframe
    #     reshaped_df = melted_df.pivot_table(values='Value', 
    #                                         index=id_vars + ['Operation'],
    #                                         columns='Measure')
        
    #     # Reset index and sort
    #     result_df = reshaped_df.reset_index()
    #     if 'MB/s' in result_df.columns:
    #         result_df = result_df.sort_values('MB/s', ascending=False)
        
    #     return result_df

    @classmethod
    def profile_df(self, *args, group_by=GROUPBY, sort_by=SORTBY, by_speed=False, operations={'Read','Write'},**kwargs):
        df = self.profile(*args, **kwargs)
        if operations:
            df = df[df.Operation.isin(operations)]
        df = pd.concat(
            gdf.sort_values('write_num').assign(**{
                'Cumulative Time (s)':gdf['Time (s)'].cumsum(),
                'Cumulative Size (MB)':gdf['Size (B)'].cumsum() / 1024 / 1024,
            })
            for g,gdf in df.groupby([x for x in group_by if not x.startswith('write_num')])
        )
        if group_by:
            df = df.groupby(group_by).mean(numeric_only=True).round(4).sort_index()
        if sort_by:
            df = df.sort_values(sort_by, ascending=False)
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

    parser = argparse.ArgumentParser(description="Profile FileHashDict performance")
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

    results = FileHashDictProfiler.profile(
        engine=tuple(args.engine),
        compress=tuple(args.compress),
        b64=tuple(args.b64),
        size=tuple(args.size),
        iterations=args.iterations,
        verbose=args.verbose,
        num_proc=tuple(args.num_proc)
    )
    print(pd.DataFrame(results))