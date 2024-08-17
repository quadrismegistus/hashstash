from ..hashstash import *
from ..engines.redis import *
import pandas as pd
import plotnine as p9
from tqdm import tqdm





class HashStashPerformance:
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

            cache_obj = HashStash(engine=group['engine'], root_dir=root_dir)
            cache_obj.clear()

            shutil.rmtree(root_dir, ignore_errors=True)
    
    @classmethod
    def profile_cache_parallel(cls, args):
        return cls.profile_cache(**args)
    
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

    





if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Profile FileHashStash performance")
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

    results = FileHashStashProfiler.profile(
        engine=tuple(args.engine),
        compress=tuple(args.compress),
        b64=tuple(args.b64),
        size=tuple(args.size),
        iterations=args.iterations,
        verbose=args.verbose,
        num_proc=tuple(args.num_proc)
    )
    print(pd.DataFrame(results))