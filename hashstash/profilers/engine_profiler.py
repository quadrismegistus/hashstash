from . import *
from ..utils.encodings import encode, decode

opts_all = dict(
    iterations=1_000,
    size=1_000_00,
    serializers=SERIALIZERS,
    engines=ENGINES,
    # compress=get_working_compressers(),
    compress=[
        x
        for x in get_working_compressers()
        if x
        in {
            "lz4",
            "blosc",
            "raw",
            "zlib",
        }
    ],
    b64=[True],
    num_procs=[8],
    progress_inner=True,
    operations=None,
)

opts_serializers = dict(
    iterations=1_000,
    size=1_000_00,
    serializers=SERIALIZERS,
    engines=["memory"],
    compress=[False],
    b64=[True],
    num_procs=[8],
    progress_inner=True,
    operations=["Serialize", "Deserialize", "Serialize + Deserialize"],
)

opts_encoders = dict(
    iterations=1_000,
    size=1_000_00,
    serializers=["pickle"],
    engines=[DEFAULT_ENGINE_TYPE],
    b64=[False],
    compress=get_working_compressers(),
    num_procs=[8],
    progress_inner=True,
    operations=["Encode", "Decode", "Encode + Decode"],
)

opts_engines = dict(
    iterations=1_00,
    size=1_000_000,
    engines=ENGINES,
    # engines=[e for e in ENGINES if e!='sqlite'],
    serializers=["pickle"],
    compress=[OPTIMAL_COMPRESS],
    num_procs=[8],
    b64=[True],
    progress_inner=True,
    operations=["Set", "Get", "Set + Get"],
)


RAW_SIZE_KEY = "Raw Size (B)"


profiler_stash = HashStash(name="profilers", compress=False, b64=False)


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
        return {"stash": self.stash.to_dict()}

    @classmethod
    def from_dict(cls, d):
        from hashstash import HashStash

        return cls(HashStash(**d["stash"]))

    # def __reduce__(self):
    # return (self.__class__.from_dict, (self.to_dict(),))

    def __eq__(self, other):
        if not isinstance(other, HashStashProfiler):
            return False
        return self.to_dict() == other.to_dict()

    def __repr__(self):
        return f"{self.__class__.__name__}({self.stash})"

    # @profiler_stash.stashed_dataframe
    def profile(
        self,
        iterations: int = DEFAULT_ITERATIONS,
        size: list = DEFAULT_DATA_SIZE,
        num_proc: int = 1,
        verbose: bool = False,
        progress: bool = True,
        data_type=DEFAULT_DATA_TYPE,
        operations=None,
        _force=False,
        **kwargs,
    ):
        import pandas as pd

        assert isinstance(data_type, str), "One data type only"
        with self.stash.tmp(dbname="_tmp_profile", use_tempfile=False) as tmp_stash:
            common_data = {
                "Engine": tmp_stash.engine,
                "Serializer": tmp_stash.serializer,
                "Encoding": get_encoding_str(tmp_stash.compress, tmp_stash.b64),
                "Data Type": data_type,
                "Num Proc": num_proc,
                "Size (B)": size,
            }
            task = {"size": size, "data_type": data_type, "operations": operations}

            smap = profiler_stash.map(
                profile_stash_transaction,
                objects=tmp_stash,
                options=task,
                total=iterations,
                num_proc=num_proc,
                ordered=True,
                progress=progress,
                desc=f"Profiling {tmp_stash.engine} {tmp_stash.serializer} {tmp_stash.compress} {tmp_stash.b64} {data_type} {operations}",
                stash_runs=False,
                stash_map=True,
                _force=_force,
                **common_data,
            )
            return pd.DataFrame(
                {"Iteration": i + 1, **sres}
                for i, sres in enumerate(smap.iter_results())
                if sres is not None
            )
            # timestart = time.time()
            # sizestart = 0
            # o = []

            # for i, result_dict in enumerate(smap.iter_results()):
            #     sizestart += result_dict["Raw Size (B)"]
            #     out_d = {
            #         "Iteration": i,
            #         "Num Proc": num_proc,
            #         "Size": size,
            #         **common_data,
            #         **result_dict,
            #         "Cumulative Time (s)": time.time() - timestart,
            #         "Cumulative Size (B)": sizestart,
            #         "Cumulative Rate (it/s)": (
            #             len(o) / (time.time() - timestart)
            #             if (time.time() - timestart)
            #             else 0
            #         ),
            #         "Cumulative Speed (MB/s)": (
            #             (sizestart / 1024 / 1024 / (time.time() - timestart))
            #             if (time.time() - timestart)
            #             else 0
            #         ),
            #     }
            #     o.append(out_d)
            # odf = pd.DataFrame(o)
            # return odf

    @classmethod
    def profile_stash(cls, stash, **opt):
        return HashStashProfiler(stash).profile(**opt)

    @classmethod
    # @profiler_stash.stashed_dataframe
    def run_profiles(
        cls,
        iterations=1_000,
        size=1_000_00,
        engines=get_working_engines(),
        serializers=get_working_serializers(),
        num_procs=[8],
        compress=get_working_compressers(),
        b64=[True],
        append_mode=[False],
        num_proc=1,
        _force=False,
        progress=True,
        progress_inner=True,
        operations=None,
        data_types=DATA_TYPES,
        **kwargs,
    ):
        import pandas as pd

        stashes = cls.get_stashes_from_options(
            engines, serializers, compress, b64, num_procs, append_mode, _force
        )
        objects = []
        options = []
        for nproc in num_procs:
            for dtype in data_types:
                for stash in stashes:
                    # if stash.engine in {'shelve'} and nproc > 1:
                    # continue
                    opt = {
                        "num_proc": nproc,
                        "progress": progress_inner,
                        "iterations": iterations,
                        "size": size,
                        "operations": operations,
                        "data_type": dtype,
                    }
                    options.append(opt)
                    objects.append(stash)

        smap = profiler_stash.map(
            HashStashProfiler.profile_stash,
            objects=objects,
            options=options,
            # total=iterations,
            num_proc=num_proc,
            ordered=True,
            progress=progress,
            stash_runs=True,
            stash_map=True,
            desc=f"Profiling {len(objects)} stashes",
            _force=_force,
        )
        return pd.concat(smap.results)
        # # @parallelized(num_proc=num_proc, progress=progress)
        # def process(**opt):
        #     from hashstash import HashStash
        #     stash = HashStash(**opt)
        #     return stash.profiler.profile(**opt)

        # # stash = HashStash(engine='dataframe')
        # # out = stash.map(process, options=opts, progress=progress, num_proc=num_proc)
        # # return pd.concat(out.results) if out.results else pd.DataFrame()
        # return pd.concat(process(**opt) for opt in opts)

    @staticmethod
    def get_stashes_from_options(
        engines, serializers, compress, b64, num_procs, append_mode, _force=False
    ):
        if "redis" in engines:
            start_redis_server()
        if "mongo" in engines:
            start_mongo_server()
        opts = []
        for engine in engines:
            for serializer in serializers:
                for compressx in compress:
                    for b64x in b64:
                        for appendmode in append_mode:
                            opt = {
                                "engine": engine,
                                "serializer": serializer,
                                "compress": compressx,
                                "b64": b64x,
                                "append_mode": appendmode,
                                "_force": _force,
                            }
                            opts.append(opt)
        return [HashStash(**opt) for opt in opts]

    @classmethod
    def profile_serializers(cls, **opts):
        return cls.get_profile_data(**{**opts_serializers, **opts})

    @classmethod
    def profile_engines(cls, **opts):
        return cls.get_profile_data(**{**opts_engines, **opts})

    @classmethod
    def profile_encodings(cls, **opts):
        return cls.get_profile_data(**{**opts_encoders, **opts})

    @classmethod
    def get_size_data(cls, **profile_kwargs):
        import pandas as pd

        df = cls.run_profiles(**profile_kwargs)
        df["Data Type"] = df["Data Type"].apply(
            lambda x: str(x).split("['")[-1].split("']")[0]
        )
        # df["Num Proc"] = df["Num Proc"].astype(str)
        df["Encoding"] = (
            df["Encoding"]
            .fillna("")
            .apply(lambda x: x if x and str(x) != "nan" else "raw")
        )

        groupby = [
            "Iteration",
            "Engine",
            "Data Type",
            "Num Proc",
            "Serializer",
            "Encoding",
        ]
        avg_df = (
            df.groupby([g for g in groupby if g != "Data Type"])
            .mean(numeric_only=True)
            .reset_index()
        )
        avg_df["Data Type"] = "Average"
        df = pd.concat([df, avg_df])

        # Calculate cumulative sizes
        df = df.sort_values("Iteration")
        df["Cumulative Raw Size (B)"] = df.groupby(groupby[1:])["Raw Size (B)"].cumsum()
        df["Raw Size (KB)"] = df["Raw Size (B)"] / 1024
        try:
            df["Cumulative Serialized Size (B)"] = df.groupby(groupby[:1])[
                "Serialized Size (B)"
            ].cumsum()
            df["Serialized Size (KB)"] = df["Serialized Size (B)"] / 1024
            df["Serialized Compression Ratio"] = (
                df["Serialized Size (B)"] / df["Raw Size (B)"]
            )
        except KeyError:
            pass
        try:
            df["Cumulative Encoded Size (B)"] = df.groupby(groupby[1:])[
                "Encoded Size (B)"
            ].cumsum()
            df["Encoded Size (KB)"] = df["Encoded Size (B)"] / 1024
            df["Encoded Compression Ratio"] = (
                df["Encoded Size (B)"] / df["Raw Size (B)"]
            )
        except KeyError:
            pass

        return df

    @classmethod
    def get_profile_data(
        cls, melted=True, operations=None, _force=False, **profile_kwargs
    ):
        import pandas as pd

        df = cls.run_profiles(operations=operations, _force=_force, **profile_kwargs)
        df["Data Type"] = df["Data Type"].apply(
            lambda x: str(x).split("['")[-1].split("']")[0]
        )
        df["Num Proc"] = df["Num Proc"].astype(str)
        df["Encoding"] = (
            df["Encoding"]
            .fillna("")
            .apply(lambda x: x if x and str(x) != "nan" else "raw")
        )
        try:
            df["Serialize + Deserialize Time (s)"] = (
                df["Serialize Time (s)"] + df["Deserialize Time (s)"]
            )
        except KeyError:
            pass
        try:
            df["Encode + Decode Time (s)"] = (
                df["Encode Time (s)"] + df["Decode Time (s)"]
            )
        except KeyError:
            pass
        try:
            df["Set + Get Time (s)"] = df["Set Time (s)"] + df["Get Time (s)"]
        except KeyError:
            pass

        # Calculate the average values across all data types
        groupby = [
            "Iteration",
            "Engine",
            "Data Type",
            "Num Proc",
            "Serializer",
            "Encoding",
            "Raw Size (B)",
        ]
        avg_df = df.groupby(groupby).mean(numeric_only=True).reset_index()
        avg_df["Data Type"] = "Average"

        # Calculate cumulative Serialized Size and Encoded Size across Iterations
        try:
            df["Cumulative Serialized Size (B)"] = df.groupby(groupby)[
                "Serialized Size (B)"
            ].cumsum()
        except KeyError:
            pass
        try:
            df["Cumulative Encoded Size (B)"] = df.groupby(groupby)[
                "Encoded Size (B)"
            ].cumsum()
        except KeyError:
            pass

        # Append the average DataFrame to the original DataFrame
        df = pd.concat([df, avg_df], ignore_index=True)
        if melted:
            dfmelt = df.melt(
                id_vars=groupby,
                value_vars=[x for x in df.columns if x.endswith("Time (s)")],
                var_name="Operation",
                value_name="Time (s)",
            )
            dfmelt["Operation"] = dfmelt["Operation"].apply(
                lambda x: x.split(" Time")[0]
            )
            if operations:
                dfmelt = dfmelt[dfmelt.Operation.isin(operations)]

            dfmelt["Rate (it/s)"] = 1 / dfmelt["Time (s)"]
            dfmelt["Speed (MB/s)"] = (
                (dfmelt["Raw Size (B)"] / dfmelt["Time (s)"]) / 1024 / 1024
            )
            dfmelt.sort_values("Iteration")
            rollby = groupby[1:]
            dfmelt["Time (ms) Rolling"] = dfmelt.groupby(rollby)["Time (s)"].transform(
                lambda x: x.rolling(window=100).median()
            )
            dfmelt["Rate (it/s) Rolling"] = dfmelt.groupby(rollby)[
                "Rate (it/s)"
            ].transform(lambda x: x.rolling(window=100).median())
            dfmelt["Speed (MB/s) Rolling"] = dfmelt.groupby(rollby)[
                "Speed (MB/s)"
            ].transform(lambda x: x.rolling(window=100).median())

            return dfmelt.set_index(
                [
                    "Data Type",
                    "Engine",
                    "Serializer",
                    "Encoding",
                    "Num Proc",
                    "Operation",
                    "Iteration",
                    "Raw Size (B)",
                ]
            ).sort_index()
        return df

    @classmethod
    def plot(
        cls,
        df,
        group_by=None,
        color_by="Engine",
        label_by="Engine",
        x="Iteration",
        y="Rate (it/s)",
        moving_window=100,
        facet_by="Operation",
        facet_grid_by=None,
        facet_order=None,
        operations=None,
        log_y=False,
        width=10,
        height=10,
        ncol=None,
        nrow=None,
        scales="fixed",
        filename=None,
        smooth=True,
        **kwargs,
    ):
        import pandas as pd
        import plotnine as p9

        p9.options.figure_size = (width, height)
        p9.options.dpi = 300

        if group_by is None:
            group_by = [
                "Engine",
                "Serializer",
                "Encoding",
                "Num Proc",
                "Operation",
                "Data Type",
            ]
        df = reset_index(df)
        group_by = [x for x in group_by if x in df.columns]
        if operations:
            df = df[df.Operation.isin(operations)]
        # df = df.sort_values("Iteration")

        figdf = (
            df.groupby((["Iteration"] if "Iteration" in df.columns else []) + group_by)
            .median(numeric_only=True)
            .reset_index()
        )
        if facet_by:
            if facet_order is None:
                if facet_by == "Operation":
                    facet_order = operations
            if facet_order is not None:
                figdf[facet_by] = pd.Categorical(
                    figdf[facet_by], categories=facet_order, ordered=True
                )

        fig = p9.ggplot(figdf, p9.aes(x=x, y=y, color=color_by))
        # fig += p9.geom_line()
        if smooth:
            fig += p9.geom_smooth(method="loess", se=True, alpha=0.1)
        # fig += p9.geom_point(data=figdf[figdf.Iteration % 10 == 0], alpha=0.5)

        if label_by:
            label_df = pd.concat(
                gdf.assign(
                    **{
                        x: gdf[x].sample(n=1).values[0],
                        y: gdf[y].median(),
                    }
                )
                for g, gdf in figdf.groupby(group_by)
            )
            fig += p9.geom_label(
                p9.aes(label=label_by),
                data=label_df,
                size=6,
            )
        fig += p9.theme_classic()
        fig += p9.scale_color_brewer(type="qualitative", palette=2)
        if log_y:
            fig += p9.scale_y_log10()
        if facet_grid_by:
            fig += p9.facet_grid(facet_grid_by, scales=scales)
        elif facet_by:
            fig += p9.facet_wrap(facet_by, scales=scales, ncol=ncol, nrow=nrow)

        caption = []
        for g in group_by:
            if not g in {color_by, label_by, facet_by}:
                caption.append(f'* {g}: {", ".join(str(x) for x in figdf[g].unique())}')
        filesize = f'* File size: {figdf["Raw Size (B)"].median()/1024/1024:.2f}'
        stdev = figdf["Raw Size (B)"].std()
        if stdev:
            filesize += f" +/- {stdev/1024/1024:.2f}"
        caption.append(filesize + " MB")

        fig += p9.labs(
            title=f"Comparing {color_by.lower()}s",
            y=y if not log_y else y + " (log)",
            caption="\n".join(caption),
        )
        if filename is None:
            filename = f"fig.comparing_{color_by.lower()}s.png"
        figfn = Path(profiler_stash.path).parent / "figures" / filename
        figfn.parent.mkdir(parents=True, exist_ok=True)
        fig.save(figfn)
        return fig

    @classmethod
    def plot_serializers(
        cls,
        df=None,
        color_by="Serializer",
        operations=["Serialize", "Deserialize", "Serialize + Deserialize"],
        label_by="Serializer",
        log_y=True,
        x="Iteration",
        y="Rate (it/s)",
        height=6,
        width=8,
        ncol=None,
        nrow=1,
        scales="fixed",
        group_by=None,
        moving_window=100,
        facet_by=None,
        facet_grid_by="Operation ~ Data Type",
        facet_order=None,
        filename="fig.comparing_serializers.png",
        **opts,
    ):
        if df is None:
            df = cls.profile_serializers(**opts).reset_index()
            df = df[df.Operation.isin(["Serialize + Deserialize"])]

        return cls.plot(
            df=df,
            color_by=color_by,
            operations=operations,
            label_by=label_by,
            log_y=log_y,
            x=x,
            y=y,
            height=height,
            width=width,
            ncol=ncol,
            nrow=nrow,
            scales=scales,
            group_by=group_by,
            moving_window=moving_window,
            facet_by=facet_by,
            facet_grid_by=facet_grid_by,
            facet_order=facet_order,
            filename=filename,
            **opts,
        )

    @classmethod
    def plot_engines(
        cls,
        df=None,
        color_by="Engine",
        operations=["Set + Get", "Set", "Get"],
        label_by="Engine",
        log_y=False,
        x="Iteration",
        y="Rate (it/s)",
        height=6,
        width=8,
        ncol=None,
        nrow=1,
        scales="free_y",
        group_by=None,
        moving_window=100,
        facet_by=None,
        facet_grid_by="Operation ~ Data Type",
        facet_order=None,
        filename="fig.comparing_engines.png",
        **opts,
    ):
        if df is None:
            df = cls.profile_engines(**opts).reset_index()
            df = df[df.Operation.isin(["Set + Get"])]
        return cls.plot(
            df=df,
            color_by=color_by,
            operations=operations,
            label_by=label_by,
            log_y=log_y,
            x=x,
            y=y,
            height=height,
            width=width,
            ncol=ncol,
            nrow=nrow,
            scales=scales,
            group_by=group_by,
            moving_window=moving_window,
            facet_by=facet_by,
            facet_grid_by=facet_grid_by,
            facet_order=facet_order,
            filename=filename,
            **opts,
        )

    @classmethod
    def plot_encodings(
        cls,
        df=None,
        color_by="Encoding",
        operations=["Encode", "Decode", "Encode + Decode"],
        label_by="Encoding",
        log_y=True,
        x="Iteration",
        y="Rate (it/s)",
        height=6,
        width=8,
        ncol=None,
        nrow=1,
        scales="fixed",
        group_by=None,
        moving_window=100,
        facet_by=None,
        facet_grid_by="Operation ~ Data Type",
        facet_order=None,
        filename="fig.comparing_encodings.png",
        **opts,
    ):
        if df is None:
            df = cls.profile_encodings(**opts).reset_index()
            # df = df[df.Operation.isin(['Encode + Decode'])]
        return cls.plot(
            df=df,
            color_by=color_by,
            operations=operations,
            label_by=label_by,
            log_y=log_y,
            x=x,
            y=y,
            height=height,
            width=width,
            ncol=ncol,
            nrow=nrow,
            scales=scales,
            group_by=group_by,
            moving_window=moving_window,
            facet_by=facet_by,
            facet_grid_by=facet_grid_by,
            facet_order=facet_order,
            filename=filename,
            **opts,
        )

    @classmethod
    def plot_sizes(cls):
        import plotnine as p9

        opts = {
            **opts_encoders,
            "serializers": get_working_serializers(),
            "iterations": 100,
            "operations": ["Serialize", "Deserialize", "Encode", "Decode"],
        }
        df = cls.get_size_data(**opts)
        df["Compression (%)"] = 1 - (df["Encoded Size (B)"] / df["Raw Size (B)"])
        df["Encode Time (s)"] += df["Serialize Time (s)"]
        df["Decode Time (s)"] += df["Deserialize Time (s)"]
        df["Encode + Decode Time (s)"] = df["Encode Time (s)"] + df["Decode Time (s)"]
        df["Encode + Decode Rate (it/s)"] = 1 / df["Encode + Decode Time (s)"]
        df["Encode Rate (it/s)"] = 1 / df["Encode Time (s)"]
        df["Decode Rate (it/s)"] = 1 / df["Decode Time (s)"]
        df = (
            df.groupby(["Serializer", "Encoding"])
            .median(numeric_only=True)
            .reset_index()
        )
        # df = df.melt(id_vars=['Serializer','Encoding','Compression (%)', 'Raw Size (B)'], value_vars=['Encode Rate (it/s)', 'Decode Rate (it/s)', 'Encode + Decode Rate (it/s)'], value_name='Rate (it/s)', var_name='Operation')
        # df['Operation']=df['Operation'].str.split('Rate').str[0].str.strip()
        fig = cls.plot(
            df,
            y="Encode + Decode Rate (it/s)",
            x="Compression (%)",
            facet_by=None,
            color_by="Serializer",
            label_by="Encoding",
            smooth=False,
            height=6,
            log_y=True,
            filename="fig.comparing_sizes.png",
            width=8,
        )
        # Add a vertical line at x=0
        fig += p9.geom_vline(
            xintercept=0, linetype="dashed", color="red", size=0.5, alpha=0.5
        )

        fig

        return fig
    
    @classmethod
    def plot_all(cls,filename=None,**opts):
        import plotnine as p9
        import pandas as pd
        df = cls.run_profiles(**{**opts_all, **opts}).reset_index()
        df['Rate (it/s)'] = 1 / (df['Set Time (s)'] + df['Get Time (s)'])
        df['Rate (MB/s)'] = df['Raw Size (B)'] / (df['Set Time (s)'] + df['Get Time (s)']) / 1024 / 1024
        df['Label'] = df['Engine'] + ' + ' + (df['Encoding'].str.replace('+b64',''))
        df['Filesize (MB)'] = df['Filesize (B)'] / 1024 / 1024
        p9.options.figure_size = (8, 10)
        df['Label'] = df['Engine'] + ' (' + (df['Encoding'].str.replace('+b64','')) + ')'
        rawsize = df.groupby('Iteration')['Raw Size (B)'].median().sum() / 1024 / 1024

        filesizekey=f'Final cache size (MB) [Raw size: {rawsize:.0f} MB]'
        ratekey = f'Median speed for set & get (MB/s)'

        figdf = df.groupby(['Engine','Serializer','Encoding','Label']).agg({'Rate (MB/s)': 'median', 'Filesize (MB)': 'last'}).reset_index()
        figdf['Size Ratio'] = figdf['Filesize (MB)'] / rawsize
        figdf[filesizekey] = figdf['Filesize (MB)']
        figdf[ratekey] = figdf['Rate (MB/s)']
        figdf2=figdf.melt(id_vars=['Engine','Serializer','Encoding','Label'], value_vars=[filesizekey,ratekey], var_name='Metric', value_name='Value')

        def name_serializer(x):
            return f'Serialized with {x}'

        figdf2['Serializer'] = figdf2['Serializer'].apply(name_serializer)
        figdf2['Serializer'] = pd.Categorical(figdf2['Serializer'], categories=[name_serializer(x) for x in ['pickle','hashstash', 'jsonpickle']])
        figdf2['Metric'] = pd.Categorical(figdf2['Metric'], categories=[ratekey,filesizekey])
        figdf2.sort_values(['Serializer','Metric','Value'], ascending=[True,True,True], inplace=True)
        figdf2['Label'] = pd.Categorical(figdf2['Label'], categories=figdf2['Label'].unique())
        fig=p9.ggplot(figdf2, p9.aes(x='Label', y='Value', fill='Engine')) + p9.geom_bar(stat='identity') + p9.theme_classic() + p9.theme(axis_text_x=p9.element_text(angle=45, hjust=1)) + p9.coord_flip()
        figdf2['bar_label'] = figdf2.apply(lambda row: f'{row["Value"]:.1f} MB/s' if row["Metric"] == ratekey else f'{row["Value"]:.1f} MB', axis=1)
        fig+=p9.geom_text(p9.aes(label='bar_label'), size=5, ha='left', color='black')
        fig+=p9.facet_grid('Serializer ~ Metric', scales='free')
        captions = [
            f'Num iterations: {df.Iteration.nunique()} per data type',
            f'Data types: {", ".join(df["Data Type"].unique())}',
            f'Individual file sizes: {df.groupby("Iteration")["Raw Size (B)"].median().mean() / 1024 / 1024:.1f} MB',
            f'Total file size: {rawsize:.1f} MB',
            f'Num Proc: {df["Num Proc"].median():.0f}*',
            '    * Rates measured within individual processes'
        ]
        fig+=p9.labs(
            title='Comparing engines, serializers, and encodings',
            caption='\n'.join(captions),
            x='Engine (encoding)',
            y='Rate (MB/s) | Cache size (MB)',
        )
        if filename is None:
            filename = f"fig.comparing_engines_serializers_encodings.png"
        figfn = Path(profiler_stash.path).parent / "figures" / filename
        figfn.parent.mkdir(parents=True, exist_ok=True)
        fig.save(figfn)
        return fig



def profile_stash_transaction(
    stash,
    size=DEFAULT_DATA_SIZE,
    data_type=DEFAULT_DATA_TYPE,
    operations=None,
    **common_data,
):

    key = f"test_data_{uuid.uuid4().hex}"
    data = generate_data(size, data_type=data_type)
    # Time serialization and encoding
    out = {**common_data, "Raw Size (B)": bytesize(data)}
    if not operations or {"Serialize", "Deserialize", "Encode", "Decode"} & set(
        operations
    ):
        serialized_data, out["Serialize Time (s)"] = time_function(
            lambda: stash.serialize(data)
        )
        out["Serialized Size (B)"] = bytesize(serialized_data)

    if not operations or {"Encode", "Decode"} & set(operations):
        encoded_data, out["Encode Time (s)"] = time_function(
            lambda: stash.encode(serialized_data)
        )
        out["Encoded Size (B)"] = bytesize(encoded_data)

    # Time set and get operations
    if not operations or "Set" in operations:
        _, out["Set Time (s)"] = time_function(lambda: stash.set(key, data))

    if not operations or "Get" in operations:
        _, out["Get Time (s)"] = time_function(lambda: stash.get(key))

    # Time decoding and deserialization
    if not operations or "Decode" in operations:
        _, out["Decode Time (s)"] = time_function(lambda: stash.decode(encoded_data))

    if not operations or "Deserialize" in operations:
        _, out["Deserialize Time (s)"] = time_function(
            lambda: stash.deserialize(serialized_data)
        )

    if not operations or "Set" in operations or "Get" in operations:
        out["Filesize (B)"] = stash.filesize

    return out


if __name__ == "__main__":
    run_profile()
