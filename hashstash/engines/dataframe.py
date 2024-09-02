from . import *
from .pairtree import PairtreeHashStash
from ..utils.dataframes import MetaDataFrame

class DataFrameHashStash(PairtreeHashStash):
    engine = "dataframe"
    prefix_index_cols = "_"

    def __init__(self, *args, df_engine=None, io_engine=None, **kwargs):
        ## check io engine
        super().__init__(*args, **kwargs)
        self.io_engine = get_io_engine(io_engine)
        self.df_engine = get_df_engine(df_engine)

    def to_dict(self):
        return {**super().to_dict(), 'io_engine': self.io_engine, 'df_engine': self.df_engine}

    def set(self, unencoded_key: bytes, unencoded_value: bytes) -> None:
        log.debug(f"Setting value for key: {unencoded_key}")
        # set value as pairtree does if not a dataframe
        if not is_dataframe(unencoded_value):
            log.debug(f"Input is not a DataFrame")
            return super().set(unencoded_key, unencoded_value)

        # Handle DataFrame values
        mdf = MetaDataFrame(unencoded_value)
        log.debug(f"Input is a {mdf.df_engine} DataFrame with shape: {mdf.shape}")

        encoded_key = self.encode_key(unencoded_key)
        self._set_key(encoded_key)
        filepath_value = self._get_path_new_value(encoded_key)
        return mdf.write(filepath_value, io_engine=self.io_engine, compression=self.compress)

    @log.debug
    def get_all(
        self,
        unencoded_key: Any = None,
        default: Any = None,
        with_metadata=None,
        all_results=None,
        as_dataframe=None,
        as_list=None,
        **kwargs,
    ) -> Any:
        all_results = self._all_results(all_results)
        if all_results and as_dataframe is None:
            as_dataframe = True
        paths_ld = self.get_path_values(
            unencoded_key,
            all_results=self._all_results(all_results),
            with_metadata=True,
        )

        out_l = []
        for path_d in paths_ld:
            path = path_d.pop("_path")
            decoded_value = self._decode_value_from_filepath(path)
            if is_dataframe(decoded_value):
                df = decoded_value
                if with_metadata:
                    df = df.assign(**path_d)
                out_l.append(df)
            else:
                if with_metadata:
                    obj = {**path_d, "_value": decoded_value}
                else:
                    obj = decoded_value
                if as_dataframe and not as_list:
                    obj = MetaDataFrame(
                        flatten_ld([obj]),
                        df_engine=self.df_engine,
                    )

                out_l.append(obj)

        if not out_l:
            return default

        if as_dataframe and not as_list:
            return out_l[0].concat(*out_l[1:]) if len(out_l) > 1 else out_l[0]
        else:
            return out_l

    @log.debug
    def get(
        self,
        unencoded_key: Any = None,
        default: Any = None,
        with_metadata=False,
        all_results=None,
        as_string=False,
        as_dataframe=None,
        **kwargs,
    ) -> Any:
        
        values = self.get_all(
            unencoded_key,
            default=None,
            with_metadata=with_metadata,
            all_results=all_results,
            as_dataframe=as_dataframe,
            **kwargs,
        )
        if values is None: return default
        if is_dataframe(values): return values
        value = values[-1] if values else default
        return self.serialize(value) if as_string else value
        # if values is None:
        #     return default
        
        # if not self._all_results(all_results) and isinstance(values, list):
        #     values = values[-1]
        
        # return self.serialize(values) if as_string else values

    def _decode_value_from_filepath(self, filepath):
        try:
            ext = os.path.splitext(filepath)[1]
            if not ext:
                return super().decode_value_from_filepath(filepath)
            return MetaDataFrame.read(filepath, df_engine=self.df_engine, compression=self.compress)
        except Exception as e:
            log.warning(f'error reading dataframe from {filepath}: {e}')
            return None

    @log.debug
    def items(
        self, all_results=None, with_metadata=False, as_dataframe=False, **kwargs
    ):
        for key in self.keys():
            vals = self.get_all(
                key,
                all_results=all_results,
                with_metadata=with_metadata,
                as_dataframe=as_dataframe,
                **kwargs,
            )
            if vals is None:
                log.warning(f'empty values returned for {key}')
            else:
                if as_dataframe:
                    yield key, vals
                else:
                    for val in vals:
                        yield key, val

    def assemble_df(
        self,
        all_results=None,
        with_metadata=None,
        **kwargs,
    ):
        dfs = []
        for key, df in progress_bar(self.items(
            all_results=all_results, with_metadata=with_metadata, as_dataframe=True
        ), total=len(self), desc='concatenating dataframes across values'):
            dfs.append(
                df.assign(**{k:serialize(v) for k,v in flatten_args_kwargs(key).items()})
            )
        if not dfs:
            return MetaDataFrame([], self.df_engine)
        combined_df = dfs[0].concat(*dfs[1:])
        return combined_df.set_index()

    def assemble_ld(
        self,
        all_results=None,
        with_metadata=False,
        **kwargs,
    ):
        mdf = self.assemble_df(
            all_results=all_results,
            with_metadata=with_metadata,
            **kwargs,
        )
        ld = mdf.reset_index().to_pandas().df.to_dict(orient="records")
        return filter_ld(ld, no_nan=True)
