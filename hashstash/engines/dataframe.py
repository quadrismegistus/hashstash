from . import *
from .pairtree import PairtreeHashStash
import pandas as pd
import time
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.feather as feather
import os
from ..utils.dataframes import MetaDataFrame

INDICATOR_BYTES = "bytes"


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
        log.info(f"Setting value for key: {unencoded_key}")
        # set value as pairtree does if not a dataframe
        if not is_dataframe(unencoded_value):
            log.info(f"Input is not a DataFrame")
            return super().set(unencoded_key, unencoded_value)

        # Handle DataFrame values
        mdf = MetaDataFrame(unencoded_value)
        log.info(f"Input is a {mdf.df_engine} DataFrame with shape: {mdf.shape}")

        encoded_key = self.encode_key(unencoded_key)
        self._set_key(encoded_key)
        filepath_value = self._get_path_new_value(encoded_key)
        return mdf.write(filepath_value, io_engine=self.io_engine)

    @log.debug
    def get_all(
        self,
        unencoded_key: Any = None,
        *args,
        default: Any = None,
        as_function=None,
        with_metadata=None,
        all_results=True,
        as_dataframe=None,
        as_list=None,
        **kwargs,
    ) -> Any:
        unencoded_key = self.new_unencoded_key(
            unencoded_key, *args, as_function=as_function, **kwargs
        )
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
        *args,
        as_function=None,
        with_metadata=False,
        as_string=False,
        **kwargs,
    ) -> Any:
        values = self.get_all(
            unencoded_key,
            *args,
            default=None,
            with_metadata=with_metadata,
            as_function=as_function,
            all_results=False,
            as_dataframe=False,
            **kwargs,
        )
        value = values[-1] if values else default
        return self.serialize(value) if as_string else value

    def _decode_value_from_filepath(self, filepath):
        ext = os.path.splitext(filepath)[1]
        if not ext:
            return super().decode_value_from_filepath(filepath)
        return MetaDataFrame.read(filepath, df_engine=self.df_engine)

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
        for key, df in self.items(
            all_results=all_results, with_metadata=with_metadata, as_dataframe=True
        ):
            dfs.append(
                df.assign(_key=key) if not isinstance(key, dict) else df.assign(**key)
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
        df = self.assemble_df(
            all_results=all_results,
            with_metadata=with_metadata,
            **kwargs,
        )
        ld = df.reset_index().to_dict(orient="records")
        return filter_ld(ld, no_nan=True)
