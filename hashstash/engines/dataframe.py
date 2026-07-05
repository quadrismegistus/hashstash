# Explicit stdlib imports: this package's `from . import *` chains are
# circular, and whether a name has landed in the package namespace yet
# depends on import order (spawn workers + editable installs order imports
# differently). Never rely on the star-chain for stdlib names.
from typing import Any
import os

from . import *
from .pairtree import PairtreeHashStash
from .base import _filter_by_time
from ..utils.dataframes import to_pandas, write_df, read_df, concat_dfs, set_index, reset_index

class DataFrameHashStash(PairtreeHashStash):
    engine = "dataframe"
    prefix_index_cols = "_"

    def __init__(self, *args, df_engine=None, io_engine=None, **kwargs):
        ## check io engine
        self.io_engine = get_io_engine(io_engine)
        self.df_engine = get_df_engine(df_engine)
        super().__init__(*args, **kwargs)

    def to_dict(self):
        return {**super().to_dict(), 'io_engine': self.io_engine, 'df_engine': self.df_engine}

    def set(self, unencoded_key: Any, unencoded_value: Any, append=None) -> None:
        log.debug(f"Setting value for key: {unencoded_key}")
        # set value as pairtree does if not a dataframe
        if not is_dataframe(unencoded_value):
            log.debug(f"Input is not a DataFrame")
            return super().set(unencoded_key, unencoded_value, append=append)

        # Handle DataFrame values (converted to pandas; polars input is welcome)
        df = to_pandas(unencoded_value)
        log.debug(f"Input is a DataFrame with shape: {df.shape}")

        encoded_key = self.encode_key(unencoded_key)
        self._set_key(encoded_key)
        # the io-engine extension marks this version file as a dataframe; plain
        # pairtree versions end in '.<pid>' and are routed to the base decoder
        filepath_value = f"{self._get_path_new_value(encoded_key)}.{self.io_engine}"
        write_df(df, filepath_value, io_engine=self.io_engine, compression=self.compress)
        if not (append or self.append_mode):
            # honor overwrite semantics like pairtree: without this, every set()
            # accumulated another version file forever
            self._prune_dir(filepath_value)
        self._stats["sets"] += 1

    @log.debug
    def get_all(
        self,
        unencoded_key: Any = None,
        default: Any = None,
        with_metadata=None,
        all_results=None,
        as_dataframe=None,
        as_list=None,
        before=None,
        after=None,
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
        after = self._ttl_after(after, kwargs)
        if before is not None or after is not None:
            timestamps = [p["_written_at"] for p in paths_ld]
            paths_ld, _ = _filter_by_time(paths_ld, timestamps, before=before, after=after)

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
                    import pandas as pd
                    obj = pd.DataFrame(flatten_ld([obj]))

                out_l.append(obj)

        if not out_l:
            return default

        if as_dataframe and not as_list:
            return concat_dfs(out_l) if len(out_l) > 1 else out_l[0]
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
        return self.serialize(value, as_string=True) if as_string else value
        # if values is None:
        #     return default
        
        # if not self._all_results(all_results) and isinstance(values, list):
        #     values = values[-1]
        
        # return self.serialize(values) if as_string else values

    def _decode_value_from_filepath(self, filepath):
        # dataframe version files carry their io-engine as the extension; anything
        # else is a plain pairtree value. Read errors propagate: returning None
        # here silently masked corrupted entries.
        ext = os.path.splitext(filepath)[1].lstrip(".").lower()
        if ext in get_working_io_engines():
            return read_df(filepath, io_engine=ext, compression=self.compress)
        return super().decode_value_from_filepath(filepath)

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
                log.debug(f'empty values returned for {key}')
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
            import pandas as pd
            return pd.DataFrame()
        combined_df = concat_dfs(dfs)
        return set_index(combined_df, prefix_columns=self.prefix_index_cols)

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
        ld = reset_index(to_pandas(mdf)).to_dict(orient="records")
        return filter_ld(ld, no_nan=True)
