# Explicit stdlib imports: this package's `from . import *` chains are
# circular, and whether a name has landed in the package namespace yet
# depends on import order (spawn workers + editable installs order imports
# differently). Never rely on the star-chain for stdlib names.
import io

from . import *

DEFAULT_COMPRESS = 'gzip'  # default compression for the dataframe io engines


# DataFrame io for the `dataframe` engine. pandas-only: a polars DataFrame given
# as input is converted to pandas on the way in (see to_pandas). Formerly this
# lived in a MetaDataFrame wrapper class that abstracted pandas vs polars; that
# abstraction was dropped — the engine and assemble_df return plain pandas.


def to_pandas(df):
    """Return a pandas DataFrame, converting a polars DataFrame if given one."""
    if get_dataframe_engine(df) == "polars":
        return df.to_pandas()
    return df


def _stringify_object_columns(df):
    """Coerce only object-dtype columns to str (they may hold lists/dicts arrow
    can't store); typed columns keep their dtype."""
    obj_cols = [c for c in df.columns if str(df[c].dtype) == "object"]
    if not obj_cols:
        return df
    df = df.copy()
    for c in obj_cols:
        df[c] = df[c].astype(str)
    return df


def _stringify_all(df):
    # pandas 2.1+ renamed DataFrame.applymap to DataFrame.map; support both
    mapper = getattr(df, "map", None)
    return (mapper if callable(mapper) else df.applymap)(str)


def write_df(df, path_or_buffer, io_engine=None, compression=None, string_values=None):
    """Write a DataFrame to path/buffer with the given io engine.

    feather/parquet preserve dtypes natively (incl. nullable Int64/boolean,
    datetime, categorical), so only object columns are coerced to str; csv/json
    are text formats and lose dtypes (re-inferred on read).
    """
    df = to_pandas(df)
    io_engine = get_io_engine(io_engine)
    if (
        isinstance(path_or_buffer, str)
        and path_or_buffer.split(".")[-1].lower() != io_engine
    ):
        path_or_buffer = path_or_buffer + "." + io_engine

    if string_values:
        df = _stringify_all(df)
    elif io_engine in {"feather", "parquet"}:
        df = _stringify_object_columns(df)

    # None means no compression here (symmetric with read_df); each format then
    # drops tokens it doesn't understand (e.g. the 'raw' the engines pass)
    if io_engine == "csv":
        if compression not in {'infer', 'gzip', 'bz2', 'zip', 'xz', None}:
            compression = None
        return df.to_csv(path_or_buffer, index=False, compression=compression)
    elif io_engine == "parquet":
        if compression not in {'snappy', 'gzip', 'brotli', None}:
            compression = None
        return df.to_parquet(path_or_buffer, compression=compression)
    elif io_engine == "json":
        if compression not in {'infer', 'gzip', 'bz2', 'zip', 'xz', None}:
            compression = None
        return df.to_json(path_or_buffer, orient="records", compression=compression)
    elif io_engine == "feather":
        if compression not in {'zstd', 'lz4', 'uncompressed'}:
            compression = None
        return df.to_feather(path_or_buffer, compression=compression)
    elif io_engine == "pickle":
        if compression not in {'infer', 'gzip', 'bz2', 'zip', 'xz', None}:
            compression = None
        return df.to_pickle(path_or_buffer, compression=compression)
    raise ValueError(f"Unsupported I/O engine: {io_engine}")


def read_df(path_or_buffer, io_engine=None, compression=None):
    """Read a DataFrame (always pandas) from path/buffer with the given io engine."""
    import pandas as pd

    if io_engine is None and isinstance(path_or_buffer, str):
        io_engine = path_or_buffer.split(".")[-1].lower()
    io_engine = get_io_engine(io_engine)

    if io_engine == "csv":
        if compression not in {'infer', 'gzip', 'bz2', 'zip', 'xz', None}:
            compression = None
        df = pd.read_csv(path_or_buffer, compression=compression)
    elif io_engine == "parquet":
        df = pd.read_parquet(path_or_buffer)
    elif io_engine == "json":
        if compression not in {'infer', 'gzip', 'bz2', 'zip', 'xz', None}:
            compression = None
        df = pd.read_json(path_or_buffer, compression=compression)
    elif io_engine == "feather":
        df = pd.read_feather(path_or_buffer)
    elif io_engine == "pickle":
        if compression not in {'infer', 'gzip', 'bz2', 'zip', 'xz', None}:
            compression = None
        df = pd.read_pickle(path_or_buffer, compression=compression)
    else:
        raise ValueError(f"Unsupported I/O engine: {io_engine}")

    # csv/json are text formats that lose dtypes -> re-infer them. feather/
    # parquet/pickle carry their own schema; re-inferring there would corrupt
    # legit string columns (e.g. '1','2' -> ints).
    if io_engine in {"csv", "json"}:
        reinfer_types(df)
    return df


def concat_dfs(dfs):
    """Concatenate pandas DataFrames, resetting each index first."""
    import pandas as pd

    frames = [reset_index(to_pandas(d)) for d in dfs]
    return pd.concat(frames) if frames else pd.DataFrame()


def reset_index(df, prefix_columns=None):
    if has_index(df):  # pandas
        index = [x for x in df.index.names if x is not None]
        df = df.reset_index()
        if prefix_columns is not None:
            df.columns = [
                (
                    f"{prefix_columns if not x.startswith(prefix_columns) else ''}{x}"
                    if x in index
                    else x
                )
                for x in df.columns
            ]
        return df
    return df


def set_index(
    df,
    index_columns=None,
    prefix_columns=None,
    reset_prefix=False,
    except_columns={"_value"},
):
    if get_dataframe_engine(df) != "pandas":  # must be pandas
        log.debug("can only set index on pandas df")
        return df

    assert index_columns or prefix_columns
    if has_index(df):
        df = reset_index(
            df,
            prefix_columns=(
                prefix_columns if prefix_columns and not index_columns else None
            ),
        )

    if not index_columns:
        index_columns = [
            c[len(prefix_columns) :] if reset_prefix else c
            for c in df
            if c.startswith(prefix_columns) and c not in except_columns
        ]
        df.columns = [
            (
                c[len(prefix_columns) :]
                if reset_prefix and c.startswith(prefix_columns)
                else c
            )
            for c in df
        ]
    else:
        index_columns = [c for c in index_columns if c in df.columns]
    return df.set_index(index_columns) if index_columns else df


def has_index(df):
    if not is_dataframe(df):
        raise ValueError("not a dataframe")
    if get_dataframe_engine(df) == "pandas":
        return len([x for x in df.index.names if x is not None]) > 0
    elif get_dataframe_engine(df) == "polars":
        return False
    else:
        raise ValueError(
            "Unsupported DataFrame type. Use either pandas or polars DataFrame."
        )


def reinfer_types(df):
    import pandas as pd
    import warnings

    # Infer types for pandas DataFrame. errors='ignore' was removed from to_numeric/to_datetime
    # in pandas 3.0; catch explicitly to preserve the "leave column alone if conversion fails"
    # semantics across pandas versions.
    for column in df.columns:
        try:
            df[column] = pd.to_numeric(df[column])
        except (ValueError, TypeError):
            pass
        if df[column].dtype == "object":
            try:
                with warnings.catch_warnings():
                    # per-element format inference is exactly what we're asking
                    # for here; don't spam every CSV read with the warning
                    warnings.simplefilter("ignore", UserWarning)
                    df[column] = pd.to_datetime(df[column])
            except (ValueError, TypeError):
                pass
