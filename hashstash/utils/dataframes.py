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


def _object_columns(df):
    return [c for c in df.columns if str(df[c].dtype) == "object"]


def _arrays_to_lists(df):
    """Arrow (feather/parquet) reads its list columns back as numpy ndarrays;
    convert those cells to real Python lists so a list-of-strings column
    round-trips as ``["a", "b"]`` rather than ``array(['a', 'b'])``. Non-array
    object cells (strings, dicts, None) are left untouched."""
    import numpy as np

    for c in _object_columns(df):
        s = df[c]
        if any(isinstance(x, np.ndarray) for x in s):
            df[c] = [x.tolist() if isinstance(x, np.ndarray) else x for x in s]


def _stringify_columns(df, columns):
    """Coerce the named columns to their str() form (they hold values arrow
    can't store natively); every other column keeps its dtype."""
    if not columns:
        return df
    df = df.copy()
    for c in columns:
        df[c] = df[c].astype(str)
    return df


def _stringify_all(df):
    # pandas 2.1+ renamed DataFrame.applymap to DataFrame.map; support both
    mapper = getattr(df, "map", None)
    return (mapper if callable(mapper) else df.applymap)(str)


def _is_arrow_conversion_error(e):
    """True for the errors Arrow raises when a column holds values it can't
    serialize (mixed list/dict, custom objects, ...)."""
    try:
        import pyarrow as pa

        if isinstance(e, (pa.ArrowInvalid, pa.ArrowTypeError, pa.ArrowNotImplementedError)):
            return True
    except Exception:
        pass
    return isinstance(e, (ValueError, TypeError))


def _arrow_unserializable_object_columns(df):
    """Object-dtype columns Arrow can't natively store, found by attempting the
    per-column Arrow conversion. list-of-primitive and struct (dict) columns
    convert fine and are left untouched; only genuinely unserializable columns
    (mixed list/dict, custom objects) come back."""
    try:
        import pyarrow as pa
    except Exception:
        return _object_columns(df)
    bad = []
    for c in _object_columns(df):
        try:
            pa.array(df[c])
        except Exception as e:
            if _is_arrow_conversion_error(e):
                bad.append(c)
            else:
                raise
    return bad


def _reset_buffer(path_or_buffer):
    # a failed native write to a buffer may have left bytes behind; rewind +
    # truncate so the stringified retry doesn't append to a partial stream
    if not isinstance(path_or_buffer, str):
        try:
            path_or_buffer.seek(0)
            path_or_buffer.truncate()
        except Exception:
            pass


def _write_arrow(df, path_or_buffer, write_fn):
    """Write via Arrow (feather/parquet), which stores object columns holding
    lists-of-primitives or dicts natively — so a list column round-trips as a
    real list, not its str() repr. Only if the native write raises an Arrow
    conversion error do we stringify the offending object columns and retry, so
    a genuinely unserializable column degrades to str() instead of crashing."""
    try:
        return write_fn(df, path_or_buffer)
    except Exception as e:
        if not _is_arrow_conversion_error(e):
            raise
        bad = _arrow_unserializable_object_columns(df) or _object_columns(df)
        _reset_buffer(path_or_buffer)
        return write_fn(_stringify_columns(df, bad), path_or_buffer)


def write_df(df, path_or_buffer, io_engine=None, compression=None, string_values=None):
    """Write a DataFrame to path/buffer with the given io engine.

    feather/parquet preserve dtypes natively (incl. nullable Int64/boolean,
    datetime, categorical) and store object columns holding lists/dicts as real
    Arrow list/struct values — so those columns are written natively first and
    only stringified (per column) if Arrow can't serialize them; csv/json are
    text formats and lose dtypes (re-inferred on read).
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

    # None means no compression here (symmetric with read_df); each format then
    # drops tokens it doesn't understand (e.g. the 'raw' the engines pass)
    if io_engine == "csv":
        if compression not in {'infer', 'gzip', 'bz2', 'zip', 'xz', None}:
            compression = None
        return df.to_csv(path_or_buffer, index=False, compression=compression)
    elif io_engine == "parquet":
        if compression not in {'snappy', 'gzip', 'brotli', None}:
            compression = None
        return _write_arrow(
            df, path_or_buffer,
            lambda d, p: d.to_parquet(p, compression=compression),
        )
    elif io_engine == "json":
        if compression not in {'infer', 'gzip', 'bz2', 'zip', 'xz', None}:
            compression = None
        return df.to_json(path_or_buffer, orient="records", compression=compression)
    elif io_engine == "feather":
        if compression not in {'zstd', 'lz4', 'uncompressed'}:
            compression = None
        return _write_arrow(
            df, path_or_buffer,
            lambda d, p: d.to_feather(p, compression=compression),
        )
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
    # Arrow returns its native list columns as numpy ndarrays; hand back real
    # Python lists so a list column round-trips as a list, not an ndarray.
    if io_engine in {"feather", "parquet"}:
        _arrays_to_lists(df)
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
