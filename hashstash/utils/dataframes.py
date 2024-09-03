from . import *

DEFAULT_COMPRESS = 'gzip'  # Define your default compression method here

class MetaDataFrame:
    to_dict_keys = ["data", "df_engine", "prefix_index_cols", "reset_prefix"]

    def __init__(
        self,
        data: Union[Dict, List[Dict]],
        df_engine: str = None,
        prefix_index_cols="_",
        reset_prefix=None,
    ):
        if df_engine is None and is_dataframe(data):
            self.df_engine = get_dataframe_engine(data)
        else:
            self.df_engine = get_df_engine(df_engine)
        self.is_pandas = self.df_engine == "pandas"

        self.prefix_index_cols = prefix_index_cols
        self.reset_prefix = reset_prefix

        if isinstance(data, MetaDataFrame):
            if data.is_pandas != self.is_pandas:
                data = data.data

        self.data = data
        self._df = None

    def to_dict(self):
        return {k: getattr(self, k, None) for k in self.to_dict_keys}

    @classmethod
    def from_dict(cls, data):
        opts = {k:data.get(k) for k in cls.to_dict_keys}
        return cls(**opts)

    @property
    def df(self):
        if self._df is None:
            self._df = (
                self.get_pandas_df(self.data, prefix_index_cols=self.prefix_index_cols)
                if self.is_pandas
                else self.get_polars_df(
                    self.data, prefix_index_cols=self.prefix_index_cols
                )
            )
        return self._df

    @staticmethod
    def get_polars_df(data, prefix_index_cols=None):
        df_engine = get_dataframe_engine(data)
        if df_engine == "polars":
            return data
        if df_engine == "pandas":
            data = reset_index(data, prefix_columns=prefix_index_cols)
        import polars as pl

        return pl.DataFrame(data) if not isinstance(data, pl.DataFrame) else data

    @staticmethod
    def get_pandas_df(data, prefix_index_cols=None):
        df_engine = get_dataframe_engine(data)
        if df_engine == "pandas":
            return data

        if df_engine == "polars":
            return data.to_pandas()

        import pandas as pd

        return pd.DataFrame(data)

    def __getattr__(self, name):
        if name in self.__dict__:
            return self.__dict__[name]
        if hasattr(self.df, name):
            attr = getattr(self.df, name)
            if callable(attr):

                def wrapper(*args, **kwargs):
                    result = attr(*args, **kwargs)
                    if self.is_pandas:
                        import pandas as pd

                        if isinstance(result, pd.DataFrame):
                            return MetaDataFrame(result, self.df_engine)
                    else:
                        import polars as pl

                        if isinstance(result, pl.DataFrame):
                            return MetaDataFrame(result, self.df_engine)
                    return result

                return wrapper
            return attr
        raise AttributeError(
            f"'{type(self).__name__}' object has no attribute '{name}'"
        )

    def __getitem__(self, key):
        result = self.df[key]
        if self.is_pandas:
            import pandas as pd

            if isinstance(result, pd.DataFrame):
                return MetaDataFrame(result, self.df_engine)
        else:
            import polars as pl

            if isinstance(result, pl.DataFrame):
                return MetaDataFrame(result, self.df_engine)
        return result

    def __setitem__(self, key, value):
        if self.is_pandas:
            self.df[key] = value
        else:
            import polars as pl

            if isinstance(value, pl.Series):
                self.df = self.df.with_columns(value.alias(key))
            else:
                self.df = self.df.with_columns(pl.lit(value).alias(key))

    def __len__(self):
        return len(self.df)

    def filter(self, mask):
        if self.is_pandas:
            return MetaDataFrame(self.df[mask], self.df_engine)
        else:
            return MetaDataFrame(self.df.filter(mask), self.df_engine)

    def select_columns(self, columns):
        if self.is_pandas:
            return MetaDataFrame(self.df[columns], self.df_engine)
        else:
            return MetaDataFrame(self.df.select(columns), self.df_engine)

    def applymap(self, func):
        if self.is_pandas:
            return MetaDataFrame(self.df.applymap(func), self.df_engine)
        else:
            import polars as pl

            return MetaDataFrame(
                self.df.with_columns(pl.all().map(func)), self.df_engine
            )

    def max(self):
        return self.df.max()

    def __eq__(self, other):
        from ..serializers import serialize_custom

        other = MetaDataFrame(other) if not isinstance(other, MetaDataFrame) else other
        return serialize_custom(self) == serialize_custom(other)

    def to_csv(self, path: str, index: bool = False, **kwargs):
        if self.is_pandas:
            return self.df.to_csv(path, index=index, **kwargs)
        else:
            return self.df.write_csv(path, **kwargs)

    def to_parquet(self, path: str, **kwargs):
        if self.is_pandas:
            return self.df.to_parquet(path, **kwargs)
        else:
            return self.df.write_parquet(path, **kwargs)

    def to_json(self, path: str = None, orient: str = "records", **kwargs):
        if self.is_pandas:
            return self.df.to_json(path, orient=orient, **kwargs)
        else:
            return self.df.write_json(path, **kwargs)

    def to_excel(
        self, path: str, sheet_name: str = "Sheet1", index: bool = False, **kwargs
    ):
        if self.is_pandas:
            return self.df.to_excel(path, sheet_name=sheet_name, index=index, **kwargs)
        else:
            raise NotImplementedError(
                "Polars does not have a native to_excel method. Consider converting to pandas first."
            )

    def to_feather(self, path: str, **kwargs):
        if self.is_pandas:
            return self.df.to_feather(path, **kwargs)
        else:
            return self.to_pandas().to_feather(path, **kwargs)
            # return self.df.write_ipc(path, **kwargs)

    def to_sql(self, name: str, con, **kwargs):
        if self.is_pandas:
            return self.df.to_sql(name, con, **kwargs)
        else:
            raise NotImplementedError(
                "Polars does not have a native to_sql method. Consider converting to pandas first."
            )

    def merge(self, right, how: str = "inner", on: Union[str, List[str]] = None):
        if self.is_pandas:
            return MetaDataFrame(
                self.df.merge(
                    right.df if isinstance(right, MetaDataFrame) else right,
                    how=how,
                    on=on,
                ),
                self.df_engine,
            )
        else:
            return MetaDataFrame(
                self.df.join(
                    right.df if isinstance(right, MetaDataFrame) else right,
                    how=how,
                    on=on,
                ),
                self.df_engine,
            )

    def __reduce__(self):
        # Return a tuple of (callable, args) that allows recreation of this object
        return (MetaDataFrame.from_dict, (self.to_dict(),))

    def concat(self, *others):
        others = [MetaDataFrame(other, "pandas").reset_index() for other in others]
        self_ri = self.to_pandas().reset_index()
        dfs = [self_ri.df] + [x.df for x in others]
        import pandas as pd

        df = pd.concat(dfs)
        return MetaDataFrame(df, self.df_engine)

    def to_pandas(self):
        if self.is_pandas:
            return self
        else:
            return MetaDataFrame(self.data, "pandas")

    def to_polars(self):
        if not self.is_pandas:
            return self
        else:
            return MetaDataFrame(self.data, "polars")

    def reset_index(self):
        if has_index(self.df):
            return MetaDataFrame(reset_index(self.df), self.df_engine)
        return self

    def set_index(self, index=None, prefix=None, reset_prefix=None):
        return MetaDataFrame(
            set_index(
                self.df,
                index_columns=index,
                prefix_columns=prefix if prefix is not None else self.prefix_index_cols,
                reset_prefix=(
                    reset_prefix if reset_prefix is not None else self.reset_prefix
                ),
            ),
            self.df_engine,
        )

    # Serialize DataFrame to bytes
    def encode(self, io_engine: str = None, string_values: bool = None, **kwargs):
        return encode(self.serialize(io_engine, string_values, **kwargs))

    def serialize(self, io_engine: str = None, string_values: bool = None, **kwargs):
        from ..serializers import serialize

        return serialize(self.stuff(io_engine, string_values, **kwargs))

    def stuff(self, io_engine: str = None, string_values: bool = None, **kwargs):
        from ..serializers import stuff

        buffer = io.BytesIO()
        io_engine = get_io_engine(io_engine)
        self.write(
            buffer,
            io_engine=io_engine,
            string_values=string_values,
            **kwargs,
        )
        serialized_df = buffer.getvalue()
        return stuff(
            {
                "data": b64encode(serialized_df).decode(),
                "df_engine": self.df_engine,
                "io_engine": io_engine,
            }
        )

    @classmethod
    def decode(cls, encoded_df):
        return cls.deserialize(decode(encoded_df))

    @classmethod
    def deserialize(cls, serialized_data):
        from ..serializers import deserialize

        stuffed_data = deserialize(serialized_data)
        return cls.unstuff(stuffed_data)

    @classmethod
    def unstuff(cls, stuffed_data):
        from ..serializers import unstuff

        unstuffed_data = unstuff(stuffed_data)
        serialized_df_b = b64decode(unstuffed_data["data"].encode())
        io_engine = unstuffed_data["io_engine"]
        df_engine = unstuffed_data["df_engine"]
        buffer = io.BytesIO(serialized_df_b)
        return cls.read(buffer, io_engine=io_engine, df_engine=df_engine)

    def write(
        self, path_or_buffer, io_engine: str = None, string_values=None, compression=None, **kwargs
    ):
        """
        Write the DataFrame to a file or buffer using the specified I/O engine.

        Args:
            path_or_buffer: The path to save the file or a file-like object.
            io_engine (str, optional): The I/O engine to use. If None, it will be inferred from the file extension.
            string_values (bool): Whether to convert all values to strings before writing.
            compression (str, optional): Compression to use (e.g., 'gzip', 'bz2', 'zip', 'xz').
            **kwargs: Additional keyword arguments to pass to the specific write method.

        Raises:
            ValueError: If the I/O engine is not supported or installed.
        """
        if io_engine is None and isinstance(path_or_buffer, str):
            io_engine = path_or_buffer.split(".")[-1].lower()
            log.debug(f"inferring io_engine from file extension: {io_engine}")
        io_engine = get_io_engine(io_engine)

        if (
            isinstance(path_or_buffer, str)
            and path_or_buffer.split(".")[-1].lower() != io_engine
        ):
            path_or_buffer = path_or_buffer + "." + io_engine

        log.debug(f"writing with {io_engine}")
        if io_engine in {"feather", "parquet"}:
            string_values = True

        if string_values:
            self = self.applymap(str)

        if compression is None:
            compression = DEFAULT_COMPRESS

        if io_engine == "csv":
            if compression not in {'infer', 'gzip', 'bz2', 'zip', 'xz', None}:
                compression = None
            return self.to_csv(path_or_buffer, compression=compression, **kwargs)
        elif io_engine == "parquet":
            if compression not in {'snappy', 'gzip', 'brotli', None}:
                compression = None
            return self.to_parquet(path_or_buffer, compression=compression, **kwargs)
        elif io_engine == "json":
            if compression not in {'infer', 'gzip', 'bz2', 'zip', 'xz', None}:
                compression = None
            return self.to_json(path_or_buffer, compression=compression, **kwargs)
        elif io_engine == "feather":
            if compression not in {'zstd', 'lz4', 'uncompressed'}:
                compression = None
            return self.to_feather(path_or_buffer, compression=compression, **kwargs)
        elif io_engine == "pickle":
            if compression not in {'infer', 'gzip', 'bz2', 'zip', 'xz', None}:
                compression = None
            return self.to_pandas().df.to_pickle(path_or_buffer, compression=compression, **kwargs)
        else:
            raise ValueError(f"Unsupported I/O engine: {io_engine}")

    @classmethod
    def read(
        cls, path_or_buffer, io_engine: str = None, df_engine: str = None, compression=None, **kwargs
    ):
        """
        Read a DataFrame from a file or buffer using the specified I/O engine.

        Args:
            path_or_buffer: The path to read the file from or a file-like object.
            io_engine (str, optional): The I/O engine to use. If None, it will be inferred from the file extension.
            df_engine (str, optional): The DataFrame engine to use (pandas or polars).
            compression (str, optional): Compression to use (e.g., 'gzip', 'bz2', 'zip', 'xz').
            **kwargs: Additional keyword arguments to pass to the specific read method.

        Returns:
            MetaDataFrame: A new MetaDataFrame instance containing the read data.

        Raises:
            ValueError: If the I/O engine is not supported or installed.
        """
        if io_engine is None and isinstance(path_or_buffer, str):
            io_engine = path_or_buffer.split(".")[-1].lower()
            log.debug(f"inferring io_engine from file extension: {io_engine}")

        io_engine = get_io_engine(io_engine)
        df_engine = get_df_engine(df_engine)

        log.debug(f"reading with {df_engine} and {io_engine}")

        if df_engine == "pandas":
            import pandas as pd

            if io_engine == "csv":
                if compression not in {'infer', 'gzip', 'bz2', 'zip', 'xz', None}:
                    compression = None
                df = pd.read_csv(path_or_buffer, compression=compression, **kwargs)
            elif io_engine == "parquet":
                df = pd.read_parquet(path_or_buffer, **kwargs)
            elif io_engine == "json":
                if compression not in {'infer', 'gzip', 'bz2', 'zip', 'xz', None}:
                    compression = None
                df = pd.read_json(path_or_buffer, compression=compression, **kwargs)
            elif io_engine == "feather":
                df = pd.read_feather(path_or_buffer, **kwargs)
            elif io_engine == "pickle":
                if compression not in {'infer', 'gzip', 'bz2', 'zip', 'xz', None}:
                    compression = None
                df = pd.read_pickle(path_or_buffer, compression=compression, **kwargs)
            else:
                raise ValueError(f"Unsupported I/O engine: {io_engine}")

            reinfer_types(df)
        else:  # polars
            import polars as pl

            if io_engine == "csv":
                if compression not in {'gzip', 'zlib', None}:
                    compression = None
                df = pl.read_csv(path_or_buffer, infer_schema_length=10000, compression=compression, **kwargs)
            elif io_engine == "parquet":
                df = pl.read_parquet(path_or_buffer, **kwargs)
            elif io_engine == "json":
                if compression not in {'gzip', 'zlib', None}:
                    compression = None
                df = pl.read_json(path_or_buffer, infer_schema_length=10000, compression=compression, **kwargs)
            elif io_engine == "feather":
                df = pl.read_ipc(path_or_buffer, **kwargs)
            elif io_engine == "pickle":
                df = cls.read(path_or_buffer, io_engine=io_engine, df_engine="pandas")
                df = pl.DataFrame(df)
            else:
                raise ValueError(f"Unsupported I/O engine: {io_engine}")

        log.debug(f"done reading with {df_engine} and {io_engine}")

        return cls(df, df_engine)

    @property
    def columns(self):
        return self.df.columns

    @property
    def shape(self):
        return self.df.shape

    def __str__(self):
        return str(self.df)

    def __repr__(self):
        return repr(self.df)

    def assign(self, **kwargs):
        """
        Assign new columns to the DataFrame.

        Args:
            **kwargs: Keyword arguments of the form column=value or column=callable.

        Returns:
            MetaDataFrame: A new MetaDataFrame with the assigned columns.
        """
        if self.is_pandas:
            new_df = self.df.assign(**kwargs)
        else:
            import polars as pl

            new_df = self.df.clone()
            for column, value in kwargs.items():
                if callable(value):
                    # If value is a callable, apply it to the DataFrame
                    new_column = value(new_df)
                    if isinstance(new_column, pl.Series):
                        new_df = new_df.with_columns(new_column.alias(column))
                    else:
                        new_df = new_df.with_columns(pl.lit(new_column).alias(column))
                else:
                    # If value is not callable, add it as a new column
                    new_df = new_df.with_columns(pl.lit(value).alias(column))

        return MetaDataFrame(new_df, self.df_engine)



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
        log.warning("can only set index on pandas df")
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

    # Infer types for pandas DataFrame
    for column in df.columns:
        df[column] = pd.to_numeric(df[column], errors="ignore")
        if df[column].dtype == "object":
            df[column] = pd.to_datetime(df[column], errors="ignore")
