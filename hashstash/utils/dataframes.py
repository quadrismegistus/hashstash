from . import *


class MetaDataFrame:
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

        self.prefix_index_cols = prefix_index_cols
        self.reset_prefix = reset_prefix

        if isinstance(data, MetaDataFrame):
            if data.is_pandas != self.is_pandas:
                data = data.data
        
        self.data = data
        
        

    @cached_property
    def df(self):
        return (
            self.get_pandas_df(self.data, prefix_index_cols=self.prefix_index_cols)
            if self.is_pandas
            else self.get_polars_df(self.data, prefix_index_cols=self.prefix_index_cols)
        )

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

    @property
    def is_pandas(self):
        return self.df_engine == "pandas"

    def __getattr__(self, name):
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

    def to_dict(self):
        return {
            'data': self.reset_index().to_pandas().df,
            'df_engine': self.df_engine,
        }

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
            return self.df.write_ipc(path, **kwargs)

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
        return (self.__class__.from_dict, (self.to_dict(),))
    
    @classmethod
    def from_dict(cls, data):
        return cls(**data)

    def concat(self, *others):
        others = [
            MetaDataFrame(other, 'pandas').reset_index()
            for other in others
        ]
        self_ri = self.to_pandas().reset_index()
        dfs = [self_ri.df] + [x.df for x in others]
        import pandas as pd
        df = pd.concat(dfs)
        return MetaDataFrame(df, self.df_engine)

    def to_pandas(self):
        if self.is_pandas:
            return self
        else:
            return MetaDataFrame(self.df, "pandas")

    def to_polars(self):
        if not self.is_pandas:
            return self
        else:
            return MetaDataFrame(self.df, "polars")

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

    def write(self, path: str, io_engine: str = None, **kwargs):
        """
        Write the DataFrame to a file using the specified I/O engine.

        Args:
            path (str): The path to save the file.
            io_engine (str, optional): The I/O engine to use. If None, it will be inferred from the file extension.
            **kwargs: Additional keyword arguments to pass to the specific write method.

        Raises:
            ValueError: If the I/O engine is not supported or installed.
        """
        if io_engine is None:
            io_engine = path.split(".")[-1].lower()
            log.info(f"inferring io_engine from file extension: {io_engine}")
        io_engine = get_io_engine(io_engine)
        # Write the io_engine to a separate file
        if path.split(".")[-1].lower() != io_engine:
            path = path + "." + io_engine
        log.info(f"writing to {path} with {io_engine}")

        if io_engine == "csv":
            return self.to_csv(path, **kwargs)
        elif io_engine == "parquet":
            return self.to_parquet(path, **kwargs)
        elif io_engine == "json":
            return self.to_json(path, **kwargs)
        elif io_engine == "feather":
            return self.to_feather(path, **kwargs)
        elif io_engine == "pickle":
            if self.is_pandas:
                return self.df.to_pickle(path, **kwargs)
            else:
                import pandas as pd

                return pd.DataFrame(self.df).to_pickle(path, **kwargs)
        else:
            raise ValueError(f"Unsupported I/O engine: {io_engine}")

    @classmethod
    def read(cls, path: str, io_engine: str = None, df_engine: str = None, **kwargs):
        """
        Read a DataFrame from a file using the specified I/O engine.

        Args:
            path (str): The path to read the file from.
            io_engine (str, optional): The I/O engine to use. If None, it will be inferred from the file extension or the .type file.
            df_engine (str, optional): The DataFrame engine to use (pandas or polars).
            **kwargs: Additional keyword arguments to pass to the specific read method.

        Returns:
            MetaDataFrame: A new MetaDataFrame instance containing the read data.

        Raises:
            ValueError: If the I/O engine is not supported or installed.
        """
        # Try to read the io_engine from the .type file
        if io_engine is None:
            io_engine = path.split(".")[-1].lower()
            log.info(f"inferring io_engine from file extension: {io_engine}")

        io_engine = get_io_engine(io_engine)
        df_engine = get_df_engine(df_engine)

        if df_engine == "pandas":
            import pandas as pd

            if io_engine == "csv":
                df = pd.read_csv(path, **kwargs)
            elif io_engine == "parquet":
                df = pd.read_parquet(path, **kwargs)
            elif io_engine == "json":
                df = pd.read_json(path, **kwargs)
            elif io_engine == "feather":
                df = pd.read_feather(path, **kwargs)
            elif io_engine == "pickle":
                df = pd.read_pickle(path, **kwargs)
            else:
                raise ValueError(f"Unsupported I/O engine: {io_engine}")
        else:  # polars
            import polars as pl

            if io_engine == "csv":
                df = pl.read_csv(path, **kwargs)
            elif io_engine == "parquet":
                df = pl.read_parquet(path, **kwargs)
            elif io_engine == "json":
                df = pl.read_json(path, **kwargs)
            elif io_engine == "feather":
                df = pl.read_ipc(path, **kwargs)
            elif io_engine == "pickle":
                import pandas as pd

                df = pl.DataFrame(pd.read_pickle(path, **kwargs))
            else:
                raise ValueError(f"Unsupported I/O engine: {io_engine}")

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


@fcache
def get_working_io_engines():
    """
    Determine which I/O engines are available based on current pip installations.

    Returns:
        list: A list of available I/O engine names.
    """
    working_engines = ["csv", "json", "pickle"]

    # Check for specific engines
    try:
        import pyarrow

        working_engines.extend(["parquet", "feather"])
    except ImportError:
        pass

    return set(working_engines)


def get_io_engine(io_engine=None):
    if io_engine is None:
        if check_io_engine(OPTIMAL_DATAFRAME_IO_ENGINE):
            return OPTIMAL_DATAFRAME_IO_ENGINE
        return DEFAULT_DATAFRAME_IO_ENGINE
    if check_io_engine(io_engine):
        return io_engine
    raise ValueError(
        f"IO engine {io_engine} not found or installed. Please choose one of: {get_working_io_engines()}"
    )


def check_io_engine(io_engine):
    return io_engine in get_working_io_engines()


def get_working_df_engines() -> Set[str]:
    working_engines = set()
    try:
        import pandas

        working_engines.add("pandas")
    except ImportError:
        pass

    try:
        import polars

        working_engines.add("polars")
    except ImportError:
        pass

    return working_engines


def check_df_engine(df_engine):
    return df_engine in get_working_df_engines()


def get_df_engine(df_engine=None):
    if df_engine is None:
        if check_df_engine(OPTIMAL_DATAFRAME_DF_ENGINE):
            return OPTIMAL_DATAFRAME_DF_ENGINE
        return DEFAULT_DATAFRAME_DF_ENGINE
    if check_df_engine(df_engine):
        return df_engine
    raise ValueError(
        f"DF engine {df_engine} not found or installed. Please choose one of: {get_working_df_engines()}"
    )


def get_dataframe_engine(df):
    if not is_dataframe(df):
        return
    if isinstance(df, MetaDataFrame):
        return df.df_engine
    return get_obj_addr(df).split(".")[0]


def reset_index(df, prefix_columns=None):
    if has_index(df): # pandas
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
        raise ValueError('not a dataframe')
    if get_dataframe_engine(df) == "pandas":
        return len([x for x in df.index.names if x is not None]) > 0
    elif get_dataframe_engine(df) == "polars":
        return False
    else:
        raise ValueError(
            "Unsupported DataFrame type. Use either pandas or polars DataFrame."
        )
