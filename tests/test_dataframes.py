"""hashstash.utils.dataframes — pandas-only io + index helpers.

The MetaDataFrame pandas/polars wrapper was removed; the dataframe engine and
assemble_df now return plain pandas DataFrames. A polars DataFrame given as
input is converted to pandas (to_pandas)."""
import tempfile

import pytest

pd = pytest.importorskip("pandas")

from hashstash.utils.dataframes import (
    to_pandas, write_df, read_df, concat_dfs,
    reset_index, set_index, has_index,
)
from hashstash.config import (
    get_io_engine, check_io_engine, get_working_io_engines,
    get_df_engine, check_df_engine, get_dataframe_engine,
)


@pytest.fixture
def df():
    return pd.DataFrame({"A": [1, 2, 3], "B": ["a", "b", "c"], "C": [4.0, 5.0, 6.0]})


# -- io round-trip --

@pytest.mark.parametrize("io_engine", ["csv", "parquet", "json", "feather", "pickle"])
def test_write_read_df_roundtrip(df, io_engine, tmp_path):
    path = str(tmp_path / f"t.{io_engine}")
    write_df(df, path, io_engine=io_engine, compression=None)
    got = read_df(path, io_engine=io_engine, compression=None)
    assert list(got.columns) == list(df.columns)
    assert got.shape == df.shape


@pytest.mark.parametrize("io_engine", ["feather", "parquet"])
def test_native_io_preserves_nullable_dtypes(io_engine, tmp_path):
    df = pd.DataFrame({"a": pd.array([1, 2, None], dtype="Int64"), "b": ["x", "y", "z"]})
    path = str(tmp_path / f"t.{io_engine}")
    write_df(df, path, io_engine=io_engine)
    got = read_df(path, io_engine=io_engine)
    assert str(got["a"].dtype) == "Int64"


def test_write_invalid_engine(df, tmp_path):
    with pytest.raises(ValueError):
        write_df(df, str(tmp_path / "t.x"), io_engine="invalid_engine")


# -- polars input is converted to pandas --

def test_to_pandas_converts_polars():
    pl = pytest.importorskip("polars")
    got = to_pandas(pl.DataFrame({"A": [1, 2, 3]}))
    assert isinstance(got, pd.DataFrame)
    assert got["A"].tolist() == [1, 2, 3]


def test_to_pandas_passthrough(df):
    assert to_pandas(df) is df


# -- concat --

def test_concat_dfs():
    a = pd.DataFrame({"A": [1, 2], "B": ["a", "b"]})
    b = pd.DataFrame({"A": [3, 4], "B": ["c", "d"]})
    got = concat_dfs([a, b])
    assert len(got) == 4
    assert list(got.columns) == ["A", "B"]


# -- index helpers --

def test_reset_index_with_prefix():
    df = pd.DataFrame({"A": [1, 2, 3], "B": ["a", "b", "c"]}).set_index("A")
    got = reset_index(df, prefix_columns="_")
    assert "_A" in got.columns
    assert has_index(got) is False


def test_set_index_columns():
    got = set_index(pd.DataFrame({"A": [1, 2, 3], "B": ["a", "b"] + ["c"]}), index_columns=["A"])
    assert has_index(got) is True
    assert got.index.name == "A"


def test_set_index_with_prefix():
    got = set_index(pd.DataFrame({"_A": [1, 2, 3], "B": ["a", "b", "c"]}),
                    prefix_columns="_", reset_prefix=True)
    assert has_index(got) is True
    assert got.index.name == "A"


def test_has_index_errors_on_non_df():
    with pytest.raises(ValueError):
        has_index([1, 2, 3])


# -- engine detection (polars still detected as input) --

def test_get_dataframe_engine(df):
    assert get_dataframe_engine(df) == "pandas"
    assert get_dataframe_engine([1, 2, 3]) is None
    pl = pytest.importorskip("polars")
    assert get_dataframe_engine(pl.DataFrame({"A": [1]})) == "polars"


def test_io_engine_helpers():
    engines = get_working_io_engines()
    assert isinstance(engines, set) and "csv" in engines
    assert get_io_engine("csv") == "csv"
    with pytest.raises(ValueError):
        get_io_engine("invalid_engine")
    assert check_io_engine("csv") is True
    assert check_io_engine("invalid_engine") is False


def test_df_engine_helpers():
    assert check_df_engine("pandas") is True
    assert get_df_engine("pandas") == "pandas"
    with pytest.raises(ValueError):
        get_df_engine("invalid_engine")
