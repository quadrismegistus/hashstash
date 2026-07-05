"""The dataframe engine stores DataFrames natively via feather/parquet, which
preserve dtypes — including nullable extension dtypes (Int64/boolean),
datetime, and categorical. Regression guard: a previous blanket str() coercion
on write + type re-inference on read silently dropped every extension dtype."""
import tempfile

import pytest

pytest.importorskip("pandas")
pytest.importorskip("pyarrow")
import pandas as pd

from hashstash import HashStash


def _rich_df():
    return pd.DataFrame({
        "int": [1, 2, 3],
        "nullable_int": pd.array([1, 2, None], dtype="Int64"),
        "float": [1.5, 2.5, 3.5],
        "str": ["x", "y", "z"],
        "dt": pd.to_datetime(["2021-01-01", "2021-06-15", "2021-12-31"]),
        "cat": pd.Categorical(["lo", "hi", "lo"]),
        "nullable_bool": pd.array([True, False, None], dtype="boolean"),
    })


@pytest.mark.parametrize("io_engine", ["feather", "parquet"])
def test_dataframe_engine_preserves_dtypes(io_engine, tmp_path):
    df = _rich_df()
    stash = HashStash(engine="dataframe", io_engine=io_engine, root_dir=str(tmp_path / io_engine))
    stash["k"] = df
    got = stash["k"]
    assert dict(got.dtypes.astype(str)) == dict(df.dtypes.astype(str))
    assert got.equals(df)


def test_dataframe_engine_default_io_is_lossless(tmp_path):
    # the default io_engine resolves to feather, so the out-of-the-box engine
    # preserves nullable dtypes without any extra configuration
    df = _rich_df()
    stash = HashStash(engine="dataframe", root_dir=str(tmp_path / "default"))
    stash["k"] = df
    assert stash["k"]["nullable_int"].dtype == "Int64"
    assert stash["k"].equals(df)


def test_dataframe_engine_object_columns_still_stored(tmp_path):
    # object columns holding arrow-unfriendly values (lists/dicts) must not crash
    # the write, and typed columns alongside them keep their dtype
    df = pd.DataFrame({"i": pd.array([1, 2], dtype="Int64"), "obj": [[1, 2], {"x": 1}]})
    stash = HashStash(engine="dataframe", io_engine="feather", root_dir=str(tmp_path / "obj"))
    stash["k"] = df
    assert stash["k"]["i"].dtype == "Int64"
