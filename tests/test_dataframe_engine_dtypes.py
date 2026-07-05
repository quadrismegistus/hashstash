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


@pytest.mark.parametrize("io_engine", ["feather", "parquet"])
def test_assemble_df_renders_readable_keys(io_engine, tmp_path):
    # assemble_df must render the key as its readable value ("Animal 1"), not the
    # serialized bytes (b'"Animal 1"') the engine used to emit.
    stash = HashStash(engine="dataframe", io_engine=io_engine, root_dir=str(tmp_path / io_engine))
    stash["Animal 1"] = pd.DataFrame({"v": [1, 2]})
    adf = stash.assemble_df()
    assert adf.index.name == "_key"
    assert set(adf.index) == {"Animal 1"}
    ld = stash.assemble_ld()
    assert all(row["_key"] == "Animal 1" for row in ld)


@pytest.mark.parametrize("io_engine", ["feather", "parquet"])
def test_dataframe_engine_list_column_roundtrips_as_list(io_engine, tmp_path):
    # Arrow stores list-of-primitive columns natively, so a list column must come
    # back as real lists -- NOT silently coerced to its str() repr ("['a', 'b']")
    # by a blanket object->str stringify on write.
    df = pd.DataFrame({"id": [1, 2], "tags": [["a", "b"], ["c"]]})
    stash = HashStash(engine="dataframe", io_engine=io_engine, root_dir=str(tmp_path / io_engine))
    stash["k"] = df
    got = stash["k"]
    assert got["tags"].iloc[0] == ["a", "b"]
    assert got["tags"].iloc[1] == ["c"]
    assert all(isinstance(v, list) for v in got["tags"])
