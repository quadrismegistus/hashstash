"""stash.sql() / stash.duckdb() on the dataframe engine: SQL over the cached
DataFrames by scanning their parquet files in place (no deserialization)."""
import tempfile

import pytest

pytest.importorskip("pandas")
pytest.importorskip("duckdb")
pytest.importorskip("pyarrow")
import pandas as pd

from hashstash import HashStash


@pytest.fixture
def stash(tmp_path):
    s = HashStash(engine="dataframe", io_engine="parquet", root_dir=str(tmp_path / "df"))
    for city, temp in [("NYC", 22), ("LA", 30), ("SF", 18)]:
        s[city] = pd.DataFrame({"city": [city] * 3, "hour": [9, 12, 15],
                                "temp": [temp, temp + 3, temp + 1]})
    return s


def test_sql_aggregates_across_cached_frames(stash):
    out = stash.sql("SELECT city, avg(temp) AS t FROM data GROUP BY city ORDER BY t DESC")
    assert isinstance(out, pd.DataFrame)
    assert list(out["city"]) == ["LA", "NYC", "SF"]
    assert round(out.iloc[0]["t"], 2) == 31.33


def test_sql_row_count(stash):
    assert stash.sql("SELECT count(*) AS n FROM data").iloc[0]["n"] == 9


def test_duckdb_connection_for_multiple_queries(stash):
    con = stash.duckdb()
    assert con.sql("SELECT count(DISTINCT city) FROM data").fetchone()[0] == 3
    assert con.sql("SELECT max(temp) FROM data").fetchone()[0] == 33


def test_custom_table_name(stash):
    out = stash.sql("SELECT count(*) AS n FROM readings", table="readings")
    assert out.iloc[0]["n"] == 9


def test_empty_stash_returns_empty(tmp_path):
    s = HashStash(engine="dataframe", io_engine="parquet", root_dir=str(tmp_path / "e"))
    assert len(s.sql("SELECT * FROM data")) == 0


def test_requires_parquet_io_engine(tmp_path):
    s = HashStash(engine="dataframe", io_engine="feather", root_dir=str(tmp_path / "f"))
    with pytest.raises(ValueError, match="parquet"):
        s.sql("SELECT 1")
