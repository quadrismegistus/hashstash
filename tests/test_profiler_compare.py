"""compare_serializers() and the non-native 'mixed' generator."""
import logging

import pytest

pytest.importorskip("pandas")

from hashstash import deserialize, serialize
from hashstash.profilers.engine_profiler import HashStashProfiler
from hashstash.profilers.profiler import generate_data, generate_mixed


def test_mixed_generator_exercises_full_path():
    """The 'mixed' payload must contain non-JSON-native types (so the serializer
    takes its full path, not the fast-path)."""
    data = generate_mixed(2000)
    assert isinstance(data["_tuple"], tuple)
    assert isinstance(data["_set"], set)
    assert isinstance(data["_bytes"], bytes)
    # datetime present
    import datetime

    assert isinstance(data["_datetime"], datetime.datetime)


def test_mixed_roundtrips_through_hashstash():
    data = generate_mixed(2000)
    out = deserialize(serialize(data, serializer="hashstash"), serializer="hashstash")
    assert out["_tuple"] == data["_tuple"]
    assert out["_set"] == data["_set"]
    assert out["_bytes"] == data["_bytes"]
    assert out["_datetime"] == data["_datetime"]


def test_generate_data_mixed_type():
    d = generate_data(2000, data_type="mixed")
    assert isinstance(d, dict)
    assert "_set" in d


def test_compare_serializers_fans_out_and_keeps_identity(caplog):
    df = HashStashProfiler.compare_serializers(
        serializers=["hashstash", "pickle", "msgpack"],
        sizes=(3000,),
        iterations=4,
        data_types=("dict", "mixed"),
    )
    d = df.df if hasattr(df, "df") else df
    # every requested serializer is present and identified (the bug: get_profile_data
    # dropped the Serializer column)
    assert "Serializer" in d.columns
    assert set(d["Serializer"].unique()) == {"hashstash", "pickle", "msgpack"}
    assert "Data Type" in d.columns
    # timing columns exist for supported combos
    supported = d[d.get("Unsupported").isna()] if "Unsupported" in d.columns else d
    assert "Serialize Time (s)" in supported.columns
    assert supported["Serialize Time (s)"].notna().any()


def test_compare_serializers_marks_unsupported():
    """msgpack cannot encode the 'mixed' payload (sets) — it must be recorded as
    unsupported, not crash the whole comparison."""
    df = HashStashProfiler.compare_serializers(
        serializers=["hashstash", "msgpack"],
        sizes=(2000,),
        iterations=3,
        data_types=("mixed",),
    )
    d = df.df if hasattr(df, "df") else df
    assert "Unsupported" in d.columns
    unsupported = d[d["Unsupported"] == True]  # noqa: E712
    assert "msgpack" in set(unsupported["Serializer"])
    # hashstash handles mixed, so it is NOT unsupported
    assert "hashstash" not in set(unsupported["Serializer"])
