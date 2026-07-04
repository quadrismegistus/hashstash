"""Tests for written_at, before/after filtering, and prune()."""
import os
import time
import logging
from datetime import datetime, timedelta, timezone

import pytest

from hashstash import HashStash, TypedStash, logger as hashstash_logger
from hashstash.engines.base import _unwrap_envelope, _coerce_timestamp, ENVELOPE_MARKER


@pytest.fixture(autouse=True)
def _ensure_warnings_logged():
    # Other test modules (e.g. test_engines.py) raise the hashstash logger level to silence
    # noise, which also silences our naive-datetime warnings. Keep WARNING visible here.
    saved = hashstash_logger.level
    hashstash_logger.setLevel(logging.WARNING)
    try:
        yield
    finally:
        hashstash_logger.setLevel(saved)


ENGINES_UNDER_TEST = ["pairtree", "sqlite", "memory", "diskcache", "lmdb", "jsonl"]


@pytest.fixture(params=ENGINES_UNDER_TEST)
def stash(request, tmp_path):
    s = HashStash(engine=request.param, root_dir=str(tmp_path), dbname="t")
    s.clear()
    yield s


class TestWrittenAt:
    def test_timestamp_recorded_on_write(self, stash):
        before = time.time()
        stash["k"] = "v"
        after = time.time()
        result = stash.get_all("k", with_metadata=True, all_results=True)
        assert len(result) == 1
        ts = result[0]["_written_at"]
        assert before <= ts <= after

    def test_append_mode_latest_wins(self, stash):
        stash.append_mode = True
        stash["k"] = "v1"
        t1 = time.time()
        time.sleep(0.01)
        stash["k"] = "v2"
        time.sleep(0.01)
        stash["k"] = "v3"
        result = stash.get_all("k", with_metadata=True, all_results=True)
        assert len(result) == 3
        assert result[0]["_value"] == "v1"
        assert result[2]["_value"] == "v3"
        # Timestamps strictly increasing
        assert result[0]["_written_at"] < result[1]["_written_at"] < result[2]["_written_at"]
        # Latest is >= t1
        assert result[-1]["_written_at"] >= t1

    def test_non_append_overwrite_updates_timestamp(self, stash):
        stash["k"] = "v1"
        r1 = stash.get_all("k", with_metadata=True)[-1]["_written_at"]
        time.sleep(0.02)
        stash["k"] = "v2"
        r2 = stash.get_all("k", with_metadata=True)[-1]["_written_at"]
        assert r2 > r1


class TestBeforeAfterFilter:
    def test_items_after_filters_out_old(self, stash):
        stash["old"] = 1
        time.sleep(0.05)
        midpoint = time.time()
        time.sleep(0.05)
        stash["new"] = 2
        results = dict(stash.items(after=midpoint))
        assert "new" in results
        assert "old" not in results

    def test_items_before_filters_out_new(self, stash):
        stash["old"] = 1
        time.sleep(0.05)
        midpoint = time.time()
        time.sleep(0.05)
        stash["new"] = 2
        results = dict(stash.items(before=midpoint))
        assert "old" in results
        assert "new" not in results

    def test_items_accepts_datetime(self, stash, caplog):
        stash["k"] = "v"
        # tz-aware datetime — no warning
        with caplog.at_level(logging.WARNING):
            results = dict(stash.items(
                after=datetime.fromtimestamp(0, tz=timezone.utc)
            ))
        assert "k" in results
        assert not any("naive" in r.message for r in caplog.records)

    def test_naive_datetime_warns(self, stash, caplog):
        stash["k"] = "v"
        with caplog.at_level(logging.WARNING):
            list(stash.items(after=datetime(1970, 1, 1)))
        assert any("naive" in r.message for r in caplog.records)


class TestBackwardCompatUnstamped:
    def test_unwrap_legacy_list_format(self):
        """A bare (non-envelope) list is ONE value with t=0. Treating it as multiple
        versions silently corrupted list-valued caches (get() returned the last
        element instead of the stored list)."""
        values, timestamps = _unwrap_envelope(["v1", "v2"])
        assert values == [["v1", "v2"]]
        assert timestamps == [0.0]

    def test_unwrap_envelope_format(self):
        envelope = {
            ENVELOPE_MARKER: True,
            "_values": ["a", "b"],
            "_written_at": [100.0, 200.0],
        }
        values, timestamps = _unwrap_envelope(envelope)
        assert values == ["a", "b"]
        assert timestamps == [100.0, 200.0]

    def test_legacy_entries_excluded_from_after_queries(self, stash):
        # Simulate a pre-feature entry by writing directly at engine layer with no envelope.
        # Legacy => timestamp=0 => fails after=(any positive) filter.
        values, timestamps = _unwrap_envelope("legacy")
        kept = [(v, t) for v, t in zip(values, timestamps) if t > time.time() - 3600]
        assert kept == []  # legacy timestamp=0 is excluded from "after: last hour" query


class TestPrune:
    def test_prune_dry_run_default(self, stash):
        stash["old_key"] = "old_value"
        # Wait then put a recent one
        time.sleep(0.05)
        recent_cutoff = time.time() + 0.01  # future — everything is "older"
        stash["new_key"] = "new_value"
        # Prune everything older than "now + epsilon" — both match
        count = stash.prune(older_than=datetime.fromtimestamp(recent_cutoff + 1, tz=timezone.utc))
        assert count == 2
        # dry_run=True default → nothing actually deleted
        assert len(stash) == 2

    def test_prune_actually_deletes(self, stash):
        stash["k1"] = 1
        stash["k2"] = 2
        # capture the midpoint BETWEEN the writes, with margin on both sides:
        # deriving it from wall-clock after the k3 write was flaky on loaded CI
        # runners (a slow write pushed k3's timestamp before the midpoint)
        time.sleep(0.05)
        midpoint = datetime.fromtimestamp(time.time(), tz=timezone.utc)
        time.sleep(0.05)
        stash["k3"] = 3
        count = stash.prune(older_than=midpoint, dry_run=False)
        assert count == 2  # k1, k2 are older than midpoint
        assert len(stash) == 1
        assert "k3" in stash

    def test_prune_with_timedelta(self, stash):
        stash["old"] = 1
        # age is 0 seconds — prune older_than=1 hour should match nothing
        count = stash.prune(older_than=timedelta(hours=1), dry_run=True)
        assert count == 0

    def test_prune_requires_age_filter(self, stash):
        stash["k"] = "v"
        with pytest.raises(ValueError, match="older_than"):
            stash.prune()
        # cache untouched
        assert "k" in stash

    def test_prune_logs_summary(self, stash, caplog):
        stash["k1"] = 1
        stash["k2"] = 2
        hashstash_logger.setLevel(logging.INFO)
        with caplog.at_level(logging.INFO):
            stash.prune(older_than=timedelta(days=365), dry_run=True)
        assert any("prune" in r.message and "matched" in r.message for r in caplog.records)


class TestCoerceTimestamp:
    def test_float_passthrough(self):
        assert _coerce_timestamp(100.5) == 100.5

    def test_int_coerced_to_float(self):
        assert _coerce_timestamp(100) == 100.0

    def test_aware_datetime_converted(self):
        dt = datetime(2025, 1, 1, tzinfo=timezone.utc)
        assert _coerce_timestamp(dt) == dt.timestamp()

    def test_none_passthrough(self):
        assert _coerce_timestamp(None) is None

    def test_invalid_raises(self):
        with pytest.raises(TypeError):
            _coerce_timestamp("2025-01-01")


class TestTypedStashForwarding:
    def test_typed_items_forwards_before_after(self, tmp_path):
        stash = HashStash(engine="pairtree", root_dir=str(tmp_path), dbname="typed")
        stash.clear()
        stash["old"] = {"x": 1}
        time.sleep(0.05)
        midpoint = time.time()
        time.sleep(0.05)
        stash["new"] = {"x": 2}

        typed = TypedStash(stash, loader=lambda raw: raw["x"])
        results = dict(typed.items(after=midpoint))
        assert results == {"new": 2}

    def test_typed_filter_forwards_before_after(self, tmp_path):
        stash = HashStash(engine="pairtree", root_dir=str(tmp_path), dbname="typed2")
        stash.clear()
        stash["keep_old"] = {"x": 1}
        stash["skip_old"] = {"x": 2}
        time.sleep(0.05)
        midpoint = time.time()
        time.sleep(0.05)
        stash["keep_new"] = {"x": 3}

        typed = TypedStash(stash, loader=lambda raw: raw["x"])
        # Predicate filters by key prefix, then age filter narrows further
        results = dict(typed.filter(lambda k: k.startswith("keep_"), after=midpoint))
        assert results == {"keep_new": 3}


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
