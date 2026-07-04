"""Size-bounded eviction via max_entries."""
import time

import pytest

from hashstash import HashStash


@pytest.mark.parametrize("engine", ["pairtree", "sqlite", "memory", "jsonl"])
def test_max_entries_caps_size(engine, tmp_path):
    if engine == "sqlite":
        pytest.importorskip("sqlitedict")
    stash = HashStash(engine=engine, root_dir=str(tmp_path / engine), max_entries=10)
    for i in range(30):
        stash[f"k{i}"] = i
        time.sleep(0.001)  # keep write timestamps strictly ordered
    # never exceeds the cap (may sit at/below it after amortized eviction)
    assert len(stash) <= 10


def test_evicts_oldest_first(tmp_path):
    stash = HashStash(root_dir=str(tmp_path / "c"), max_entries=5)
    for i in range(5):
        stash[f"k{i}"] = i
        time.sleep(0.005)
    # newest survivors present, oldest gone once we push over the cap
    for i in range(5, 20):
        stash[f"k{i}"] = i
        time.sleep(0.005)
    assert len(stash) <= 5
    # the most recent keys must be present
    assert f"k19" in stash
    # the very first keys must be gone
    assert "k0" not in stash


def test_max_entries_rejects_zero(tmp_path):
    with pytest.raises(ValueError):
        HashStash(root_dir=str(tmp_path / "c"), max_entries=0)


def test_max_entries_none_is_unbounded(tmp_path):
    stash = HashStash(root_dir=str(tmp_path / "c"))
    assert stash.max_entries is None
    for i in range(50):
        stash[f"k{i}"] = i
    assert len(stash) == 50


def test_max_entries_inherited_by_substash(tmp_path):
    stash = HashStash(root_dir=str(tmp_path / "c"), max_entries=100)
    sub = stash.sub(dbname="child")
    assert sub.max_entries == 100
