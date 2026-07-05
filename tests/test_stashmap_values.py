"""StashMap iterates/indexes the computed VALUES (like builtin map / pmap);
the StashMapRun wrappers are available via `.runs`. Guards the 1.0 API change
away from the old "iterating yields wrapper objects" behavior."""
import tempfile

import pytest

from hashstash import HashStash


def _square(x):
    return x * x


def _mul(x, y=1):
    return x * y


@pytest.fixture
def stash():
    return HashStash(engine="memory", root_dir=tempfile.mkdtemp())


@pytest.mark.parametrize("num_proc", [1, 2])
def test_iteration_yields_values(stash, num_proc):
    m = stash.map(_square, objects=[1, 2, 3, 4], num_proc=num_proc, progress=False)
    assert list(m) == [1, 4, 9, 16]                       # not StashMapRun objects


@pytest.mark.parametrize("num_proc", [1, 2])
def test_for_loop_yields_values(stash, num_proc):
    m = stash.map(_square, objects=[1, 2, 3], num_proc=num_proc, progress=False)
    assert [x for x in m] == [1, 4, 9]


def test_indexing_returns_value(stash):
    m = stash.map(_square, objects=[1, 2, 3, 4], num_proc=1, progress=False)
    assert m[0] == 1
    assert m[-1] == 16
    with pytest.raises(IndexError):
        m[99]


def test_slicing_returns_values(stash):
    m = stash.map(_square, objects=[1, 2, 3, 4, 5], num_proc=1, progress=False)
    assert list(m[1:4]) == [4, 9, 16]


def test_runs_exposes_wrappers(stash):
    m = stash.map(_square, objects=[1, 2, 3], num_proc=1, progress=False)
    runs = m.runs
    assert [type(r).__name__ for r in runs] == ["StashMapRun"] * 3
    assert [r.result for r in runs] == [1, 4, 9]
    assert runs[1].args == (2,)


def test_results_values_items(stash):
    m = stash.map(_mul, objects=[1, 2, 3], y=10, num_proc=1, progress=False)
    assert list(m.results) == [10, 20, 30]
    assert m.values_l() == [10, 20, 30]
    assert [v for _, v in m.items_l()] == [10, 20, 30]
    assert m.items_l()[0][0] == ((1,), {"y": 10})


def test_len_is_lazy_and_correct(stash):
    m = stash.map(_square, objects=[1, 2, 3, 4], num_proc=1, progress=False)
    assert len(m) == 4


def test_results_cached_across_calls(tmp_path):
    stash = HashStash(root_dir=str(tmp_path / "cache"))
    assert list(stash.map(_square, objects=[5, 6, 7], num_proc=1, progress=False)) == [25, 36, 49]
    # second identical map returns the same values (from cache)
    assert list(stash.map(_square, objects=[5, 6, 7], num_proc=1, progress=False)) == [25, 36, 49]
