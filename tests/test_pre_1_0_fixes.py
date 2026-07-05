"""Regression guards for the final pre-1.0 fixes."""
import tempfile
import time
from datetime import timedelta

import pytest

from hashstash import HashStash


def test_prune_does_not_keyerror_on_ttl_expired_key():
    # prune iterates the raw key view and matches expired entries; deleting them
    # went through has() (TTL-aware), which raised KeyError. prune now deletes
    # the physical entry directly.
    s = HashStash(engine="pairtree", root_dir=tempfile.mkdtemp(), ttl=1)
    s.clear()
    s["k"] = "v"
    time.sleep(1.2)  # entry is now TTL-expired
    pruned = s.prune(older_than=timedelta(seconds=0), dry_run=False)
    assert pruned == 1
    assert len(s) == 0


def _make_closure_in_exec():
    # a closure whose source is unavailable (exec/-c/REPL context)
    ns = {}
    exec("def mk(n):\n    def inner(x):\n        return x + n\n    return inner", ns)
    return ns["mk"](10)


def test_source_unavailable_closure_fails_loud_not_cryptic():
    s = HashStash(engine="memory", root_dir=tempfile.mkdtemp())
    s.clear()
    s["f"] = _make_closure_in_exec()
    # reading it back must raise a clear ValueError with guidance, not a cryptic
    # NameError from a wrapper that references an undefined inner name
    with pytest.raises(ValueError, match="cannot reconstruct closure"):
        _ = s["f"]


def test_fraction_serializes_version_independently():
    # Fraction.__reduce__ differs between Python 3.9 ('22/7') and 3.11+ ((22,7)),
    # which gave the same Fraction a different cache key across versions. Store
    # numerator/denominator so the form (and hash) is version-stable.
    from fractions import Fraction
    from hashstash import serialize, deserialize, encode_hash, safe_deserialization

    for fr in [Fraction(22, 7), Fraction(-3, 4), Fraction(0), Fraction(5)]:
        s = serialize(fr, sort_keys=True)
        assert '"numerator"' in s and '"denominator"' in s  # not the reducer form
        assert "__args__" not in s
        assert deserialize(serialize(fr)) == fr
        with safe_deserialization(True):
            assert deserialize(serialize(fr)) == fr  # data-only, safe-mode friendly


def test_stats_always_has_all_four_keys():
    s = HashStash(engine="memory", root_dir=tempfile.mkdtemp())
    assert set(s.stats) == {"hits", "misses", "sets", "deletes"}
    assert all(v == 0 for v in s.stats.values())


def test_repr_shows_nonzero_stats():
    s = HashStash(engine="memory", root_dir=tempfile.mkdtemp())
    assert "[" not in repr(s)  # no activity -> no stats suffix
    s["a"] = 1
    _ = s["a"]
    r = repr(s)
    assert "sets=1" in r and "hits=1" in r
    assert type(s).__name__ in r  # still identifies the class + path
