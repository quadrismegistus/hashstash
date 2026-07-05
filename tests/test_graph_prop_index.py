"""Edge-property equality index: edges_where narrows sources by an equality
predicate on an edge property (like the rel index), instead of scanning every
source. Correctness must match a full scan; non-equality/unhashable predicates
fall back to scanning."""
import tempfile

import pytest

from hashstash import HashStash


@pytest.fixture
def graph():
    g = HashStash(engine="memory", root_dir=tempfile.mkdtemp()).graph("g")
    with g.batch():
        for i in range(100):
            g.add_edge(
                f"n{i}", f"m{i}", rel="knows",
                color=("red" if i % 10 == 0 else "blue"), weight=i,
            )
        # a rel-less edge + one holding an unhashable prop value
        g.add_edge("special", "t", rel="likes", color="red", tags=["a", "b"])
    return g


def _scan(g, pred):
    edges = g.edges() if callable(g.edges) else g.edges
    return sorted((s, d) for s, d, r, p in edges if pred(p, r))


def test_equality_predicate_matches_full_scan(graph):
    got = sorted((s, d) for s, d, r, p in graph.edges_where(color="red"))
    assert got == _scan(graph, lambda p, r: p.get("color") == "red")
    assert len(got) == 11  # 10 "knows" + 1 "likes"


def test_rel_and_prop_intersect(graph):
    got = graph.edges_where(rel="knows", color="red")
    assert len(got) == 10
    assert all(r == "knows" and p["color"] == "red" for _, _, r, p in got)


def test_range_predicate_still_scans_within_narrowed_set(graph):
    # weight__gt is not an equality predicate -> applied per-edge, not indexed
    got = graph.edges_where(weight__gt=95)
    assert sorted(s for s, d, r, p in got) == ["n96", "n97", "n98", "n99"]


def test_unhashable_query_value_falls_back(graph):
    # an unhashable query value can't use the index; must still match via scan
    got = graph.edges_where(tags=["a", "b"])
    assert [(s, d) for s, d, r, p in got] == [("special", "t")]


def test_absent_property_returns_empty(graph):
    assert graph.edges_where(nonexistent="x") == []


def test_index_maintained_on_add(graph):
    graph.add_edge("nX", "mX", rel="knows", color="red", weight=0)
    assert len(graph.edges_where(color="red")) == 12


def test_index_invalidated_on_remove(graph):
    graph.remove_node("special")
    assert len(graph.edges_where(color="red")) == 10  # the "likes" edge is gone
    # still matches a fresh full scan
    got = sorted((s, d) for s, d, r, p in graph.edges_where(color="red"))
    assert got == _scan(graph, lambda p, r: p.get("color") == "red")
