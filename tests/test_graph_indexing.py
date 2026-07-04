"""GraphStash rel-index: edges_where(rel=...) narrows the scan via a secondary
index, and stays correct across adds, bulk loads, batches, and removes."""
import pytest

from hashstash import HashStash


@pytest.fixture
def graph(tmp_path):
    return HashStash(root_dir=str(tmp_path / "g")).graph()


def _brute_edges_where(g, **kwargs):
    """Reference implementation: full scan, no index — for parity checks."""
    from hashstash.graph import _match

    out = []
    for src in g._out_keys():
        for dst, rel, props in g._get_out(src):
            sp = g._get_node_props(src) or {}
            dp = g._get_node_props(dst) or {}
            if _match(sp, dp, rel, props, kwargs):
                out.append((src, dst, rel, dict(props)))
    return out


def _norm(edges):
    return sorted((s, d, r, tuple(sorted(p.items()))) for s, d, r, p in edges)


def test_index_matches_bruteforce_on_rel(graph):
    graph.add_edge("a", "b", rel="knows", w=1)
    graph.add_edge("a", "c", rel="knows", w=5)
    graph.add_edge("a", "d", rel="likes", w=2)
    graph.add_edge("b", "c", rel="knows", w=9)
    for rel in ("knows", "likes", "nope"):
        assert _norm(graph.edges_where(rel=rel)) == _norm(_brute_edges_where(graph, rel=rel))


def test_index_actually_narrows_sources(graph):
    graph.add_edge("hub", "x", rel="likes")
    for i in range(20):
        graph.add_edge(f"n{i}", f"m{i}", rel="knows")
    # only 'hub' has a 'likes' edge; the index must return just that source
    assert graph._rel_sources("likes") == {"hub"}
    assert len(graph.edges_where(rel="likes")) == 1


def test_rel_none_indexed(graph):
    graph.add_edge("a", "b")            # rel None
    graph.add_edge("a", "c", rel="k")
    hits = graph.edges_where(rel=None)
    assert [(s, d) for s, d, r, p in hits] == [("a", "b")]


def test_index_maintained_on_bulk(graph):
    graph.add_edges_bulk([("a", "b", "r1", {}), ("c", "d", "r2", {}), ("a", "e", "r1", {})])
    assert graph._rel_sources("r1") == {"a"}
    assert graph._rel_sources("r2") == {"c"}
    assert len(graph.edges_where(rel="r1")) == 2


def test_index_maintained_in_batch(graph):
    with graph.batch():
        for i in range(10):
            graph.add_edge("s", f"t{i}", rel="e")
    assert graph._rel_sources("e") == {"s"}
    assert len(graph.edges_where(rel="e")) == 10


def test_index_rebuilds_after_remove_edge(graph):
    graph.add_edge("a", "b", rel="knows")
    graph.add_edge("a", "c", rel="likes")
    assert len(graph.edges_where(rel="knows")) == 1  # builds index
    graph.remove_edge("a", "b", rel="knows")
    # index invalidated on remove; next query rebuilds and reflects the removal
    assert graph.edges_where(rel="knows") == []
    assert len(graph.edges_where(rel="likes")) == 1


def test_index_rebuilds_after_remove_node(graph):
    graph.add_edge("a", "b", rel="knows")
    graph.add_edge("c", "b", rel="knows")
    assert len(graph.edges_where(rel="knows")) == 2
    graph.remove_node("a")
    assert _norm(graph.edges_where(rel="knows")) == _norm(_brute_edges_where(graph, rel="knows"))
    assert len(graph.edges_where(rel="knows")) == 1


def test_index_reflects_lazy_build_across_instances(tmp_path):
    root = str(tmp_path / "g")
    g1 = HashStash(root_dir=root).graph()
    g1.add_edge("a", "b", rel="knows")
    g1.add_edge("a", "c", rel="likes")
    # fresh instance builds its index from disk on first query
    g2 = HashStash(root_dir=root).graph()
    assert len(g2.edges_where(rel="knows")) == 1
    assert g2._rel_sources("likes") == {"a"}


def test_rels_and_edges_with_rel(graph):
    graph.add_edge("a", "b", rel="knows")
    graph.add_edge("a", "c")  # None
    graph.add_edge("b", "c", rel="likes")
    assert graph.rels() == [None, "knows", "likes"]
    assert len(graph.edges_with_rel("knows")) == 1
    assert len(graph.edges_with_rel(None)) == 1


def test_rel_index_with_extra_predicates(graph):
    graph.add_edge("a", "b", rel="knows", weight=5)
    graph.add_edge("a", "c", rel="knows", weight=1)
    graph.add_edge("a", "d", rel="likes", weight=9)
    hits = graph.edges_where(rel="knows", weight__gte=3)
    assert [(s, d) for s, d, r, p in hits] == [("a", "b")]


def test_clear_resets_index(graph):
    graph.add_edge("a", "b", rel="knows")
    assert len(graph.edges_where(rel="knows")) == 1
    graph.clear()
    assert graph.edges_where(rel="knows") == []
    assert graph.rels() == []
