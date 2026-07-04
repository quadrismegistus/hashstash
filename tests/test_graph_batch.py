"""GraphStash.batch(): buffered edge writes."""
import pytest

from hashstash import HashStash


@pytest.fixture
def graph(tmp_path):
    return HashStash(root_dir=str(tmp_path / "g")).graph()


def test_batch_persists_all_edges(graph):
    with graph.batch():
        for i in range(50):
            graph.add_edge("hub", f"leaf{i}", rel="has")
    assert graph.num_edges == 50
    # fresh instance reads from disk: writes were actually persisted
    g2 = graph._stash.graph()
    assert g2.num_edges == 50
    assert len(g2.neighbors("hub")) == 50


def test_batch_writes_each_node_once(graph, monkeypatch):
    """Inside a batch, the hub's adjacency list is written once on flush, not
    once per edge."""
    writes = []
    out_stash = graph._out_stash
    real_setitem = type(out_stash).__setitem__

    def counting_setitem(self, key, value):
        # the three sub-stashes share a class; count only the out-stash instance
        if self is out_stash:
            writes.append(key)
        return real_setitem(self, key, value)

    monkeypatch.setattr(type(out_stash), "__setitem__", counting_setitem)

    with graph.batch():
        for i in range(20):
            graph.add_edge("hub", f"leaf{i}")

    hub_writes = writes.count("hub")
    assert hub_writes == 1, f"hub written {hub_writes} times, expected 1"


def test_batch_flushes_on_exception(graph):
    with pytest.raises(RuntimeError):
        with graph.batch():
            graph.add_edge("a", "b")
            graph.add_edge("a", "c")
            raise RuntimeError("boom")
    # edges added before the exception are still persisted
    g2 = graph._stash.graph()
    assert g2.num_edges == 2


def test_batch_queries_work_after(graph):
    with graph.batch():
        graph.add_edge("a", "b", weight=5)
        graph.add_edge("a", "c", weight=1)
    hits = graph.edges_where(weight__gte=3)
    assert [(s, d) for s, d, r, p in hits] == [("a", "b")]


def test_batch_not_reentrant(graph):
    with graph.batch():
        with pytest.raises(RuntimeError):
            with graph.batch():
                pass


def test_batch_equivalent_to_incremental(tmp_path):
    edges = [("u", f"v{i}", i) for i in range(30)]

    g_inc = HashStash(root_dir=str(tmp_path / "inc")).graph()
    for u, v, w in edges:
        g_inc.add_edge(u, v, weight=w)

    g_bat = HashStash(root_dir=str(tmp_path / "bat")).graph()
    with g_bat.batch():
        for u, v, w in edges:
            g_bat.add_edge(u, v, weight=w)

    assert g_inc.num_edges == g_bat.num_edges == 30
    assert sorted(g_inc.neighbors("u")) == sorted(g_bat.neighbors("u"))
    assert len(g_bat.edges_where(weight__gte=15)) == len(g_inc.edges_where(weight__gte=15))
