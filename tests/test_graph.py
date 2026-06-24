import os
import pytest
from hashstash import HashStash, GraphStash


@pytest.fixture
def stash(tmp_path):
    return HashStash(engine="pairtree", root_dir=str(tmp_path))


@pytest.fixture
def g(stash):
    return stash.graph("test")


class TestNodes:
    def test_add_and_get(self, g):
        g.add_node("alice", name="Alice", age=30)
        assert g.node("alice") == {"name": "Alice", "age": 30}

    def test_has_node(self, g):
        assert not g.has_node("alice")
        g.add_node("alice")
        assert g.has_node("alice")

    def test_contains(self, g):
        g.add_node("alice")
        assert "alice" in g
        assert "bob" not in g

    def test_nodes_list(self, g):
        g.add_node("alice")
        g.add_node("bob")
        assert sorted(g.nodes) == ["alice", "bob"]

    def test_empty_props(self, g):
        g.add_node("alice")
        assert g.node("alice") == {}

    def test_update_props(self, g):
        g.add_node("alice", name="Alice")
        g.add_node("alice", age=30)
        assert g.node("alice") == {"name": "Alice", "age": 30}

    def test_get_missing_raises(self, g):
        with pytest.raises(KeyError):
            g.node("ghost")

    def test_remove_node(self, g):
        g.add_node("alice")
        g.remove_node("alice")
        assert not g.has_node("alice")

    def test_remove_missing_raises(self, g):
        with pytest.raises(KeyError):
            g.remove_node("ghost")

    def test_len(self, g):
        assert len(g) == 0
        g.add_node("alice")
        g.add_node("bob")
        assert len(g) == 2


class TestEdges:
    def test_add_and_get(self, g):
        g.add_edge("alice", "bob", rel="knows", since=2020)
        assert g.edge("alice", "bob", rel="knows") == {"since": 2020}

    def test_has_edge(self, g):
        assert not g.has_edge("alice", "bob")
        g.add_edge("alice", "bob")
        assert g.has_edge("alice", "bob")

    def test_auto_creates_nodes(self, g):
        g.add_edge("alice", "bob")
        assert g.has_node("alice")
        assert g.has_node("bob")

    def test_default_rel_none(self, g):
        g.add_edge("alice", "bob", weight=5)
        assert g.has_edge("alice", "bob", rel=None)
        assert g.edge("alice", "bob") == {"weight": 5}

    def test_multigraph_same_rel(self, g):
        g.add_edge("alice", "bob", rel="knows", since=2020)
        g.add_edge("alice", "bob", rel="knows", since=2025)
        assert g.num_edges == 2
        edges = g.edges_where(rel="knows")
        years = sorted(e[3]["since"] for e in edges)
        assert years == [2020, 2025]

    def test_multiple_rels(self, g):
        g.add_edge("alice", "bob", rel="knows")
        g.add_edge("alice", "bob", rel="works_with")
        assert g.has_edge("alice", "bob", rel="knows")
        assert g.has_edge("alice", "bob", rel="works_with")
        assert g.num_edges == 2

    def test_remove_edge(self, g):
        g.add_edge("alice", "bob", rel="knows")
        g.remove_edge("alice", "bob", rel="knows")
        assert not g.has_edge("alice", "bob", rel="knows")

    def test_remove_edge_targeted(self, g):
        g.add_edge("a", "b", rel="sft", prompt="anger", r=2.3)
        g.add_edge("a", "b", rel="sft", prompt="fear", r=0.5)
        g.remove_edge("a", "b", rel="sft", prompt="anger")
        assert g.num_edges == 1
        remaining = g.edges_where(rel="sft")
        assert remaining[0][3]["prompt"] == "fear"

    def test_remove_edge_all_matching_rel(self, g):
        g.add_edge("a", "b", rel="sft", prompt="anger")
        g.add_edge("a", "b", rel="sft", prompt="fear")
        g.remove_edge("a", "b", rel="sft")
        assert g.num_edges == 0

    def test_remove_missing_raises(self, g):
        with pytest.raises(KeyError):
            g.remove_edge("alice", "bob", rel="knows")

    def test_edge_missing_raises(self, g):
        with pytest.raises(KeyError):
            g.edge("alice", "bob")

    def test_edges_of_out(self, g):
        g.add_edge("alice", "bob", rel="knows")
        g.add_edge("alice", "carol", rel="likes")
        edges = g.edges_of("alice")
        assert len(edges) == 2

    def test_edges_of_in(self, g):
        g.add_edge("alice", "bob")
        g.add_edge("carol", "bob")
        edges = g.edges_of("bob", direction="in")
        assert len(edges) == 2

    def test_edges_property(self, g):
        g.add_edge("alice", "bob", rel="knows")
        g.add_edge("bob", "carol", rel="likes")
        edges = g.edges
        assert len(edges) == 2
        srcs = {e[0] for e in edges}
        assert srcs == {"alice", "bob"}

    def test_num_edges(self, g):
        assert g.num_edges == 0
        g.add_edge("alice", "bob")
        g.add_edge("bob", "carol")
        assert g.num_edges == 2

    def test_self_loop(self, g):
        g.add_edge("alice", "alice", rel="self")
        assert g.has_edge("alice", "alice", rel="self")
        assert g.num_edges == 1


class TestRemoveNodeCascade:
    def test_removes_outgoing_from_neighbors(self, g):
        g.add_edge("alice", "bob")
        g.add_edge("alice", "carol")
        g.remove_node("alice")
        assert g.edges_of("bob", direction="in") == []
        assert g.edges_of("carol", direction="in") == []

    def test_removes_incoming_from_neighbors(self, g):
        g.add_edge("bob", "alice")
        g.add_edge("carol", "alice")
        g.remove_node("alice")
        assert g.edges_of("bob", direction="out") == []
        assert g.edges_of("carol", direction="out") == []

    def test_self_loop_cleanup(self, g):
        g.add_edge("alice", "alice", rel="self")
        g.add_edge("alice", "bob")
        g.remove_node("alice")
        assert not g.has_node("alice")
        assert g.edges_of("bob", direction="in") == []

    def test_middle_node_in_chain(self, g):
        g.add_edge("a", "b")
        g.add_edge("b", "c")
        g.remove_node("b")
        assert g.has_node("a")
        assert g.has_node("c")
        assert g.edges_of("a", direction="out") == []
        assert g.edges_of("c", direction="in") == []


class TestNeighbors:
    def test_out(self, g):
        g.add_edge("alice", "bob")
        g.add_edge("alice", "carol")
        assert sorted(g.neighbors("alice")) == ["bob", "carol"]

    def test_in(self, g):
        g.add_edge("bob", "alice")
        g.add_edge("carol", "alice")
        assert sorted(g.neighbors("alice", direction="in")) == ["bob", "carol"]

    def test_both(self, g):
        g.add_edge("alice", "bob")
        g.add_edge("carol", "alice")
        assert sorted(g.neighbors("alice", direction="both")) == ["bob", "carol"]

    def test_filter_by_rel(self, g):
        g.add_edge("alice", "bob", rel="knows")
        g.add_edge("alice", "carol", rel="likes")
        assert g.neighbors("alice", rel="knows") == ["bob"]

    def test_isolated_node(self, g):
        g.add_node("lonely")
        assert g.neighbors("lonely") == []

    def test_no_duplicates(self, g):
        g.add_edge("alice", "bob", rel="knows")
        g.add_edge("alice", "bob", rel="likes")
        nbrs = g.neighbors("alice")
        assert nbrs == ["bob"]


class TestTraverse:
    def _build_chain(self, g):
        g.add_edge("a", "b")
        g.add_edge("b", "c")
        g.add_edge("c", "d")

    def test_depth_1(self, g):
        self._build_chain(g)
        levels = g.traverse("a", depth=1)
        assert levels[0] == ["a"]
        assert levels[1] == ["b"]
        assert 2 not in levels

    def test_depth_2(self, g):
        self._build_chain(g)
        levels = g.traverse("a", depth=2)
        assert levels[0] == ["a"]
        assert levels[1] == ["b"]
        assert levels[2] == ["c"]

    def test_depth_0(self, g):
        g.add_node("a")
        levels = g.traverse("a", depth=0)
        assert levels == {0: ["a"]}

    def test_missing_node_raises(self, g):
        with pytest.raises(KeyError):
            g.traverse("ghost")

    def test_cycle(self, g):
        g.add_edge("a", "b")
        g.add_edge("b", "a")
        levels = g.traverse("a", depth=10)
        assert levels[0] == ["a"]
        assert levels[1] == ["b"]
        assert 2 not in levels

    def test_branching(self, g):
        g.add_edge("a", "b")
        g.add_edge("a", "c")
        g.add_edge("b", "d")
        g.add_edge("c", "d")
        levels = g.traverse("a", depth=2)
        assert levels[0] == ["a"]
        assert sorted(levels[1]) == ["b", "c"]
        assert levels[2] == ["d"]

    def test_rel_filter(self, g):
        g.add_edge("a", "b", rel="knows")
        g.add_edge("a", "c", rel="likes")
        g.add_edge("b", "d", rel="knows")
        levels = g.traverse("a", depth=2, rel="knows")
        assert levels[0] == ["a"]
        assert levels[1] == ["b"]
        assert levels[2] == ["d"]
        all_discovered = [n for ns in levels.values() for n in ns]
        assert "c" not in all_discovered


class TestShortestPath:
    def test_direct(self, g):
        g.add_edge("a", "b")
        assert g.shortest_path("a", "b") == ["a", "b"]

    def test_multi_hop(self, g):
        g.add_edge("a", "b")
        g.add_edge("b", "c")
        g.add_edge("c", "d")
        assert g.shortest_path("a", "d") == ["a", "b", "c", "d"]

    def test_shortest_among_multiple(self, g):
        g.add_edge("a", "b")
        g.add_edge("b", "d")
        g.add_edge("a", "c")
        g.add_edge("c", "x")
        g.add_edge("x", "d")
        path = g.shortest_path("a", "d")
        assert path == ["a", "b", "d"]

    def test_no_path(self, g):
        g.add_node("a")
        g.add_node("b")
        assert g.shortest_path("a", "b") is None

    def test_same_node(self, g):
        g.add_node("a")
        assert g.shortest_path("a", "a") == ["a"]

    def test_missing_src_raises(self, g):
        g.add_node("b")
        with pytest.raises(KeyError):
            g.shortest_path("ghost", "b")

    def test_missing_dst_raises(self, g):
        g.add_node("a")
        with pytest.raises(KeyError):
            g.shortest_path("a", "ghost")


class TestEdgesWhere:
    def test_filter_by_rel(self, g):
        g.add_edge("a", "b", rel="knows")
        g.add_edge("a", "c", rel="likes")
        result = g.edges_where(rel="knows")
        assert len(result) == 1
        assert result[0][:3] == ("a", "b", "knows")

    def test_filter_by_prop_gt(self, g):
        g.add_edge("a", "b", rel="sft", resistance=2.5)
        g.add_edge("a", "c", rel="sft", resistance=0.5)
        result = g.edges_where(resistance__gt=1.0)
        assert len(result) == 1
        assert result[0][1] == "b"

    def test_filter_by_prop_lt(self, g):
        g.add_edge("a", "b", resistance=2.5)
        g.add_edge("a", "c", resistance=0.5)
        result = g.edges_where(resistance__lt=1.0)
        assert len(result) == 1
        assert result[0][1] == "c"

    def test_combined_rel_and_prop(self, g):
        g.add_edge("a", "b", rel="sft", resistance=2.5)
        g.add_edge("a", "c", rel="dpo", resistance=3.0)
        g.add_edge("a", "d", rel="sft", resistance=0.1)
        result = g.edges_where(rel="sft", resistance__gt=1.0)
        assert len(result) == 1
        assert result[0][1] == "b"

    def test_source_node_prop(self, g):
        g.add_node("a", org="allenai")
        g.add_node("b", org="meta")
        g.add_edge("a", "c", rel="sft", resistance=2.0)
        g.add_edge("b", "c", rel="sft", resistance=3.0)
        result = g.edges_where(source__org="allenai")
        assert len(result) == 1
        assert result[0][0] == "a"

    def test_target_node_prop(self, g):
        g.add_node("c", stage="sft")
        g.add_node("d", stage="dpo")
        g.add_edge("a", "c", rel="trains")
        g.add_edge("a", "d", rel="trains")
        result = g.edges_where(target__stage="sft")
        assert len(result) == 1
        assert result[0][1] == "c"

    def test_rel_startswith(self, g):
        g.add_edge("a", "b", rel="sft_of|anger")
        g.add_edge("a", "b", rel="sft_of|fear")
        g.add_edge("a", "c", rel="dpo_of|anger")
        result = g.edges_where(rel__startswith="sft_of")
        assert len(result) == 2

    def test_empty_result(self, g):
        g.add_edge("a", "b", resistance=0.5)
        assert g.edges_where(resistance__gt=10.0) == []

    def test_no_predicates_returns_all(self, g):
        g.add_edge("a", "b")
        g.add_edge("b", "c")
        assert len(g.edges_where()) == 2


class TestBulkEdges:
    def test_basic_bulk(self, g):
        edges = [
            ("a", "b", "knows", {"since": 2020}),
            ("b", "c", "knows", {"since": 2021}),
            ("a", "c", "likes", {}),
        ]
        g.add_edges_bulk(edges)
        assert g.num_edges == 3
        assert g.has_edge("a", "b", rel="knows")
        assert g.has_edge("b", "c", rel="knows")
        assert g.has_edge("a", "c", rel="likes")

    def test_bulk_auto_creates_nodes(self, g):
        g.add_edges_bulk([("x", "y", None, {})])
        assert g.has_node("x")
        assert g.has_node("y")

    def test_bulk_multigraph(self, g):
        edges = [
            ("a", "b", "knows", {"v": 1}),
            ("a", "b", "knows", {"v": 2}),
        ]
        g.add_edges_bulk(edges)
        assert g.num_edges == 2

    def test_bulk_per_prompt(self, g):
        prompts = ["anger", "fear", "joy"]
        resistances = [2.3, 0.5, 1.8]
        edges = [
            ("olmo", "olmo-sft", "sft_of", {"prompt": p, "resistance": r})
            for p, r in zip(prompts, resistances)
        ]
        g.add_edges_bulk(edges)
        assert g.num_edges == 3
        high_r = g.edges_where(rel="sft_of", resistance__gt=1.0)
        assert len(high_r) == 2
        all_sft = g.edges_where(rel="sft_of")
        assert len(all_sft) == 3

    def test_bulk_with_existing_edges(self, g):
        g.add_edge("a", "b", rel="old", x=1)
        g.add_edges_bulk([("a", "c", "new", {"x": 2})])
        assert g.num_edges == 2
        assert g.has_edge("a", "b", rel="old")

    def test_bulk_adjacency_consistency(self, g):
        g.add_edges_bulk([
            ("a", "b", "x", {}),
            ("c", "b", "y", {}),
        ])
        assert sorted(g.neighbors("b", direction="in")) == ["a", "c"]


class TestUtility:
    def test_clear(self, g):
        g.add_edge("a", "b")
        g.add_edge("b", "c")
        g.clear()
        assert len(g) == 0
        assert g.num_edges == 0
        assert g.nodes == []

    def test_repr(self, g):
        g.add_edge("a", "b")
        r = repr(g)
        assert "test" in r
        assert "nodes=2" in r
        assert "edges=1" in r

    def test_direct_construction(self, stash):
        g = GraphStash(stash, "direct")
        g.add_edge("a", "b")
        assert g.has_edge("a", "b")


@pytest.mark.parametrize("engine", ["pairtree", "sqlite"])
class TestMultiEngine:
    def test_round_trip(self, tmp_path, engine):
        s = HashStash(engine=engine, root_dir=str(tmp_path / engine))
        g = s.graph("multi")
        g.add_node("alice", name="Alice")
        g.add_edge("alice", "bob", rel="knows", since=2020)
        assert g.node("alice") == {"name": "Alice"}
        assert g.edge("alice", "bob", rel="knows") == {"since": 2020}
        assert g.neighbors("alice") == ["bob"]
        assert g.neighbors("bob", direction="in") == ["alice"]
