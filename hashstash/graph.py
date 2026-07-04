from collections import defaultdict, deque

_UNSET = object()

_OPS = {
    "eq": lambda a, b: a == b,
    "ne": lambda a, b: a != b,
    "gt": lambda a, b: a > b,
    "lt": lambda a, b: a < b,
    "gte": lambda a, b: a >= b,
    "lte": lambda a, b: a <= b,
    "contains": lambda a, b: b in a,
    "in": lambda a, b: a in b,
    "startswith": lambda a, b: isinstance(a, str) and a.startswith(b),
    "endswith": lambda a, b: isinstance(a, str) and a.endswith(b),
}


def _safe_op(op, a, b):
    # one edge storing weight="heavy" must not TypeError every weight__gt query
    try:
        return _OPS[op](a, b)
    except TypeError:
        return False


def _parse_predicate(key):
    parts = key.split("__")
    if parts[0] == "source":
        scope = "source"
        parts = parts[1:]
    elif parts[0] == "target":
        scope = "target"
        parts = parts[1:]
    else:
        scope = "edge"
    if len(parts) >= 2 and parts[-1] in _OPS:
        field = "__".join(parts[:-1])
        op = parts[-1]
    else:
        field = "__".join(parts)
        op = "eq"
    return scope, field, op


def _match(src_props, dst_props, edge_rel, edge_props, predicates):
    for key, value in predicates.items():
        scope, field, op = _parse_predicate(key)
        if scope == "source":
            obj = src_props
        elif scope == "target":
            obj = dst_props
        elif field == "rel":
            if not _safe_op(op, edge_rel, value):
                return False
            continue
        else:
            obj = edge_props
        actual = obj.get(field, _UNSET) if isinstance(obj, dict) else _UNSET
        if actual is _UNSET:
            # absent property: fails every predicate except __ne (absent != value)
            if op == "ne":
                continue
            return False
        if not _safe_op(op, actual, value):
            return False
    return True


class GraphStash:
    """Directed property graph backed by HashStash sub-stashes.

    Storage: three sub-stashes (all append_mode=False):
        _nodes: node_id -> {**props}
        _out:   node_id -> [(dst, rel, {edge_props}), ...]
        _in:    node_id -> [(src, rel, {edge_props}), ...]

    Multigraph: multiple edges between the same (src, dst, rel) are
    allowed, distinguished by their properties. edge() returns the first
    match; use edges_between() to see all parallel edges.

    Read caching: adjacency lists and node props are cached in memory
    after first read; writes update the cache in place. Call preload()
    after bulk loading to warm the cache for fast queries.

    Concurrency: a GraphStash instance assumes it is the only writer.
    Adjacency updates are read-modify-write over whole lists, and each
    instance caches reads — two concurrent writers (or a writer plus a
    long-lived second instance) can lose edges or serve stale reads.
    Create one writer instance, and re-create reader instances (or call
    a fresh stash.graph()) after another process has written.

    Performance: add_edge() rewrites the source and target adjacency
    lists on every call — O(degree) I/O per insert, quadratic in the
    final degree when building a hub incrementally. Use add_edges_bulk()
    or ``with g.batch():`` for bulk loads; they group writes per node.
    edges_where(rel=...) is served from a secondary rel index (built
    lazily, maintained on add, rebuilt after removes), so a rel-filtered
    query visits only sources with that rel instead of the whole graph;
    queries without an exact rel filter still do a full scan.
    """

    def __init__(self, stash, name="graph"):
        self._stash = stash
        self._name = name
        prefix = f"{stash.dbname}/{name}" if stash.dbname else name
        self._nodes_stash = stash.sub(root_dir=stash.root_dir, dbname=f"{prefix}/_nodes", append_mode=False)
        self._out_stash = stash.sub(root_dir=stash.root_dir, dbname=f"{prefix}/_out", append_mode=False)
        self._in_stash = stash.sub(root_dir=stash.root_dir, dbname=f"{prefix}/_in", append_mode=False)
        self._cache_nodes = {}
        self._cache_out = {}
        self._cache_in = {}
        self._cache_node_keys = None
        self._cache_out_keys = None
        # when inside a batch(), edge writes accumulate here and each touched
        # node's adjacency list is persisted ONCE on flush instead of per edge
        self._batching = False
        self._dirty_out = set()
        self._dirty_in = set()
        # secondary index: rel -> set of source node_ids that have >=1 out-edge
        # with that rel. Lets edges_where(rel=...) visit only relevant sources
        # instead of scanning every node. Built lazily, maintained incrementally
        # on add; invalidated (rebuilt on next query) on remove.
        self._rel_index = None

    def _invalidate(self, node_id=None):
        # any structural change drops the rel index; adds re-maintain it in place
        self._rel_index = None
        if node_id is None:
            self._cache_nodes.clear()
            self._cache_out.clear()
            self._cache_in.clear()
            self._cache_node_keys = None
            self._cache_out_keys = None
        else:
            self._cache_nodes.pop(node_id, None)
            self._cache_out.pop(node_id, None)
            self._cache_in.pop(node_id, None)
            self._cache_node_keys = None
            self._cache_out_keys = None

    def _ensure_rel_index(self):
        if self._rel_index is not None:
            return
        idx = defaultdict(set)
        for src in self._out_keys():
            for _dst, rel, _props in self._get_out(src):
                idx[rel].add(src)
        self._rel_index = idx

    def _rel_sources(self, rel):
        """Source nodes with at least one out-edge of exactly this rel."""
        self._ensure_rel_index()
        return self._rel_index.get(rel, set())

    def _index_add(self, src, rel):
        # keep the index current on adds without a full rebuild (no-op if the
        # index hasn't been built yet — it'll pick everything up when built)
        if self._rel_index is not None:
            self._rel_index[rel].add(src)

    def rels(self):
        """Sorted list of distinct relationship types present in the graph."""
        self._ensure_rel_index()
        return sorted(self._rel_index.keys(), key=lambda r: (r is not None, r))

    def _get_node_props(self, node_id):
        if node_id not in self._cache_nodes:
            self._cache_nodes[node_id] = self._nodes_stash.get(node_id, default=None)
        return self._cache_nodes[node_id]

    def _get_out(self, node_id):
        if node_id not in self._cache_out:
            self._cache_out[node_id] = self._out_stash.get(node_id, default=[])
        return self._cache_out[node_id]

    def _get_in(self, node_id):
        if node_id not in self._cache_in:
            self._cache_in[node_id] = self._in_stash.get(node_id, default=[])
        return self._cache_in[node_id]

    def _node_keys(self):
        if self._cache_node_keys is None:
            self._cache_node_keys = list(self._nodes_stash.keys())
        return self._cache_node_keys

    def _out_keys(self):
        if self._cache_out_keys is None:
            self._cache_out_keys = list(self._out_stash.keys())
        return self._cache_out_keys

    def preload(self):
        """Warm the in-memory cache by reading all data from disk.
        Call after bulk loading for fastest query performance."""
        for nid in self._node_keys():
            self._get_node_props(nid)
        for nid in self._out_keys():
            self._get_out(nid)
        # sink nodes (in-edges only) never appear in the out stash: warm their
        # in-cache from the in stash's own keys
        for nid in self._in_stash.keys():
            self._get_in(nid)

    # -- Nodes --

    def add_node(self, node_id, **props):
        existing = self._get_node_props(node_id)
        if existing is not None:
            merged = {**existing, **props}
        else:
            merged = dict(props)
        self._nodes_stash[node_id] = merged
        # update the cache in place: nuking key caches on every write made any
        # interleaved write/query workload re-list all keys per query
        self._cache_nodes[node_id] = merged
        if existing is None and self._cache_node_keys is not None:
            self._cache_node_keys.append(node_id)

    def node(self, node_id):
        props = self._get_node_props(node_id)
        if props is None:
            raise KeyError(node_id)
        # a copy: handing out the cached dict let callers silently diverge the
        # cache from disk by mutating it
        return dict(props)

    def has_node(self, node_id):
        return self._get_node_props(node_id) is not None

    def remove_node(self, node_id):
        if not self.has_node(node_id):
            raise KeyError(node_id)

        for dst, rel, _ in self._get_out(node_id):
            in_list = self._get_in(dst)
            in_list = [e for e in in_list if not (e[0] == node_id and e[1] == rel)]
            if in_list:
                self._in_stash[dst] = in_list
            elif self._in_stash.has(dst):
                del self._in_stash[dst]
            self._cache_in.pop(dst, None)

        for src, rel, _ in self._get_in(node_id):
            out_list = self._get_out(src)
            out_list = [e for e in out_list if not (e[0] == node_id and e[1] == rel)]
            if out_list:
                self._out_stash[src] = out_list
            elif self._out_stash.has(src):
                del self._out_stash[src]
            self._cache_out.pop(src, None)

        if self._out_stash.has(node_id):
            del self._out_stash[node_id]
        if self._in_stash.has(node_id):
            del self._in_stash[node_id]
        del self._nodes_stash[node_id]
        self._invalidate(node_id)

    @property
    def nodes(self):
        return self._node_keys()

    # -- Edges --

    def batch(self):
        """Context manager that buffers edge writes and flushes each touched
        node's adjacency list once on exit.

        Turns an incremental ``add_edge`` loop from O(degree) I/O per edge into
        O(1) writes per node — the same win as add_edges_bulk, but keeping the
        natural per-edge call style:

            with g.batch():
                for u, v in edges:
                    g.add_edge(u, v, rel="knows")
        """
        return _GraphBatch(self)

    def add_edge(self, src, dst, rel=None, **edge_props):
        """Add one edge. Outside a batch this rewrites both nodes' adjacency
        lists — O(degree) I/O per call; wrap a bulk load in ``with g.batch():``
        (or use add_edges_bulk) to persist each node once."""
        if not self.has_node(src):
            self.add_node(src)
        if not self.has_node(dst):
            self.add_node(dst)

        out_list = list(self._get_out(src))
        had_out = bool(out_list)
        out_list.append((dst, rel, edge_props))
        self._cache_out[src] = out_list
        if not had_out and self._cache_out_keys is not None:
            self._cache_out_keys.append(src)

        in_list = list(self._get_in(dst))
        in_list.append((src, rel, edge_props))
        self._cache_in[dst] = in_list

        self._index_add(src, rel)

        if self._batching:
            # defer the writes; flush persists each dirty node once
            self._dirty_out.add(src)
            self._dirty_in.add(dst)
        else:
            self._out_stash[src] = out_list
            self._in_stash[dst] = in_list

    def _flush_batch(self):
        for src in self._dirty_out:
            self._out_stash[src] = self._cache_out[src]
        for dst in self._dirty_in:
            self._in_stash[dst] = self._cache_in[dst]
        self._dirty_out.clear()
        self._dirty_in.clear()

    def edge(self, src, dst, rel=None):
        """Return the properties of the FIRST edge matching (src, dst, rel).
        Parallel edges exist in a multigraph — use edges_between() for all."""
        for d, r, props in self._get_out(src):
            if d == dst and r == rel:
                return dict(props)
        raise KeyError((src, dst, rel))

    def edges_between(self, src, dst, rel=_UNSET):
        """All parallel edges src -> dst as (rel, props) tuples, optionally
        restricted to a specific rel (rel=None matches only rel-less edges)."""
        return [
            (r, dict(props))
            for d, r, props in self._get_out(src)
            if d == dst and (rel is _UNSET or r == rel)
        ]

    def has_edge(self, src, dst, rel=None):
        for d, r, _ in self._get_out(src):
            if d == dst and r == rel:
                return True
        return False

    def _edge_matches(self, entry, target, rel, match):
        if entry[0] != target or entry[1] != rel:
            return False
        if match:
            props = entry[2] if len(entry) > 2 else {}
            return all(props.get(k) == v for k, v in match.items())
        return True

    def remove_edge(self, src, dst, rel=None, **match):
        """Remove edges matching (src, dst, rel). With **match kwargs,
        only edges whose properties also match are removed."""
        out_list = self._get_out(src)
        new_out = [e for e in out_list if not self._edge_matches(e, dst, rel, match)]
        if len(new_out) == len(out_list):
            raise KeyError((src, dst, rel))

        if new_out:
            self._out_stash[src] = new_out
        elif self._out_stash.has(src):
            del self._out_stash[src]

        in_list = self._get_in(dst)
        new_in = [e for e in in_list if not self._edge_matches(e, src, rel, match)]
        if new_in:
            self._in_stash[dst] = new_in
        elif self._in_stash.has(dst):
            del self._in_stash[dst]

        self._invalidate(src)
        self._invalidate(dst)

    def edges_of(self, node_id, direction="out"):
        results = []
        if direction in ("out", "both"):
            results.extend((o, r, dict(p)) for o, r, p in self._get_out(node_id))
        if direction in ("in", "both"):
            results.extend((o, r, dict(p)) for o, r, p in self._get_in(node_id))
        return results

    @property
    def edges(self):
        result = []
        for src in self._out_keys():
            for dst, rel, props in self._get_out(src):
                result.append((src, dst, rel, dict(props)))
        return result

    # -- Query --

    def edges_where(self, rel=_UNSET, **kwargs):
        """Filter edges by Django-style predicates.

        Operators: __gt, __lt, __gte, __lte, __ne, __contains, __in,
                   __startswith, __endswith (no suffix = equality).
        Prefixes: source__ / target__ for node props, rel / rel__op for
                  the relationship string, bare name for edge props.

        rel=None matches only rel-less edges (omit rel to match any).
        Type-mismatched comparisons (weight__gt=1 against weight='heavy')
        are non-matches, not errors. An absent property fails every
        predicate except __ne.

        An exact ``rel=`` filter is served from the rel index, so the query
        visits only sources that have an edge of that rel rather than scanning
        the whole graph; other predicates are then applied within that set.

        Returns list of (src, dst, rel, props) tuples.
        """
        if rel is not _UNSET:
            kwargs["rel"] = rel
        # index fast-path: an exact rel equality ("rel" in kwargs, as opposed to
        # rel__contains etc.) means only sources indexed under that rel can match
        if "rel" in kwargs:
            sources = self._rel_sources(kwargs["rel"])
        else:
            sources = self._out_keys()
        results = []
        for src in sources:
            src_props = None
            for dst, edge_rel, props in self._get_out(src):
                if src_props is None:
                    src_props = self._get_node_props(src) or {}
                dst_props = self._get_node_props(dst) or {}
                if _match(src_props, dst_props, edge_rel, props, kwargs):
                    results.append((src, dst, edge_rel, dict(props)))
        return results

    def edges_with_rel(self, rel):
        """All edges of a given rel as (src, dst, rel, props) tuples — the
        index fast-path, without predicate filtering."""
        return self.edges_where(rel=rel)

    def add_edges_bulk(self, edges):
        """Add edges in batch, minimizing read-modify-write cycles.

        Args:
            edges: iterable of (src, dst, rel, props_dict) tuples
        """
        out_new = defaultdict(list)
        in_new = defaultdict(list)
        nodes_seen = set()

        for src, dst, rel, props in edges:
            if src not in nodes_seen:
                if not self.has_node(src):
                    self.add_node(src)
                nodes_seen.add(src)
            if dst not in nodes_seen:
                if not self.has_node(dst):
                    self.add_node(dst)
                nodes_seen.add(dst)
            props = dict(props)  # detach from the caller's dict
            out_new[src].append((dst, rel, props))
            in_new[dst].append((src, rel, props))
            self._index_add(src, rel)

        for src, new_entries in out_new.items():
            out_list = list(self._get_out(src))
            out_list.extend(new_entries)
            self._out_stash[src] = out_list
            self._cache_out[src] = out_list

        for dst, new_entries in in_new.items():
            in_list = list(self._get_in(dst))
            in_list.extend(new_entries)
            self._in_stash[dst] = in_list
            self._cache_in[dst] = in_list

        # sources may have gained their first out-edges; relist lazily
        self._cache_out_keys = None

    # -- Neighbors --

    def neighbors(self, node_id, rel=None, direction="out"):
        edges = self.edges_of(node_id, direction=direction)
        seen = set()
        result = []
        for entry in edges:
            other = entry[0]
            edge_rel = entry[1]
            if rel is not None and edge_rel != rel:
                continue
            if other not in seen:
                seen.add(other)
                result.append(other)
        return result

    # -- Traversal --

    def traverse(self, start, depth=1, rel=None, direction="out"):
        if not self.has_node(start):
            raise KeyError(start)

        visited = {start}
        levels = {0: [start]}
        frontier = [start]

        for level in range(1, depth + 1):
            next_frontier = []
            for node_id in frontier:
                for nbr in self.neighbors(node_id, rel=rel, direction=direction):
                    if nbr not in visited:
                        visited.add(nbr)
                        next_frontier.append(nbr)
            if not next_frontier:
                break
            levels[level] = next_frontier
            frontier = next_frontier

        return levels

    def shortest_path(self, src, dst, rel=None, direction="out"):
        if not self.has_node(src):
            raise KeyError(src)
        if not self.has_node(dst):
            raise KeyError(dst)
        if src == dst:
            return [src]

        visited = {src}
        parent = {src: None}
        queue = deque([src])

        while queue:
            current = queue.popleft()
            for nbr in self.neighbors(current, rel=rel, direction=direction):
                if nbr not in visited:
                    visited.add(nbr)
                    parent[nbr] = current
                    if nbr == dst:
                        path = []
                        node = dst
                        while node is not None:
                            path.append(node)
                            node = parent[node]
                        return list(reversed(path))
                    queue.append(nbr)

        return None

    # -- Utility --

    @property
    def num_edges(self):
        count = 0
        for node_id in self._out_keys():
            count += len(self._get_out(node_id))
        return count

    def clear(self):
        self._nodes_stash.clear()
        self._out_stash.clear()
        self._in_stash.clear()
        self._invalidate()

    def __len__(self):
        return len(self._nodes_stash)

    def __contains__(self, node_id):
        return self.has_node(node_id)

    def __repr__(self):
        return f"GraphStash({self._name!r}, nodes={len(self)}, edges={self.num_edges})"


class _GraphBatch:
    """Buffers edge writes for one GraphStash; flushes on exit (including on
    exception, so a partial load is still persisted consistently). Not
    reentrant — nested batches on one graph raise."""

    def __init__(self, graph):
        self._graph = graph

    def __enter__(self):
        if self._graph._batching:
            raise RuntimeError("GraphStash.batch() is not reentrant")
        self._graph._batching = True
        return self._graph

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            self._graph._flush_batch()
        finally:
            self._graph._batching = False
        return False
