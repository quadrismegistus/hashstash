from . import *
from collections import defaultdict
from .base import _filter_by_time

class JSONLHashStash(BaseHashStash):
    engine = "jsonl"
    filename = "data.jsonl"
    string_keys = True
    string_values = True
    needs_reconnect = False
    key_name = "__key__"
    value_name = "__value__"
    delete_name = "__delete__"
    written_at_name = "__written_at__"
    flat = False

    BINARY_SERIALIZERS = {"pickle"}
    to_dict_attrs = BaseHashStash.to_dict_attrs + ["flat"]

    @log.debug
    def __init__(self, *args, compress=RAW_NO_COMPRESS, b64=False, flat=True, **kwargs):
        if compress is None:
            compress = RAW_NO_COMPRESS
        if b64 is None:
            b64 = False
        if flat is None:
            flat = True
        if flat:
            b64 = False
        serializer = kwargs.get("serializer") or DEFAULT_SERIALIZER
        if not b64:
            if serializer in self.BINARY_SERIALIZERS:
                raise ValueError(
                    f"JSONLHashStash: b64=False requires a text serializer "
                    f"('hashstash' or 'jsonpickle'), but serializer={serializer!r} "
                    f"produces binary output. Pass b64=True or switch serializer."
                )
            compress = RAW_NO_COMPRESS
        self.flat = flat
        super().__init__(*args, compress=compress, b64=b64, **kwargs)
        self._keyset = OrderedSet()
        self._keyset_offset = 0  # bytes of the file already folded into _keyset
        self._flat_meta = frozenset([
            self.key_name, self.value_name, self.delete_name, self.written_at_name
        ])

    # --- flat mode key helpers ---

    def _flat_row_key(self, key):
        """Encode a Python key as a JSON-native value for storage in __key__."""
        return json.loads(self.serialize(key))

    def _flat_ks(self, row_key):
        """Stable hashable string for the keyset from a __key__ row value."""
        return json.dumps(row_key, sort_keys=True)

    def _flat_decode_row_key(self, row_key):
        """Reconstruct a Python key from the __key__ row value."""
        return self.deserialize(json.dumps(row_key))

    def _flat_row_to_value(self, row):
        """Extract the value dict from a flat row; fall back to __value__ for non-flat rows."""
        if self.value_name in row:
            return self.decode_value(row[self.value_name])
        return {k: v for k, v in row.items() if k not in self._flat_meta}

    # --- key encode/decode overrides for flat mode ---

    def encode_key(self, unencoded_key):
        if not self.flat:
            return super().encode_key(unencoded_key)
        return self._flat_ks(self._flat_row_key(unencoded_key))

    def decode_key(self, encoded_key, as_string=False):
        if not self.flat:
            return super().decode_key(encoded_key, as_string=as_string)
        return self._flat_decode_row_key(json.loads(encoded_key))

    # --- keyset loading ---

    def _ensure_keyset_loaded(self) -> None:
        """Fold any rows appended since the last scan into the keyset.

        Other writers append to the same file, so a one-shot load goes permanently
        stale; instead we remember how far we've read and incrementally fold new
        lines on every access (a single getsize() when nothing changed)."""
        try:
            size = os.path.getsize(self.path)
        except OSError:
            self._keyset = OrderedSet()
            self._keyset_offset = 0
            return
        if size < self._keyset_offset:
            # file was truncated or replaced (e.g. clear()): rescan from the top
            self._keyset = OrderedSet()
            self._keyset_offset = 0
        if size == self._keyset_offset:
            return
        with open(self.path, "r", encoding="utf-8") as f:
            f.seek(self._keyset_offset)
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    row = json.loads(line)
                except Exception:
                    log.warning(f"skipping unparseable JSONL line in {self.path}")
                    continue
                rk = row[self.key_name]
                ks = self._flat_ks(rk) if self.flat else rk
                if row.get(self.delete_name):
                    self._keyset.discard(ks)
                else:
                    self._keyset.add(ks)
            self._keyset_offset = f.tell()

    # --- get_all ---

    @log.debug
    def get_all(
        self,
        unencoded_key: Any = None,
        default: Any = None,
        with_metadata: bool = None,
        all_results: bool = True,
        before=None,
        after=None,
        **kwargs,
    ) -> Any:
        if not os.path.exists(self.path):
            return default

        self._ensure_keyset_loaded()
        encoded_key = self.encode_key(unencoded_key)

        if encoded_key not in self._keyset:
            return default

        values = []
        timestamps = []

        if self.flat:
            for row in iter_jsonl(self.path):
                row_ks = self._flat_ks(row[self.key_name])
                if row_ks != encoded_key:
                    continue
                if row.get(self.delete_name):
                    values, timestamps = [], []
                else:
                    values.append(self._flat_row_to_value(row))
                    timestamps.append(row.get(self.written_at_name, 0.0))
        else:
            for row in iter_jsonl(self.path):
                if row[self.key_name] == encoded_key:
                    if row.get(self.delete_name):
                        values, timestamps = [], []
                    else:
                        values.append(self.decode_value(row[self.value_name]))
                        timestamps.append(row.get(self.written_at_name, 0.0))

        if not self.append_mode:
            # overwrite semantics: only the last write is "the" value, matching
            # every other engine (the log retains history but it is not queryable)
            values, timestamps = values[-1:], timestamps[-1:]

        values, timestamps = _filter_by_time(values, timestamps, before=before, after=after)
        if not values:
            return default

        if with_metadata:
            values = [
                {"_version": vi + 1, "_value": v, "_written_at": t}
                for vi, (v, t) in enumerate(zip(values, timestamps))
            ]
        if not self._all_results(all_results):
            values = values[-1:]
        return values

    # --- write ---

    def _append_line(self, obj) -> None:
        try:
            os.makedirs(self.path_dirname, exist_ok=True)
        except Exception:
            pass
        line = json.dumps(obj) + "\n"
        with open(self.path, "a", encoding="utf-8") as fh:
            fh.write(line)

    def clear(self) -> "JSONLHashStash":
        for sub in self.children:
            sub.clear()
        self.close()
        self._keyset = OrderedSet()
        self._keyset_offset = 0
        if os.path.exists(self.path):
            try:
                os.remove(self.path)
            except Exception as e:
                log.warning(f"could not remove {self.path}: {e}")
        return self

    @log.debug
    def set(self, unencoded_key: Any, unencoded_value: Any, append=None) -> None:
        if not self.flat:
            return super().set(unencoded_key, unencoded_value, append=append)
        row_key = self._flat_row_key(unencoded_key)
        ks = self._flat_ks(row_key)
        if isinstance(unencoded_value, dict):
            collisions = set(unencoded_value.keys()) & self._flat_meta
            if collisions:
                raise ValueError(
                    f"JSONLHashStash flat mode: value dict contains reserved field names: "
                    f"{sorted(collisions)}"
                )
            obj = {self.key_name: row_key, **unencoded_value, self.written_at_name: time.time()}
        else:
            encoded_value = self.encode_value(self.new_unencoded_value(unencoded_value))
            obj = {self.key_name: row_key, self.value_name: encoded_value, self.written_at_name: time.time()}
        self._ensure_keyset_loaded()
        with self:
            self._append_line(obj)
            self._keyset.add(ks)

    @log.debug
    def _set(self, encoded_key: str, encoded_value: str) -> None:
        obj = {
            self.key_name: encoded_key,
            self.value_name: encoded_value,
            self.written_at_name: time.time(),
        }
        with self:
            self._append_line(obj)
            self._keyset.add(encoded_key)

    def _has(self, encoded_key: Any) -> bool:
        self._ensure_keyset_loaded()
        return encoded_key in self._keyset

    def __len__(self) -> int:
        self._ensure_keyset_loaded()
        return len(self._keyset)

    @log.debug
    def _del(self, encoded_key: Any) -> None:
        if self.flat:
            row_key = json.loads(encoded_key)
            obj = {
                self.key_name: row_key,
                self.delete_name: True,
                self.written_at_name: time.time(),
            }
        else:
            obj = {
                self.key_name: encoded_key,
                self.value_name: None,
                self.delete_name: True,
                self.written_at_name: time.time(),
            }
        with self:
            self._append_line(obj)
            self._keyset.discard(encoded_key)

    def new_unencoded_value(self, unencoded_value: Any, **kwargs):
        return unencoded_value

    @log.debug
    def _keys(self):
        yield from self._keyset

    @log.debug
    def _values(self):
        for row in iter_jsonl(self.path):
            if not row.get(self.delete_name):
                # flat rows have no __value__ field: extract from the row itself
                yield self._flat_row_to_value(row) if self.flat else row[self.value_name]

    @log.debug
    def _items(self):
        for row in iter_jsonl(self.path):
            if not row.get(self.delete_name):
                value = self._flat_row_to_value(row) if self.flat else row[self.value_name]
                yield row[self.key_name], value

    @log.debug
    def items(self, all_results=None, with_metadata=False, before=None, after=None, **kwargs):
        key2entries = defaultdict(list)
        for row in iter_jsonl(self.path):
            rk = row[self.key_name]
            ks = self._flat_ks(rk) if self.flat else rk
            if row.get(self.delete_name):
                key2entries[ks] = []
            else:
                val = self._flat_row_to_value(row) if self.flat else row[self.value_name]
                key2entries[ks].append((val, row.get(self.written_at_name, 0.0), rk))

        for ks, entries in key2entries.items():
            if not entries:
                continue
            if not self.append_mode:
                entries = entries[-1:]  # overwrite semantics: latest write only
            vals, timestamps, rks = zip(*entries)
            vals, timestamps = _filter_by_time(
                list(vals), list(timestamps), before=before, after=after
            )
            if not vals:
                continue
            if self.flat:
                key = self._flat_decode_row_key(rks[0])
            else:
                key = self.decode_key(rks[0])
                vals = [self.decode_value(v) for v in vals]
            for vi, (v, t) in enumerate(zip(vals, timestamps)):
                if with_metadata:
                    yield key, {"_version": vi + 1, "_value": v, "_written_at": t}
                else:
                    yield key, v

    def items_l(self, all_results=None, with_metadata=None, before=None, after=None, **kwargs):
        return list(self.items(
            all_results=all_results, with_metadata=with_metadata,
            before=before, after=after, **kwargs,
        ))
