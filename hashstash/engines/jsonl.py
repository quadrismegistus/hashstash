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

    @log.debug
    def __init__(self, *args, compress=RAW_NO_COMPRESS, b64=False, **kwargs):
        super().__init__(*args, compress=compress, b64=b64, **kwargs)
        self._keyset = OrderedSet()
        self._keyset_loaded = False

    def _ensure_keyset_loaded(self) -> None:
        if self._keyset_loaded:
            return
        for row in iter_jsonl(self.path):
            self._keyset.add(row[self.key_name])
            if row.get(self.delete_name):
                self._keyset.remove(row[self.key_name])
        self._keyset_loaded = True

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

        encoded_values = []
        timestamps = []
        for row in iter_jsonl(self.path):
            if row[self.key_name] == encoded_key:
                if row.get(self.delete_name):
                    encoded_values = []
                    timestamps = []
                else:
                    encoded_values.append(row[self.value_name])
                    timestamps.append(row.get(self.written_at_name, 0.0))

        encoded_values, timestamps = _filter_by_time(
            encoded_values, timestamps, before=before, after=after
        )
        if not encoded_values:
            return default

        decoded_values = [self.decode_value(ev) for ev in encoded_values]

        if with_metadata:
            decoded_values = [
                {"_version": vi + 1, "_value": v, "_written_at": t}
                for vi, (v, t) in enumerate(zip(decoded_values, timestamps))
            ]
        if not self._all_results(all_results):
            decoded_values = decoded_values[-1:]

        return decoded_values


    def _append_line(self, obj) -> None:
        try:
            os.makedirs(self.path_dirname, exist_ok=True)
        except Exception as e:
            pass
        
        line = json.dumps(obj) + "\n"
        with open(self.path, "a", encoding="utf-8") as fh:
            fh.write(line)

    def clear(self) -> None:
        self._keyset = OrderedSet()
        self._keyset_loaded = False
        if os.path.exists(self.path):
            try:
                os.remove(self.path)
            except Exception as e:
                pass

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
    
    def _del(self, encoded_key: Any) -> None:
        obj = {
            self.key_name: encoded_key,
            self.value_name: None,
            self.delete_name: True,
            self.written_at_name: time.time(),
        }
        with self:
            self._append_line(obj)
            self._keyset.remove(encoded_key)

    def new_unencoded_value(self, unencoded_value: Any, **kwargs):
        return unencoded_value

    @log.debug
    def _keys(self):
        yield from self._keyset

    @log.debug
    def _values(self):
        for row in iter_jsonl(self.path):
            if not row.get(self.delete_name):
                yield row[self.value_name]

    @log.debug
    def _items(self):
        for row in iter_jsonl(self.path):
            if not row.get(self.delete_name):
                yield row[self.key_name], row[self.value_name]
    
    @log.debug
    def items(self, all_results=None, with_metadata=False, before=None, after=None, **kwargs):
        key2entries = defaultdict(list)  # encoded_key -> [(encoded_val, timestamp), ...]
        for row in iter_jsonl(self.path):
            _key = row[self.key_name]
            if row.get(self.delete_name):
                key2entries[_key] = []
            else:
                key2entries[_key].append((row[self.value_name], row.get(self.written_at_name, 0.0)))

        for _key, entries in key2entries.items():
            if not entries:
                continue
            encoded_vals, timestamps = zip(*entries)
            encoded_vals, timestamps = _filter_by_time(
                list(encoded_vals), list(timestamps), before=before, after=after
            )
            if not encoded_vals:
                continue
            key = self.decode_key(_key)
            for vi, (_val, t) in enumerate(zip(encoded_vals, timestamps)):
                val = self.decode_value(_val)
                if with_metadata:
                    yield key, {"_version": vi + 1, "_value": val, "_written_at": t}
                else:
                    yield key, val

    def items_l(self, all_results=None, with_metadata=None, before=None, after=None, **kwargs):
        return list(self.items(
            all_results=all_results, with_metadata=with_metadata,
            before=before, after=after, **kwargs,
        ))

