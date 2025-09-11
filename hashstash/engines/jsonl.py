from . import *
from collections import defaultdict

class JSONLHashStash(BaseHashStash):
    engine = "jsonl"
    filename = "data.jsonl"
    string_keys = True
    string_values = True
    needs_reconnect = False
    key_name = "__key__"
    value_name = "__value__"
    delete_name = "__delete__"

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
        **kwargs,
    ) -> Any:
        if not os.path.exists(self.path):
            return default
    
        self._ensure_keyset_loaded()
        
        encoded_key = self.encode_key(unencoded_key)
        if encoded_key not in self._keyset:
            return default
        
        encoded_values = []
        for row in iter_jsonl(self.path):
            if row[self.key_name] == encoded_key:
                if row.get(self.delete_name):
                    encoded_values = []
                else:
                    encoded_values.append(row[self.value_name])
        
        decoded_values = [self.decode_value(encoded_value) for encoded_value in encoded_values]

        if with_metadata:
            decoded_values = [
                {"_version": vi + 1, "_value": value} for vi, value in enumerate(decoded_values)
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

    # def encode_key(self, unencoded_key: Any) -> Union[str, bytes]:
    #     return json.dumps(super().encode_key(unencoded_key))

    # def encode_value(self, unencoded_value: Any) -> Union[str, bytes]:
    #     return json.dumps(super().encode_value(unencoded_value))

    # def decode_key(self, encoded_key: Any, as_string=False) -> Union[str, bytes]:
    #     return super().decode_key(json.loads(encoded_key), as_string=as_string)

    # def decode_value(self, encoded_value: Any, as_string=False) -> Union[str, bytes]:
    #     return super().decode_value(json.loads(encoded_value), as_string=as_string)

    @log.debug
    def _set(self, encoded_key: str, encoded_value: str) -> None:
        obj = {self.key_name: encoded_key, self.value_name: encoded_value}
        self._append_line(obj)
        self._keyset.add(encoded_key)

    def _has(self, encoded_key: Any) -> bool:
        self._ensure_keyset_loaded()
        return encoded_key in self._keyset

    def __len__(self) -> int:
        self._ensure_keyset_loaded()
        return len(self._keyset)
    
    def _del(self, encoded_key: Any) -> None:
        obj = {self.key_name: encoded_key, self.value_name: None, self.delete_name: True}
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
    def items(self, all_results=None, with_metadata=False, **kwargs):
        key_counts = Counter()
        for _key,_val in self._items():
            key_counts[_key] += 1
            key = self.decode_key(_key)
            val = self.decode_value(_val)

            if with_metadata:
                yield key, {"_version": key_counts[_key], "_value": val}
            else:
                yield key, val

    def items_l(self, all_results=None, with_metadata=None, **kwargs):
        key2vals = defaultdict(list)
        for row in iter_jsonl(self.path):
            _key = row[self.key_name]
            _val = row[self.value_name]
            _delete = row.get(self.delete_name)
            if _delete:
                key2vals[_key] = []
            else:
                key2vals[_key].append(_val)
        
        o = []
        for _key, _vals in key2vals.items():
            key = self.decode_key(_key)
            for vi, _val in enumerate(_vals):
                val = self.decode_value(_val)
                if with_metadata:
                    val = {"_version": vi + 1, "_value": val}
                o.append((key, val))
        return o

