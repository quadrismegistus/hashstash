from . import *
from collections import defaultdict

class JSONLHashStash(BaseHashStash):
    engine = "jsonl"
    filename = "data.jsonl"
    string_keys = True
    string_values = True
    needs_reconnect = False

    @log.debug
    def __init__(self, *args, compress=RAW_NO_COMPRESS, b64=False, **kwargs):
        super().__init__(*args, compress=compress, b64=b64, **kwargs)
        self._loaded = False
        self._index = defaultdict(list)  # encoded_key(str) -> last encoded_value(str)

    def _ensure_loaded(self) -> None:
        if self._loaded:
            return

        print(f"Loading JSONL from {self.path}")
        for entry in iter_jsonl(self.path):
            key = entry.pop("key", None)
            value = entry.pop("value", None)
            delete = entry.pop("delete", False)
            
            if key is None: 
                continue
            
            if delete: 
                self._index.pop(key, None)
                continue

            if value is None:
                continue

            # if not isinstance(value,str):
            #     value = str(value)
            
            #     value = value.decode("utf-8")
            #             value = str(value)
            #         value = value if isinstance(value, str) else value.decode("utf-8")
            self._index[key].append(value)
            
        self._loaded = True

    def _append_line(self, obj: dict) -> None:
        os.makedirs(self.path_dirname, exist_ok=True)
        with open(self.path, "a", encoding="utf-8") as fh:
            fh.write(json.dumps(obj, ensure_ascii=False))
            fh.write("\n")

    # @log.debug
    # def _get(self, encoded_key, default=None):
    #     self._ensure_loaded()
    #     k = encoded_key if isinstance(encoded_key, str) else encoded_key.decode("utf-8")
    #     v = self._index.get(k)
    #     return v if v is not None else default

    # @log.debug
    # def _set(self, encoded_key, encoded_value) -> None:
    #     self._ensure_loaded()
    #     with self:
    #         self._append_line({"key": encoded_key, "value": encoded_value})
    #         self._index[encoded_key].append(encoded_value)

    # @log.debug
    # def _has(self, encoded_key) -> bool:
    #     self._ensure_loaded()
    #     k = encoded_key if isinstance(encoded_key, str) else encoded_key.decode("utf-8")
    #     return k in self._index

    # @log.debug
    # def __len__(self) -> int:
    #     self._ensure_loaded()
    #     return len(self._index)

    # @log.debug
    # def _keys(self):
    #     self._ensure_loaded()
    #     for k in self._index.keys():
    #         yield k

    # @log.debug
    # def _del(self, encoded_key) -> None:
    #     self._ensure_loaded()
    #     k = encoded_key if isinstance(encoded_key, str) else encoded_key.decode("utf-8")
    #     with self:
    #         self._append_line({"key": k, "delete": True})
    #         self._index.pop(k, None)


    # @log.debug
    # def get(
    #     self,
    #     unencoded_key: Any = None,
    #     default: Any = None,
    #     with_metadata=False,
    #     as_dataframe=None,
    #     as_string=False,
    #     all_results=None,
    #     **kwargs,
    # ) -> Any:
    #     values = self.get_all(
    #         unencoded_key,
    #         default=None,
    #         with_metadata=with_metadata,
    #         all_results=all_results,
    #         as_dataframe=as_dataframe,
    #         **kwargs,
    #     )
    #     value = values[-1] if values else default
    #     return self.serialize(value) if as_string else value

    @log.debug
    def get_all(
        self,
        unencoded_key: Any = None,
        default: Any = None,
        with_metadata: bool = None,
        all_results: bool = True,
        **kwargs,
    ) -> Any:
        self._ensure_loaded()
        encoded_key = self.encode_key(unencoded_key)
        encoded_values = self._index[encoded_key]
        print(f"Encoded values: {encoded_values}")
        if encoded_values is None or encoded_values == []:
            return default

        values = [self.decode_value(encoded_value) for encoded_value in encoded_values]
        # if with_metadata:
        #     values = [
        #         {"_version": vi + 1, "_value": value} for vi, value in enumerate(values)
        #     ]
        if not self._all_results(all_results):
            values = values[-1:]
        return values

    @log.debug
    def set(self, unencoded_key: Any, unencoded_value: Any, append=None) -> None:
        encoded_key = self.encode_key(unencoded_key)
        encoded_value = self.encode_value(unencoded_value)
        self._append_line({"key": encoded_key, "value": encoded_value})
        self._index[encoded_key].append(encoded_value)

    @log.debug
    def has(self, unencoded_key: Any) -> bool:
        self._ensure_loaded()
        encoded_key = self.encode_key(unencoded_key)
        return encoded_key in self._index

    @log.debug
    def __len__(self) -> int:
        self._ensure_loaded()
        return len(self._index)

    @log.debug
    def delete(self, unencoded_key: Any) -> None:
        encoded_key = self.encode_key(unencoded_key)
        self._append_line({"key": encoded_key, "delete": True})
        self._index.pop(encoded_key, None)

    @log.debug
    def _keys(self):
        self._ensure_loaded()
        for encoded_key in self._index.keys():
            yield self.decode_key(encoded_key)