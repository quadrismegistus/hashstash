from . import *


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
        self._index = {}  # encoded_key(str) -> last encoded_value(str)

    def _ensure_loaded(self) -> None:
        if self._loaded:
            return
        self._index = {}
        if os.path.exists(self.path):
            try:
                with open(self.path, "r", encoding="utf-8") as fh:
                    for line in fh:
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            entry = json.loads(line)
                        except Exception:
                            continue
                        k = entry.get("key")
                        if k is None:
                            continue
                        if entry.get("delete") is True:
                            self._index.pop(k, None)
                            continue
                        v = entry.get("value")
                        if v is not None:
                            if not isinstance(v, (str, bytes)):
                                v = str(v)
                            self._index[k] = v if isinstance(v, str) else v.decode("utf-8")
            except Exception as e:
                log.error(f"Failed to load JSONL at {self.path}: {e}")
        self._loaded = True

    def _append_line(self, obj: dict) -> None:
        os.makedirs(self.path_dirname, exist_ok=True)
        with open(self.path, "a", encoding="utf-8") as fh:
            fh.write(json.dumps(obj, ensure_ascii=False))
            fh.write("\n")

    @log.debug
    def _get(self, encoded_key, default=None):
        self._ensure_loaded()
        k = encoded_key if isinstance(encoded_key, str) else encoded_key.decode("utf-8")
        v = self._index.get(k)
        return v if v is not None else default

    @log.debug
    def _set(self, encoded_key, encoded_value) -> None:
        self._ensure_loaded()
        with self:
            self._append_line({"key": encoded_key, "value": encoded_value})
            self._index[encoded_key] = encoded_value

    @log.debug
    def _has(self, encoded_key) -> bool:
        self._ensure_loaded()
        k = encoded_key if isinstance(encoded_key, str) else encoded_key.decode("utf-8")
        return k in self._index

    @log.debug
    def __len__(self) -> int:
        self._ensure_loaded()
        return len(self._index)

    @log.debug
    def _keys(self):
        self._ensure_loaded()
        for k in self._index.keys():
            yield k

    @log.debug
    def _del(self, encoded_key) -> None:
        self._ensure_loaded()
        k = encoded_key if isinstance(encoded_key, str) else encoded_key.decode("utf-8")
        with self:
            self._append_line({"key": k, "delete": True})
            self._index.pop(k, None)