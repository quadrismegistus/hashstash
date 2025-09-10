from . import *


class _JSONLDB:
    """
    Minimal mapping-like backend over a single JSONL file.

    - Each set appends a line: {"key": <encoded_key:str>, "value": <encoded_value:str>}
    - Deletion appends a tombstone: {"key": <encoded_key:str>, "delete": true}
    - On first access, lazily scans the file to build an in-memory index:
        key -> list[str encoded_single_value]
      while respecting tombstones (removing keys upon last delete entry).
    - get(key) returns encoded bytes/str representing the encoded list of values
      (built by decoding each single encoded value and re-encoding the list),
      so it matches BaseHashStash expectations.
    """

    def __init__(self, stash: "JSONLHashStash") -> None:
        self._stash = stash
        self._path = stash.path
        self._loaded = False
        self._index = {}  # encoded_key(str) -> list[str encoded_single_value]

    def _ensure_loaded(self) -> None:
        if self._loaded:
            return
        self._index = {}
        if os.path.exists(self._path):
            try:
                with open(self._path, "r", encoding="utf-8") as fh:
                    for line in fh:
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            entry = json.loads(line)
                        except Exception:
                            continue
                        key = entry.get("key")
                        if key is None:
                            continue
                        if entry.get("delete") is True:
                            # Tombstone: remove key from index
                            self._index.pop(key, None)
                            continue
                        if "value" in entry:
                            val = entry["value"]
                            if not isinstance(val, (str, bytes)):
                                # Persist only strings; coerce others via json
                                val = str(val)
                            self._index.setdefault(key, []).append(val)
            except Exception as e:
                log.error(f"Failed to load JSONL DB at {self._path}: {e}")
        self._loaded = True

    def _append_line(self, obj: dict) -> None:
        os.makedirs(os.path.dirname(self._path), exist_ok=True)
        with open(self._path, "a", encoding="utf-8") as fh:
            fh.write(json.dumps(obj, ensure_ascii=False))
            fh.write("\n")

    # Mapping protocol
    def get(self, encoded_key, default=None):
        self._ensure_loaded()
        key_s = encoded_key if isinstance(encoded_key, str) else encoded_key.decode("utf-8")
        vals = self._index.get(key_s)
        if not vals:
            return default
        # Build list-of-values by decoding each single encoded value
        try:
            decoded_vals = [self._stash.decode_value(v) for v in vals]
            # Re-encode list-of-values to match Base expectations
            combined = self._stash.encode_value(decoded_vals)
            return combined
        except Exception as e:
            log.error(f"JSONL get error for key: {e}")
            return default

    def __getitem__(self, encoded_key):
        res = self.get(encoded_key)
        if res is None:
            raise KeyError(encoded_key)
        return res

    def __setitem__(self, encoded_key, encoded_value):
        # encoded_value represents encoded list-of-values; we need just the newest single value
        try:
            values = self._stash.decode_value(encoded_value)
            # values is a python list (per Base.new_unencoded_value semantics)
            new_value = values[-1] if isinstance(values, list) and values else values
        except Exception as e:
            log.error(f"JSONL set decode error: {e}")
            # As a fallback, treat the whole encoded_value as the single value
            new_value = encoded_value

        # Re-encode the single value using stash settings (string_values expected True)
        try:
            single_encoded = self._stash.encode(
                self._stash.serialize(new_value),
                as_string=self._stash.string_values,
            )
            key_s = encoded_key if isinstance(encoded_key, str) else encoded_key.decode("utf-8")
            val_s = single_encoded if isinstance(single_encoded, str) else single_encoded.decode("utf-8")
            self._append_line({"key": key_s, "value": val_s})
            # update index in-memory
            self._ensure_loaded()
            self._index.setdefault(key_s, []).append(val_s)
        except Exception as e:
            log.error(f"JSONL set error: {e}")

    def __delitem__(self, encoded_key):
        key_s = encoded_key if isinstance(encoded_key, str) else encoded_key.decode("utf-8")
        self._append_line({"key": key_s, "delete": True})
        # Update index
        self._ensure_loaded()
        self._index.pop(key_s, None)

    def __contains__(self, encoded_key):
        self._ensure_loaded()
        key_s = encoded_key if isinstance(encoded_key, str) else encoded_key.decode("utf-8")
        return key_s in self._index

    def __iter__(self):
        self._ensure_loaded()
        return iter(self._index.keys())

    def __len__(self):
        self._ensure_loaded()
        return len(self._index)


class JSONLHashStash(BaseHashStash):
    engine = "jsonl"
    filename = "data.jsonl"
    string_keys = True
    string_values = True
    needs_reconnect = False

    @log.debug
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Force defaults suitable for external readability:
        # - No compression
        # - No base64 (plain UTF-8 strings)
        if self.compress != RAW_NO_COMPRESS:
            self.compress = RAW_NO_COMPRESS
        # Base sets b64 True if compress truthy; ensure disabled for jsonl
        self.b64 = False

    @log.debug
    @retry_patiently()
    def get_db(self):
        # Ensure directory exists
        os.makedirs(self.path_dirname, exist_ok=True)
        return _JSONLDB(self)

    @log.debug
    def _set(self, encoded_key: str, encoded_value: Any) -> None:
        # Delegate to DB mapping, which appends a single line for the new value
        with self as cache, cache.db as db:
            db[encoded_key] = encoded_value

    @log.debug
    def _get(self, encoded_key: str, default: Any = None) -> Any:
        with self as cache, cache.db as db:
            return db.get(encoded_key, default)

    @log.debug
    def _has(self, encoded_key: Union[str, bytes]) -> bool:
        with self as cache, cache.db as db:
            return encoded_key in db

    @log.debug
    def __len__(self) -> int:
        with self as cache, cache.db as db:
            return len(db)

    @log.debug
    def _keys(self):
        with self as cache, cache.db as db:
            for k in db:
                yield k

    @log.debug
    def _del(self, encoded_key: Union[str, bytes]) -> None:
        with self as cache, cache.db as db:
            del db[encoded_key]


