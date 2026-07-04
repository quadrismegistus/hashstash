# Explicit stdlib imports: this package's `from . import *` chains are
# circular, and whether a name has landed in the package namespace yet
# depends on import order (spawn workers + editable installs order imports
# differently). Never rely on the star-chain for stdlib names.
from typing import Any
from typing import Union
import os

from . import *


class DuckDBHashStash(BaseHashStash):
    """Key-value engine backed by a single DuckDB table.

    Keys and values are stored as BLOBs. The base encoder hands us bytes
    (``string_keys``/``string_values`` are False), and DuckDB returns BLOB
    columns as ``bytes``, so the round-trip is byte-consistent in both
    directions. This is a solid key-value store; it does NOT attempt native
    DataFrame assembly.
    """

    engine = "duckdb"
    filename = "data.duckdb"
    # DuckDB round-trips BLOB columns as bytes; keep keys/values as bytes so
    # what we store is exactly what we read back (a str stored in a BLOB comes
    # back as bytes anyway, which would desync string_keys/string_values).
    string_keys = False
    string_values = False
    # DuckDB manages its own concurrency (single-writer per file); a local file
    # lock can't coordinate across processes for it anyway.
    needs_lock = False
    needs_reconnect = False

    @log.debug
    @retry_patiently()
    def get_db(self):
        import duckdb

        os.makedirs(self.path_dirname, exist_ok=True)
        conn = duckdb.connect(self.path)
        conn.execute(
            "CREATE TABLE IF NOT EXISTS hashstash (key BLOB PRIMARY KEY, value BLOB)"
        )
        return conn

    @staticmethod
    def _as_blob(x):
        """DuckDB binds bytes to BLOB; encode str keys/values consistently."""
        return x.encode("utf-8") if isinstance(x, str) else x

    @log.debug
    def _set(self, encoded_key: Union[str, bytes], encoded_value: Union[str, bytes]) -> None:
        with self.db as db:
            db.execute(
                "INSERT OR REPLACE INTO hashstash (key, value) VALUES (?, ?)",
                [self._as_blob(encoded_key), self._as_blob(encoded_value)],
            )

    @log.debug
    def _get(self, encoded_key: Union[str, bytes], default: Any = None) -> Any:
        with self.db as db:
            row = db.execute(
                "SELECT value FROM hashstash WHERE key = ?",
                [self._as_blob(encoded_key)],
            ).fetchone()
        return row[0] if row is not None else default

    @log.debug
    def _has(self, encoded_key: Union[str, bytes]) -> bool:
        with self.db as db:
            row = db.execute(
                "SELECT 1 FROM hashstash WHERE key = ? LIMIT 1",
                [self._as_blob(encoded_key)],
            ).fetchone()
        return row is not None

    @log.debug
    def _del(self, encoded_key: Union[str, bytes]) -> None:
        with self.db as db:
            db.execute(
                "DELETE FROM hashstash WHERE key = ?",
                [self._as_blob(encoded_key)],
            )

    @log.debug
    def __len__(self) -> int:
        with self.db as db:
            return db.execute("SELECT COUNT(*) FROM hashstash").fetchone()[0]

    @log.debug
    def _keys(self):
        with self.db as db:
            for row in db.execute("SELECT key FROM hashstash").fetchall():
                yield row[0]

    @log.debug
    def _values(self):
        with self.db as db:
            for row in db.execute("SELECT value FROM hashstash").fetchall():
                yield row[0]

    @log.debug
    def _items(self):
        with self.db as db:
            for row in db.execute("SELECT key, value FROM hashstash").fetchall():
                yield row[0], row[1]

    @log.debug
    def clear(self) -> "DuckDBHashStash":
        # base clear() closes the pooled connection (releasing DuckDB's file
        # lock) and removes path_dirname; also drop the write-ahead-log left
        # beside the db file when we don't own the whole directory.
        super().clear()
        wal = self.path + ".wal"
        if os.path.exists(wal):
            try:
                os.remove(wal)
            except Exception as e:
                log.debug(f"error removing DuckDB wal {wal}: {e}")
        return self
