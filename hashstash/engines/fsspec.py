# Explicit stdlib imports: the package's `from . import *` chains are circular.
import os
import posixpath

from . import *
from .pairtree import PairtreeHashStash


class FsspecHashStash(PairtreeHashStash):
    """Pairtree layout served over any fsspec-supported filesystem — S3, GCS,
    Azure, HDFS, SFTP, HTTP, or the in-memory 'memory://' filesystem — giving a
    shared, serverless cache backed by object storage.

    Point ``root_dir`` at a URL: ``HashStash(engine='fsspec',
    root_dir='s3://my-bucket/cache')``. Filesystem options (credentials, etc.)
    go in ``storage_options``. Requires the ``fsspec`` package plus the backend
    driver (``s3fs`` for S3, ``gcsfs`` for GCS, ...).

    Inherits every pairtree read/write/query path; only the low-level file
    primitives are overridden to use the fsspec filesystem. Remote atomic
    rename is best-effort: object stores emulate it with copy+delete, so a
    torn write is far less likely than on local disk but not transactionally
    guaranteed.
    """

    engine = "fsspec"
    needs_lock = False  # a local file lock can't coordinate remote writers

    def __init__(self, *args, storage_options=None, **kwargs):
        self.storage_options = storage_options or {}
        self._fs = None
        self._fs_root = None
        super().__init__(*args, **kwargs)

    @property
    def fs(self):
        if self._fs is None:
            import fsspec

            # split "protocol://root" from the pairtree-built self.path; the fs
            # is bound to the protocol, paths are the remainder
            self._fs, self._fs_root = fsspec.core.url_to_fs(
                self.path, **self.storage_options
            )
        return self._fs

    def _strip_protocol(self, path):
        # url_to_fs paths are protocol-relative; fsspec accepts either form but
        # we normalize so joins/compares are consistent
        return self.fs._strip_protocol(path)

    # --- filesystem primitives (override pairtree's os-based ones) --------

    def _fs_join(self, *parts):
        # object stores are posix-style regardless of the host OS
        return posixpath.join(*parts)

    def _fs_dirname(self, path):
        return posixpath.dirname(path)

    def _fs_basename(self, path):
        return posixpath.basename(path)

    def _fs_exists(self, path):
        return self.fs.exists(path)

    def _fs_isfile(self, path):
        return self.fs.isfile(path)

    def _fs_listdir(self, path):
        # return bare names (pairtree expects listdir semantics, not full paths)
        if not self.fs.exists(path):
            raise FileNotFoundError(path)
        return [self._fs_basename(p.rstrip("/")) for p in self.fs.ls(path, detail=False)]

    def _fs_read(self, path):
        with self.fs.open(path, "rb") as f:
            return f.read()

    def _fs_write_atomic(self, path, data):
        # write to a temp key then move: object stores lack true rename, so this
        # is copy+delete under the hood — still safer than a partial in-place put
        self._fs_makedirs(self._fs_dirname(path))
        tmp_path = self._fs_join(
            self._fs_dirname(path), f".tmp.{os.getpid()}.{self._fs_basename(path)}"
        )
        with self.fs.open(tmp_path, "wb") as f:
            f.write(data)
        try:
            self.fs.mv(tmp_path, path)
        except Exception:
            # some backends can't overwrite via mv: fall back to put + cleanup
            with self.fs.open(path, "wb") as f:
                f.write(data)
            try:
                self.fs.rm(tmp_path)
            except Exception:
                pass

    def _fs_makedirs(self, path):
        try:
            self.fs.makedirs(path, exist_ok=True)
        except (FileExistsError, NotImplementedError):
            pass  # key-based stores (S3) have no real directories

    def _fs_remove(self, path):
        self.fs.rm(path)

    def _fs_rmtree(self, path):
        if self.fs.exists(path):
            self.fs.rm(path, recursive=True)

    def _fs_walk(self, path):
        if not self.fs.exists(path):
            return
        for root, _dirs, files in self.fs.walk(path):
            yield root, list(files)

    def clear(self):
        for sub in self.children:
            sub.clear()
        self.close()
        if getattr(self, "_owns_dir", True):
            # Same sweep as the base engine, through the fsspec primitives:
            # removing path_dirname outright deleted every sub-stash nested in
            # it, since sub() puts children inside the parent's param folder.
            try:
                entries = self._fs_listdir(self.path_dirname)
            except (FileNotFoundError, OSError):
                self._fs_rmtree(self.path)
                return self
            for entry in entries:
                entry_path = self._fs_join(self.path_dirname, entry)
                if self._owns_entry(entry, self._fs_isfile(entry_path)):
                    self._fs_rmtree(entry_path)
            try:
                if not self._fs_listdir(self.path_dirname):
                    self._fs_rmtree(self.path_dirname)
            except (FileNotFoundError, OSError):
                pass
        else:
            self._fs_rmtree(self.path)
        return self
