# Explicit stdlib imports: this package's `from . import *` chains are
# circular, and whether a name has landed in the package namespace yet
# depends on import order (spawn workers + editable installs order imports
# differently). Never rely on the star-chain for stdlib names.
from typing import Any
import os
import shutil
import time

from . import *
from .base import _filter_by_time


class PairtreeHashStash(BaseHashStash):
    engine = "pairtree"
    filename_is_dir = True
    key_filename = ".key"
    valtype_filename = ".valtype"
    metadata_cols = ["_version", "_written_at"]
    needs_lock = False

    def connect(self):
        pass

    # --- filesystem primitives -------------------------------------------
    # All disk access goes through these so alternative backends (see the
    # fsspec engine) can serve the same pairtree layout over remote storage
    # by overriding just this handful of methods.

    def _fs_join(self, *parts):
        return os.path.join(*parts)

    def _fs_dirname(self, path):
        return os.path.dirname(path)

    def _fs_basename(self, path):
        return os.path.basename(path)

    def _fs_exists(self, path):
        return os.path.exists(path)

    def _fs_isfile(self, path):
        return os.path.isfile(path)

    def _fs_listdir(self, path):
        return os.listdir(path)

    def _fs_read(self, path):
        with open(path, "rb") as f:
            return f.read()

    def _fs_write_atomic(self, path, data):
        # temp file + rename: a crash mid-write must not leave a truncated
        # version file that poisons every later read of this key
        self._fs_makedirs(self._fs_dirname(path))
        tmp_path = self._fs_join(
            self._fs_dirname(path),
            f".tmp.{os.getpid()}.{self._fs_basename(path)}",
        )
        with open(tmp_path, "wb") as f:
            f.write(data)
        os.replace(tmp_path, path)

    def _fs_makedirs(self, path):
        os.makedirs(path, exist_ok=True)

    def _fs_remove(self, path):
        os.remove(path)

    def _fs_rmtree(self, path):
        shutil.rmtree(path, ignore_errors=True)

    def _fs_walk(self, path):
        """Yield (dirpath, filenames) for every directory under path."""
        for root, _dirs, files in os.walk(path):
            yield root, files

    @log.debug
    def _get_path(self, encoded_key):
        hashed_key = self.hash(encoded_key)
        dir1, dir2, dir3, fname = (
            hashed_key[:2],
            hashed_key[2:4],
            hashed_key[4:6],
            hashed_key[6:],
        )
        return self._fs_join(self.path, dir1, dir2, dir3, fname)

    @log.debug
    def get_path(self, unencoded_key):
        return self._get_path(self.encode_key(unencoded_key))

    @log.debug
    def _get_path_key(self, encoded_key):
        return self._fs_join(self._get_path(encoded_key), self.key_filename)

    @log.debug
    def _get_path_valtype(self, encoded_key):
        return self._fs_join(self._get_path(encoded_key), self.valtype_filename)

    @log.debug
    def get_path_key(self, unencoded_key):
        return self._get_path_key(self.encode_key(unencoded_key))

    @log.debug
    def _get_path_values(self, encoded_key, all_results=None, with_metadata=None):
        path = self._get_path(encoded_key)
        if not self._fs_exists(path): return []
        try:
            paths = [
                self._fs_join(path, f)
                for f in self._fs_listdir(path)
                if f[0] != "." and f != self.key_filename
            ]
        except NotADirectoryError:
            return []
        paths.sort()
        if not self._all_results(all_results):
            paths = paths[-1:]
        if with_metadata:
            paths = self._get_path_values_metadata(paths, incl_path=True)
        return paths

    @log.debug
    def get_path_values(self, unencoded_key, all_results=True, with_metadata=False):
        return self._get_path_values(self.encode_key(unencoded_key), all_results, with_metadata)

    @log.debug
    def _get_path_value(self, encoded_key):
        paths = self._get_path_values(encoded_key)
        return paths[-1] if paths else None

    def get_path_value(self, unencoded_key):
        return self._get_path_value(self.encode_key(unencoded_key))

    @log.debug
    def _get_path_new_value(self, encoded_key):
        # the .pid suffix disambiguates concurrent writers hitting the same
        # microsecond (they used to silently overwrite each other's version);
        # readers parse the timestamp with splitext, which strips the suffix
        return self._fs_join(
            self._get_path(encoded_key),
            f"{int(time.time() * 1000000)}.{os.getpid()}",
        )

    @log.debug
    def get_path_new_value(self, unencoded_key):
        return self._get_path_new_value(self.encode_key(unencoded_key))

    @log.debug
    def get_all(
        self,
        unencoded_key: Any = None,
        default: Any = None,
        with_metadata=None,
        all_results=True,
        before=None,
        after=None,
        **kwargs,
    ) -> Any:
        paths_ld = self.get_path_values(
            unencoded_key,
            all_results=self._all_results(all_results),
            with_metadata=True,
        )
        after = self._ttl_after(after, kwargs)
        if before is not None or after is not None:
            timestamps = [p["_written_at"] for p in paths_ld]
            paths_ld, _ = _filter_by_time(paths_ld, timestamps, before=before, after=after)
        out = []
        for path_d in paths_ld:
            path = path_d.pop("_path")
            encoded_value = self._get_from_filepath(path)
            if encoded_value is None:
                # version file pruned by a concurrent writer between listing and
                # reading — skip it (clean miss) rather than decode None (crash)
                continue
            decoded_value = self.decode_value(encoded_value)
            if not with_metadata:
                out.append(decoded_value)
            else:
                path_d['_value'] = decoded_value
                out.append(path_d)
        return out if out else default

    def new_unencoded_value(self, unencoded_value: Any, *args, **kwargs):
        return unencoded_value # file versioning takes care of this

    def _get_from_filepath(self, filepath):
        if not self._fs_exists(filepath):
            return None
        try:
            return self._fs_read(filepath)
        except FileNotFoundError:
            # a concurrent writer's prune removed this version file between the
            # exists-check and the read (TOCTOU). Treat as a miss, not a crash.
            return None

    def _set_to_filepath(self, filepath, encoded_data):
        self._fs_write_atomic(filepath, encoded_data)

    @log.debug
    def _set(self, encoded_key: str, encoded_value: Any) -> None:
        self._set_key(encoded_key)
        filepath_value = self._get_path_new_value(encoded_key)
        self._set_to_filepath(filepath_value, encoded_value)
        if not self.append_mode:
            self._prune_dir(filepath_value)


    def _prune_dir(self, filepath_value):
        dir_path = self._fs_dirname(filepath_value)
        # value filenames are '{microsecond}.{pid}' — timestamp-ordered, so max()
        # is the newest. Keep the LATEST, not necessarily the file THIS writer
        # wrote: two concurrent same-key writers each pruning "all but mine"
        # deleted each other's file and left the key with NO value (data loss +
        # len/has disagreement). Keeping the shared max means the last write wins
        # and a value always survives; older versions are pruned as intended.
        files = [f for f in self._fs_listdir(dir_path) if f and f[0] != '.']
        if not files:
            return
        keep = max(files)
        for file in files:
            if file != keep:
                file_path = self._fs_join(dir_path, file)
                if self._fs_isfile(file_path):
                    try:
                        self._fs_remove(file_path)
                    except FileNotFoundError:
                        pass  # concurrent delete/prune already removed it


        

    @log.debug
    def _set_key(self, encoded_key):
        filepath_key = self._get_path_key(encoded_key)
        if not self._fs_exists(filepath_key):
            self._set_to_filepath(filepath_key, encoded_key)

    @log.debug
    def _has(self, encoded_key: bytes) -> bool:
        return bool(self._get_path_values(encoded_key))

    @log.debug
    def __len__(self):
        return sum(1 for _ in self.paths())

    @log.debug
    def paths(self):
        for root, files in self._fs_walk(self.path):
            if self.key_filename in files:
                yield root

    def paths_keys(self, all_results=False, with_metadata=None):
        for keypath, valpaths in self.paths_items(all_results=all_results, with_metadata=False):
            for valpath in valpaths:
                yield keypath

    def paths_values(self, all_results=None, with_metadata=None):
        yield from (
            v
            for k, v in self.paths_items(
                all_results=all_results, with_metadata=with_metadata
            )
        )

    @staticmethod
    def _parse_written_at(vpath):
        # filenames are '<micros>[.<pid>][.<io_ext>]': the timestamp is everything
        # before the first dot
        name = vpath.rsplit("/", 1)[-1].rsplit(os.sep, 1)[-1]
        try:
            return float(name.split(".", 1)[0]) / 1_000_000
        except ValueError:
            return 0.0

    @classmethod
    def _get_path_values_metadata(cls, path_values, incl_path=False):
        return [
            {
                **({"_path": vpath} if incl_path else {}),
                "_version": vi + 1,
                "_written_at": cls._parse_written_at(vpath),
            }
            for vi, vpath in enumerate(path_values)
        ]

    def decode_value_from_filepath(self, filepath):
        return self.decode_value(self._get_from_filepath(filepath))

    def paths_items(self, all_results=None, with_metadata=None):
        for root, files in self._fs_walk(self.path):
            if not self.key_filename in set(files):
                continue
            key_path = self._fs_join(root, self.key_filename)
            value_paths = sorted(
                self._fs_join(root, file)
                for file in files
                if file != self.key_filename
                and file[0] != "."
            )
            if not value_paths:
                continue
            if with_metadata:
                value_paths = self._get_path_values_metadata(
                    value_paths, incl_path=True
                )

            if self._all_results(all_results):
                yield (key_path, value_paths)
            else:
                yield (key_path, value_paths[-1:])

    @log.debug
    def _keys(self):
        for path in self.paths_keys():
            yield self._get_from_filepath(path)

    @log.debug
    def _values(self, all_results=None):
        for paths in self.paths_values(all_results=all_results):
            for path in paths:
                yield self._get_from_filepath(path)

    @log.debug
    def _items(self, all_results=None):
        for path_key, path_values in self.paths_items(all_results=all_results):
            encoded_key = self._get_from_filepath(path_key)
            for path_value in path_values:
                encoded_value = self._get_from_filepath(path_value)
                yield (encoded_key, encoded_value)

    @log.debug
    def _del(self, encoded_key: bytes) -> None:
        path = self._get_path(encoded_key)
        if not self._fs_exists(path):
            raise KeyError(encoded_key)
        self._fs_rmtree(path)
