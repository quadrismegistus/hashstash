from . import *


class PairtreeHashStash(BaseHashStash):
    engine = "pairtree"
    filename_is_dir = True
    key_filename = ".key"
    valtype_filename = ".valtype"
    metadata_cols = ["_version", "_timestamp"]
    needs_lock = False

    def connect(self):
        pass

    @log.debug
    def _get_path(self, encoded_key):
        hashed_key = self.hash(encoded_key)
        dir1, dir2, dir3, fname = (
            hashed_key[:2],
            hashed_key[2:4],
            hashed_key[4:6],
            hashed_key[6:],
        )
        return os.path.join(self.path, dir1, dir2, dir3, fname)

    @log.debug
    def get_path(self, unencoded_key):
        return self._get_path(self.encode_key(unencoded_key))

    @log.debug
    def _get_path_key(self, encoded_key):
        return os.path.join(self._get_path(encoded_key), self.key_filename)

    @log.debug
    def _get_path_valtype(self, encoded_key):
        return os.path.join(self._get_path(encoded_key), self.valtype_filename)

    @log.debug
    def get_path_key(self, unencoded_key):
        return self._get_path_key(self.encode_key(unencoded_key))

    @log.debug
    def _get_path_values(self, encoded_key, all_results=None, with_metadata=None):
        path = self._get_path(encoded_key)
        if not os.path.exists(path): return []
        try:
            paths = [
                os.path.join(path, f)
                for f in os.listdir(path)
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
        return os.path.join(
            self._get_path(encoded_key), str(int(time.time() * 1000000))
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
        **kwargs,
    ) -> Any:
        paths_ld = self.get_path_values(
            unencoded_key,
            all_results=self._all_results(all_results),
            with_metadata=True,
        )
        out = []
        for path_d in paths_ld:
            path = path_d.pop("_path")
            encoded_value = self._get_from_filepath(path)
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
        if not os.path.exists(filepath):
            return None

        with open(filepath, "rb") as f:
            return f.read()

    def _set_to_filepath(self, filepath, encoded_data):
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        with open(filepath, "wb") as f:
            f.write(encoded_data)

    @log.debug
    def _set(self, encoded_key: str, encoded_value: Any) -> None:
        self._set_key(encoded_key)
        filepath_value = self._get_path_new_value(encoded_key)
        self._set_to_filepath(filepath_value, encoded_value)
        if not self.append_mode:
            self._prune_dir(filepath_value)


    def _prune_dir(self, filepath_value):
        dir_path = os.path.dirname(filepath_value)
        files = os.listdir(dir_path)
        for file in files:
            if file and file[0]!='.':
                file_path = os.path.join(dir_path, file)
                if file_path != filepath_value and os.path.isfile(file_path):
                    os.remove(file_path)


        

    @log.debug
    def _set_key(self, encoded_key):
        filepath_key = self._get_path_key(encoded_key)
        if not os.path.exists(filepath_key):
            self._set_to_filepath(filepath_key, encoded_key)

    @log.debug
    def _has(self, encoded_key: bytes) -> bool:
        return bool(self._get_path_values(encoded_key))

    @log.debug
    def __len__(self):
        return sum(1 for _ in self.paths())

    @log.debug
    def paths(self):
        for root, _, files in os.walk(self.path):
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
    def _get_path_values_metadata(path_values, incl_path=False):
        return [
            {
                **({"_path": vpath} if incl_path else {}),
                "_version": vi + 1,
                "_timestamp": float(os.path.splitext(os.path.basename(vpath))[0]) / 1_000_000,
            }
            for vi, vpath in enumerate(path_values)
        ]

    def decode_value_from_filepath(self, filepath):
        return self.decode_value(self._get_from_filepath(filepath))

    def paths_items(self, all_results=None, with_metadata=None):
        for root, _, files in os.walk(self.path):
            if not self.key_filename in set(files):
                continue
            key_path = os.path.join(root, self.key_filename)
            value_paths = [
                os.path.join(root, file)
                for file in files
                if file != self.key_filename
                and file[0] != "."
            ]
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
    # def _values(self, all_results=None):
    #     for paths in self.paths_values(all_results=all_results):
    #         for path in paths:
    #             yield self._get_from_filepath(path)

    @log.debug
    # def values(self, all_results=None, **kwargs):
    #     yield from (
    #         self.decode_value(value) for value in self._values(all_results=all_results)
    #     )

    @log.debug
    # def _items(self, all_results=None):
    #     for path_key, path_values in self.paths_items(all_results=all_results):
    #         encoded_key = self._get_from_filepath(path_key)
    #         for path_value in path_values:
    #             encoded_value = self._get_from_filepath(path_value)
    #             yield (encoded_key, encoded_value)

    @log.debug
    # def items(self, all_results=None, with_metadata=False, **kwargs):
    #     for path_key, path_values in self.paths_items(
    #         all_results=all_results, with_metadata=True
    #     ):
    #         encoded_key = self._get_from_filepath(path_key)
    #         decoded_key = self.decode_key(encoded_key)

    #         for path_value_d in path_values:
    #             path_value = path_value_d.pop("_path")
    #             encoded_value = self._get_from_filepath(path_value)
    #             decoded_value = self.decode_value(encoded_value)
    #             if not with_metadata:
    #                 yield (decoded_key, decoded_value)
    #             else:
    #                 key = decoded_key
    #                 value = decoded_value
    #                 key_d = {"_key": key} if not isinstance(key, dict) else {**key}
    #                 value_d = {
    #                     "_value": value
    #                 }  # if not isinstance(value,dict) else {**value}
    #                 meta_d = path_value_d
    #                 yield {**key_d, **meta_d, **value_d}

    def __delitem__(self, unencoded_key: str) -> None:
        path = self.get_path(unencoded_key)
        if not os.path.exists(path):
            raise KeyError(unencoded_key)
        shutil.rmtree(path, ignore_errors=True)
