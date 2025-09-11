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

    @log.debug
    def __init__(self, *args, compress=RAW_NO_COMPRESS, b64=False, **kwargs):
        super().__init__(*args, compress=compress, b64=b64, **kwargs)
        self.key_prefix = f'"{self.key_name}": "'
        self.value_prefix = f'", "{self.value_name}": "'

    @log.debug
    def get_all(
        self,
        unencoded_key: Any = None,
        default: Any = None,
        with_metadata: bool = None,
        all_results: bool = True,
        **kwargs,
    ) -> Any:
        encoded_key = self.encode_key(unencoded_key)
        print(f"Unencoded key: {unencoded_key}")
        print(f"Encoded key: {encoded_key}")

        if not os.path.exists(self.path):
            return default

        encoded_values = []
        with open(self.path, "r") as f:
            for line in f:
                line=line.strip()
                if self.key_prefix in line:
                    print(f"Line: {line}")
                    key = line[line.index(self.key_prefix) + len(self.key_prefix):line.index(self.value_prefix)]
                    print(f"Key: {key}")
                    if key == encoded_key:
                        encoded_value = line[line.index(self.value_prefix) + len(self.value_prefix):-2]
                        encoded_values.append(encoded_value)
    
        print(f"Encoded values: {encoded_values}")
        decoded_values = [self.decode_value(encoded_value) for encoded_value in encoded_values]

        # if with_metadata:
        #     values = [
        #         {"_version": vi + 1, "_value": value} for vi, value in enumerate(values)
        #     ]
        if not self._all_results(all_results):
            decoded_values = decoded_values[-1:]
        
        return decoded_values


    def _append_line(self, key_str: str, value_str: str) -> None:
        os.makedirs(self.path_dirname, exist_ok=True)
        line = "{" + f'"{self.key_name}": "{key_str}", "{self.value_name}": "{value_str}"' + "}\n"
        with open(self.path, "a", encoding="utf-8") as fh:
            fh.write(line)

    def encode_key(self, unencoded_key: Any) -> Union[str, bytes]:
        return json.dumps(super().encode_key(unencoded_key))

    def encode_value(self, unencoded_value: Any) -> Union[str, bytes]:
        return json.dumps(super().encode_value(unencoded_value))

    def decode_key(self, encoded_key: Any, as_string=False) -> Union[str, bytes]:
        return super().decode_key(json.loads(encoded_key), as_string=as_string)

    def decode_value(self, encoded_value: Any, as_string=False) -> Union[str, bytes]:
        return super().decode_value(json.loads(encoded_value), as_string=as_string)

    def set(self, unencoded_key: Any, unencoded_value: Any, append=None) -> None:
        encoded_key = self.encode_key(unencoded_key)
        encoded_value = self.encode_value(unencoded_value)
        # self._index[encoded_key].append(encoded_value)
        self._append_line(encoded_key, encoded_value)

    