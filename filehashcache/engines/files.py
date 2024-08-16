from .base import *

class FileHashCache(BaseHashCache):
    engine = 'file'
    filename = 'db_files'
    
    def _encode_filepath(self, encoded_key):
        hashed_key = self.hash(encoded_key)
        dir, fname = hashed_key[:2], hashed_key[2:]
        return os.path.join(self.path, dir, fname)
    
    def __setitem__(self, key: str, value: Any) -> None:
        encoded_key = self.encode_key(key)
        filepath = self._encode_filepath(encoded_key)
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        
        encoded_value = self.encode(value)
        
        with open(filepath, 'wb') as f:
            f.write(encoded_key + b'\n' + encoded_value)

    def __getitem__(self, key: str) -> Any:
        encoded_key = self.encode_key(key)
        filepath = self._encode_filepath(encoded_key)
        if not os.path.exists(filepath):
            raise KeyError(key)
        
        with open(filepath, 'rb') as f:
            f.readline() # skip key
            encoded_value = f.read()
        
        return self.decode(encoded_value)

    def __contains__(self, key: str) -> bool:
        encoded_key = self.encode_key(key)
        filepath = self._encode_filepath(encoded_key)
        return os.path.exists(filepath)

    def clear(self) -> None:
        import shutil
        shutil.rmtree(self.path, ignore_errors=True)
        os.makedirs(self.path, exist_ok=True)

    def __len__(self):
        return sum(1 for _ in self._paths())

    def _paths(self):
        for root, _, files in os.walk(self.path):
            for file in files:
                if len(file) == 30 and file[0]!='.':
                    yield os.path.join(root, file)

    def _keys(self):
        for path in self._paths():
            with open(path, 'rb') as f:
                yield f.readline().strip()
    
    def _values(self):
        for path in self._paths():
            with open(path, 'rb') as f:
                f.readline()
                yield f.readline().strip()

    def _items(self):
        for path in self._paths():
            with open(path, 'rb') as f:
                key = f.readline().strip()
                val = f.readline().strip()
                yield (key,val)
    

    def __delitem__(self, key: str) -> None:
        filepath = self._encode_filepath(key)
        if not os.path.exists(filepath):
            raise KeyError(key)
        os.remove(filepath)
        
        # Remove empty parent directories
        dir_path = os.path.dirname(filepath)
        while dir_path != self.path:
            if not os.listdir(dir_path):
                os.rmdir(dir_path)
                dir_path = os.path.dirname(dir_path)
            else:
                break