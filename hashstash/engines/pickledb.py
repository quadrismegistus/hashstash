from . import *

# Create or load a database


class PickleDBHashStash(BaseHashStash):
    engine = "pickledb"
    string_keys = True
    string_values = True

    def get_db(self):
        import pickledb
        return DictContext(pickledb.load(self.path, True))

    def clear(self) -> None:
        if os.path.exists(self.path):
            os.remove(self.path)


