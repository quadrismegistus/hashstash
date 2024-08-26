from . import *

# Create or load a database


class PickleDBHashStash(BaseHashStash):
    engine = "pickledb"
    string_keys = True
    string_values = True

    def get_db(self):
        import pickledb
        os.makedirs(self.path_dirname, exist_ok=True)
        return DictContext(pickledb.load(self.path, True))
