from .base import *
import pickledb

# Create or load a database


class PickleDBHashStash(BaseHashStash):
    engine = "sqlite"
    filename = "db.pickledb"
    string_keys = True
    string_values = True

    def get_db(self):
        return DictContext(pickledb.load(self.path, True))

    def clear(self) -> None:
        if os.path.exists(self.path):
            os.remove(self.path)


