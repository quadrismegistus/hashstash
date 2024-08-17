from .base import *
import shelve

class ShelveHashDict(BaseHashDict):
    engine = 'shelve'
    filename = 'db.shelve'
    string_keys = True

    def get_db(self, writeback=True):
        return shelve.open(self.path, writeback=writeback)
    