from . import *

class ShelveHashStash(BaseHashStash):
    engine = 'shelve'
    string_keys = True

    def get_db(self, writeback=True):
        import shelve
        return shelve.open(self.path, writeback=writeback)
    