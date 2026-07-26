__version__ = '1.1.0'
from .constants import *
from .config import *
from .hashstash import *
from .utils import *
from .serializers import *
from .engines import *
from .typed import TypedStash
from .graph import GraphStash

# Curated public surface for tooling / tab-completion. The internal modules
# chain `import *` (a circular-import workaround), which leaks stdlib names
# (os, sys, json, re, ...) into this namespace. __dir__ hides them from
# dir(hashstash); we deliberately do NOT set __all__, so `from hashstash import
# *` is unchanged and nothing that relied on a star-imported name breaks.
_PUBLIC_API = [
    "HashStash", "Stash", "Config",
    "serialize", "deserialize", "serialize_custom", "deserialize_custom",
    "encode", "decode", "encode_hash", "stuff", "unstuff",
    "stashed_result", "stash_mapped", "pmap", "StashMap",
    "GraphStash", "TypedStash",
    "SafeDeserializationError", "safe_deserialization",
    "log", "logger",
]


def __dir__():
    return sorted(n for n in _PUBLIC_API if n in globals())