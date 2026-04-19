from collections.abc import MutableMapping
from .utils.logs import log


_MISSING = object()


class TypedStash(MutableMapping):
    """Thin typed view over a HashStash. Applies ``loader`` on read and optional ``dumper`` on write.

    The underlying stash is untouched — multiple TypedStash views can wrap the same stash with
    different loaders (useful during schema migrations: read-old, write-new).

    Parameters
    ----------
    stash : BaseHashStash
        The underlying stash to wrap.
    loader : Callable[[Any], T]
        Called on each raw value returned by the underlying stash. Typical shapes:
            loader=lambda raw: MyPydanticModel.model_validate(raw)
            loader=lambda raw: MyDataclass(**raw)
    dumper : Optional[Callable[[T], Any]]
        Called on each value before writing. If None (default), values are stored as-is.

    Error policy (``on_error`` kwarg on read methods)
    ------------------------------------------------
    'raise'  — propagate the loader exception (default for single-item ``.get()``)
    'skip'   — log a warning and skip the entry (default for iteration: items/values/filter)
    'return' — yield/return the exception object as the value
    """

    def __init__(self, stash, loader, dumper=None):
        self.stash = stash
        self.loader = loader
        self.dumper = dumper

    def _handle_error(self, key, exc, on_error):
        if on_error == "raise":
            raise exc
        if on_error == "skip":
            log.warning(f"TypedStash: loader failed on key={key!r}: {exc}")
            return _MISSING
        if on_error == "return":
            return exc
        raise ValueError(
            f"invalid on_error={on_error!r}; must be 'raise', 'skip', or 'return'"
        )

    def get(self, key, default=None, on_error="raise"):
        if key not in self.stash:
            return default
        try:
            return self.loader(self.stash[key])
        except Exception as exc:
            result = self._handle_error(key, exc, on_error)
            return default if result is _MISSING else result

    def __getitem__(self, key):
        return self.loader(self.stash[key])

    def __setitem__(self, key, value):
        self.stash[key] = self.dumper(value) if self.dumper is not None else value

    def __delitem__(self, key):
        del self.stash[key]

    def __contains__(self, key):
        return key in self.stash

    def __iter__(self):
        return iter(self.stash)

    def __len__(self):
        return len(self.stash)

    def keys(self):
        return self.stash.keys()

    def items(self, on_error="skip", before=None, after=None):
        for key in self.stash.keys():
            values = self.stash.get_all(
                key, all_results=False, before=before, after=after, default=None
            )
            if not values:
                continue
            try:
                yield key, self.loader(values[-1])
            except Exception as exc:
                result = self._handle_error(key, exc, on_error)
                if result is not _MISSING:
                    yield key, result

    def values(self, on_error="skip", before=None, after=None):
        for _, v in self.items(on_error=on_error, before=before, after=after):
            yield v

    def filter(self, predicate, on_error="skip", before=None, after=None):
        """Yield (key, typed_value) pairs where predicate(key) is truthy. Values for non-matching
        keys are never loaded; optional before/after further filter by write time."""
        for key in self.stash.keys():
            if not predicate(key):
                continue
            values = self.stash.get_all(
                key, all_results=False, before=before, after=after, default=None
            )
            if not values:
                continue
            try:
                yield key, self.loader(values[-1])
            except Exception as exc:
                result = self._handle_error(key, exc, on_error)
                if result is not _MISSING:
                    yield key, result
