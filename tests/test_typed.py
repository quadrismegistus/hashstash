"""Tests for TypedStash."""
import logging
import pytest

from hashstash import HashStash, TypedStash, logger as hashstash_logger


@pytest.fixture(autouse=True)
def _ensure_warnings_logged():
    saved = hashstash_logger.level
    hashstash_logger.setLevel(logging.WARNING)
    try:
        yield
    finally:
        hashstash_logger.setLevel(saved)


@pytest.fixture
def stash(tmp_path):
    s = HashStash(engine="jsonl", root_dir=str(tmp_path), dbname="typed_test")
    s.clear()
    yield s


# --- Stand-ins for pydantic/dataclass users ---

class UserModel:
    def __init__(self, name, age):
        self.name = name
        self.age = age

    def __eq__(self, other):
        return isinstance(other, UserModel) and self.name == other.name and self.age == other.age

    def to_raw(self):
        return {"name": self.name, "age": self.age}

    @classmethod
    def from_raw(cls, raw):
        return cls(raw["name"], raw["age"])


# --- Basic wrapping ---

class TestBasicWrapping:
    def test_loader_applied_on_read(self, stash):
        stash["u1"] = {"name": "alice", "age": 30}
        typed = TypedStash(stash, loader=UserModel.from_raw)
        u = typed["u1"]
        assert isinstance(u, UserModel)
        assert u.name == "alice"
        assert u.age == 30

    def test_dumper_applied_on_write(self, stash):
        typed = TypedStash(stash, loader=UserModel.from_raw, dumper=lambda u: u.to_raw())
        typed["u1"] = UserModel("bob", 42)
        # Underlying stash now stores the raw dict, not the UserModel instance
        assert stash["u1"] == {"name": "bob", "age": 42}
        # Reading back through typed view reconstructs the model
        assert typed["u1"] == UserModel("bob", 42)

    def test_no_dumper_passes_through(self, stash):
        typed = TypedStash(stash, loader=lambda raw: raw)
        typed["k"] = {"plain": "dict"}
        assert stash["k"] == {"plain": "dict"}

    def test_delegated_basics(self, stash):
        typed = TypedStash(stash, loader=UserModel.from_raw)
        stash["u1"] = {"name": "alice", "age": 30}
        stash["u2"] = {"name": "bob", "age": 42}
        assert len(typed) == 2
        assert "u1" in typed
        assert "missing" not in typed
        assert set(typed) == {"u1", "u2"}
        del typed["u1"]
        assert "u1" not in typed

    def test_multiple_views_over_same_stash(self, stash):
        # llm-claude's migration use case: different loaders over same underlying stash
        stash["u1"] = {"name": "alice", "age": 30}
        view_a = TypedStash(stash, loader=UserModel.from_raw)
        view_b = TypedStash(stash, loader=lambda raw: raw["name"])
        assert view_a["u1"] == UserModel("alice", 30)
        assert view_b["u1"] == "alice"


# --- on_error policies ---

def _broken_loader(raw):
    raise ValueError(f"cannot load: {raw}")


class TestGetOnError:
    def test_get_default_raises(self, stash):
        stash["bad"] = {"whatever": 1}
        typed = TypedStash(stash, loader=_broken_loader)
        with pytest.raises(ValueError):
            typed.get("bad")

    def test_get_skip_returns_default(self, stash, caplog):
        stash["bad"] = {"whatever": 1}
        typed = TypedStash(stash, loader=_broken_loader)
        with caplog.at_level(logging.WARNING):
            result = typed.get("bad", default="fallback", on_error="skip")
        assert result == "fallback"
        assert any("TypedStash" in r.message and "bad" in r.message for r in caplog.records)

    def test_get_return_returns_exception(self, stash):
        stash["bad"] = {"whatever": 1}
        typed = TypedStash(stash, loader=_broken_loader)
        result = typed.get("bad", on_error="return")
        assert isinstance(result, ValueError)

    def test_get_missing_key_returns_default(self, stash):
        typed = TypedStash(stash, loader=_broken_loader)
        # Missing keys go through default path, don't trigger loader
        assert typed.get("nope", default=42) == 42

    def test_get_invalid_on_error_raises(self, stash):
        stash["bad"] = {"whatever": 1}
        typed = TypedStash(stash, loader=_broken_loader)
        with pytest.raises(ValueError, match="invalid on_error"):
            typed.get("bad", on_error="explode")


class TestItemsOnError:
    def test_items_default_skips(self, stash, caplog):
        stash["ok1"] = {"name": "a", "age": 1}
        stash["bad"] = {"malformed": True}
        stash["ok2"] = {"name": "b", "age": 2}

        def strict_loader(raw):
            if "malformed" in raw:
                raise ValueError("bad row")
            return UserModel.from_raw(raw)

        typed = TypedStash(stash, loader=strict_loader)
        with caplog.at_level(logging.WARNING):
            result = list(typed.items())
        keys = [k for k, _ in result]
        assert set(keys) == {"ok1", "ok2"}  # bad skipped
        assert any("bad" in r.message for r in caplog.records)

    def test_items_raise(self, stash):
        stash["ok"] = {"name": "a", "age": 1}
        stash["bad"] = {"malformed": True}

        def strict_loader(raw):
            if "malformed" in raw:
                raise ValueError("bad row")
            return UserModel.from_raw(raw)

        typed = TypedStash(stash, loader=strict_loader)
        with pytest.raises(ValueError):
            list(typed.items(on_error="raise"))

    def test_items_return(self, stash):
        stash["ok"] = {"name": "a", "age": 1}
        stash["bad"] = {"malformed": True}

        def strict_loader(raw):
            if "malformed" in raw:
                raise ValueError("bad row")
            return UserModel.from_raw(raw)

        typed = TypedStash(stash, loader=strict_loader)
        out = dict(typed.items(on_error="return"))
        assert isinstance(out["bad"], ValueError)
        assert out["ok"] == UserModel("a", 1)


# --- filter ---

class TestFilter:
    def test_filter_lazy_loading(self, stash):
        stash["match_1"] = {"name": "a", "age": 1}
        stash["skip_1"] = {"invalid_payload": True}  # would fail loader
        stash["match_2"] = {"name": "b", "age": 2}

        loaded_keys = []
        def tracking_loader(raw):
            loaded_keys.append(raw)
            return UserModel.from_raw(raw)

        typed = TypedStash(stash, loader=tracking_loader)
        result = dict(typed.filter(lambda k: k.startswith("match_")))
        assert set(result.keys()) == {"match_1", "match_2"}
        # Loader was NOT called on skip_1 — predicate filtered it out before load
        assert len(loaded_keys) == 2

    def test_filter_handles_loader_error(self, stash, caplog):
        stash["a"] = {"name": "alice", "age": 1}
        stash["b"] = {"malformed": True}

        def strict_loader(raw):
            if "malformed" in raw:
                raise ValueError("bad")
            return UserModel.from_raw(raw)

        typed = TypedStash(stash, loader=strict_loader)
        with caplog.at_level(logging.WARNING):
            result = dict(typed.filter(lambda k: True))
        assert set(result.keys()) == {"a"}


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
