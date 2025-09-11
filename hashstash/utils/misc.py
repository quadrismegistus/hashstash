from . import *

def iter_jsonl(path):
    if os.path.exists(path):
        try:
            import orjsonl
            yield from orjsonl.stream(path)
        except ImportError:
            with open(path, "r") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        yield json.loads(line)
                    except Exception:
                        continue

def is_jsonable(obj):
    return isinstance(obj, (dict, list, str, int, float, bool, type(None)))


def prune_none_values(data, badkeys=None):
    if isinstance(data, dict):
        return {
            k: prune_none_values(v)
            for k, v in data.items()
            if v is not None and (not badkeys or k not in badkeys)
        }
    elif isinstance(data, list):
        return [prune_none_values(item) for item in data if item is not None]
    else:
        return data


@log.debug
def is_dir(path):
    fn, ext = os.path.splitext(path)
    return not bool(ext)


@log.debug
def ensure_dir(path):
    if not is_dir(path):
        path = os.path.dirname(path)
    return os.makedirs(path, exist_ok=True)


def reset_index_misc(df, _index=False):
    import pandas as pd

    index = [x for x in df.index.names if x is not None]
    df = (
        df.reset_index()
        if index
        else (df if not _index else df.rename_axis("_index").reset_index())
    )
    return df, (index if index or not _index else ["_index"])


def get_fn_ext(fn):
    # without period
    return fn.split(".")[-1]


def is_generator(obj):
    import inspect
    from collections.abc import Generator

    return (inspect.isgenerator(obj) or isinstance(obj, Generator)) or type(obj) is range


class ReusableGenerator:
    def __init__(self, func, *args, **kwargs):
        self.func = func
        self.args = args
        self.kwargs = kwargs

    def __iter__(self):
        return self.func(*self.args, **self.kwargs)


def rmtreefn(dir_path):
    if not os.path.exists(dir_path):
        return
    try:
        if os.path.isdir(dir_path):
            shutil.rmtree(dir_path, ignore_errors=True)
            # log.info(f'Deleted temporary directory: {dir_path}')
        elif os.path.isfile(dir_path):
            os.remove(dir_path)
            # log.info(f'Deleted temporary file: {dir_path}')
        else:
            log.debug(f"Temporary path does not exist: {dir_path}")
    except Exception as e:
        log.debug(f"Failed to delete temporary path {dir_path}: {e}")


def get_encoding_str(compress, b64):
    return "+".join(
        filter(
            None, [compress if compress else RAW_NO_COMPRESS, "b64" if b64 else None]
        )
    )


def fast_concat(*dfs):
    import pandas as pd

    # # Reset index for all DataFrames before concatenation
    # reset_dfs = [df.reset_index(drop=True) for df in dfs]

    # Concatenate the reset DataFrames
    result = pd.concat(list(dfs), axis=0, ignore_index=True, join="outer")

    return result


def slow_concat(*dfs):
    import pandas as pd

    return pd.DataFrame([d for df in dfs for d in df.to_dict(orient="records")])


class OrderedSet:
    """A set that preserves insertion order."""
    
    def __init__(self, iterable=None):
        self._dict = {}
        if iterable:
            for item in iterable:
                self.add(item)
    
    def add(self, item):
        """Add an item to the set."""
        self._dict[item] = None
    
    def discard(self, item):
        """Remove an item from the set if it exists."""
        self._dict.pop(item, None)
    
    def remove(self, item):
        """Remove an item from the set. Raises KeyError if not found."""
        del self._dict[item]
    
    def __contains__(self, item):
        return item in self._dict
    
    def __iter__(self):
        return iter(self._dict)
    
    def __len__(self):
        return len(self._dict)
    
    def __bool__(self):
        return bool(self._dict)
    
    def __repr__(self):
        return f"OrderedSet({list(self._dict.keys())})"
    
    def __eq__(self, other):
        if isinstance(other, OrderedSet):
            return list(self._dict.keys()) == list(other._dict.keys())
        elif isinstance(other, set):
            return set(self._dict.keys()) == other
        return False
    
    def clear(self):
        """Remove all items from the set."""
        self._dict.clear()
    
    def copy(self):
        """Return a shallow copy of the set."""
        return OrderedSet(self._dict.keys())
    
    def union(self, *others):
        """Return a new OrderedSet with items from this set and all others."""
        result = self.copy()
        for other in others:
            for item in other:
                result.add(item)
        return result
    
    def intersection(self, *others):
        """Return a new OrderedSet with items common to this set and all others."""
        result = OrderedSet()
        for item in self:
            if all(item in other for other in others):
                result.add(item)
        return result
    
    def difference(self, *others):
        """Return a new OrderedSet with items in this set but not in others."""
        result = self.copy()
        for other in others:
            for item in other:
                result.discard(item)
        return result




def is_nan(x):
    import numpy as np

    try:
        return np.isnan(x)
    except Exception:
        return False


def _flatten_dict(d, parent_key='', sep='.'):
    items = []
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(_flatten_dict(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)

def _flatten_ld(item, ind=None):
    if ind is None:
        ind = {}

    if isinstance(item, dict):
        flattened = _flatten_dict(item)
        return [{**ind, **flattened}]
    elif is_dataframe(item):
        return [
            {**ind, **_flatten_dict(row.to_dict())}
            for _, row in reset_index_misc(item, _index=False)[0].iterrows()
        ]
    elif isinstance(item, list):
        return [row for subitem in item for row in _flatten_ld(subitem, ind)]
    else:
        return [{**ind, "_value": item}]


def flatten_ld(item, ind={}):
    if isinstance(item, (dict, list)) or is_dataframe(item):
        return [d for d in _flatten_ld(item, ind) if isinstance(d, dict) and d]
    else:
        return [{**ind, "_value": item}]


def is_meta_col(col):
    return col and col[0] == "_" and col not in {"_key", "_value"}


def filter_ld(ld, no_nan=False, no_meta=False):
    ld = [
        {
            k: v
            for k, v in d.items()
            if (not no_nan or not is_nan(v)) and (not no_meta or not is_meta_col(k))
        }
        for d in ld
    ]
    ld = [
        d
        for d in ld
        if isinstance(d, dict) and len(d) and (not "_key" in ld or len(d) > 1)
    ]
    return ld


def filter_df(
    df,
    with_metadata=False,
    index=True,
    index_cols=None,
    key_col="_key",
    value_col="_value",
):
    if not with_metadata:
        df = df[[c for c in df.columns if c[0] != "_" or c in {key_col, value_col}]]
    if index:
        if not index_cols:
            index_cols = [x for x in df if x[0] == "_" and x != value_col]
        if index_cols:
            df = df.set_index(index_cols if index_cols else prefix_cols)
    prefix_cols = [x for x in df if x[0] == "_"]
    non_prefix_cols = [x for x in df if x[0] != "_"]
    df = df[prefix_cols + non_prefix_cols]
    return df


def separate_index(df, _index=None):
    index = [x for x in df.index.names if x is not None]
    return (df if not index else df.reset_index()), index


def is_dataframe(df):
    return get_obj_addr(df).endswith("DataFrame")


def flatten_args_kwargs(args_kwargs, prefix_args="_arg", prefix_kwargs="_"):
    if (
        not isinstance(args_kwargs, dict)
        or "args" not in args_kwargs
        or "kwargs" not in args_kwargs
    ):
        return {"_key": args_kwargs}

    result = {}

    # Handle args
    for i, arg in enumerate(args_kwargs.get("args", []), start=1):
        if is_jsonable(arg):
            result[f"{prefix_args}{i}"] = arg

    # Handle kwargs
    result.update(
        {f"{prefix_kwargs}{k}": v for k, v in args_kwargs.get("kwargs", {}).items()}
    )
    return result


def progress_bar(iterr=None, total=None, progress=True, leave=False, **kwargs):
    global current_depth

    class DummyProgressBar:
        def __init__(self, iterable=None, total=None):
            self.iterable = iterable
            self.total = total
            self.n = 0

        def __iter__(self):
            return iter(self.iterable) if self.iterable else iter(range(self.total))

        def update(self, n=1):
            self.n += n

        def close(self):
            pass

    if not progress:
        return DummyProgressBar(iterr, total)
    else:
        try:
            from tqdm import tqdm

            current_depth += 1

            class ColoredTqdm(tqdm):
                def __init__(self, *args, desc=None, **kwargs):
                    self.green = "\033[32m"
                    self.reset = "\033[0m"
                    desc = f"{self.green}{log_prefix_str(desc,reset=True)}{self.reset}"
                    super().__init__(*args, desc=desc, **kwargs)

            if iterr is not None:
                return ColoredTqdm(iterr, leave=leave, **kwargs)
            else:
                return ColoredTqdm(total=total, leave=leave, **kwargs)
        except ImportError:
            return DummyProgressBar(iterr, total)
        finally:
            current_depth -= 1


def is_stash(x):
    from ..engines.base import BaseHashStash

    return isinstance(x, BaseHashStash)


def print_md(*args):
    from IPython.display import Markdown, display
    display(Markdown(' '.join(str(x) for x in args)))


def get_encoding_str(compress,b64):
    return "+".join(
            filter(
                None,
                [
                    compress if compress else "raw",
                    "b64" if b64 else None,
                ],
            )
        )