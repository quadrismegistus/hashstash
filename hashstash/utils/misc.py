from . import *

def is_jsonable(obj):
    return isinstance(obj, (dict, list, str, int, float, bool, type(None)))


def prune_none_values(data, badkeys=None):
    if isinstance(data, dict):
        return {k: prune_none_values(v) for k, v in data.items() if v is not None and (not badkeys or k not in badkeys)}
    elif isinstance(data, list):
        return [prune_none_values(item) for item in data if item is not None]
    else:
        return data

# @log.debug
def is_dir(path):
    fn, ext = os.path.splitext(path)
    return not bool(ext)

# @log.debug
def ensure_dir(path):
    if not is_dir(path): 
        path=os.path.dirname(path)
    return os.makedirs(path, exist_ok=True)

def reset_index(df):
    index = [x for x in df.index.names if x is not None]
    if index:
        df = df.reset_index()
    return df
    
def is_dataframe(obj):
    addr = get_obj_addr(obj)
    return addr.startswith('pandas.') and addr.endswith('.DataFrame')

def get_fn_ext(fn):
    # without period
    return fn.split('.')[-1]


def reformat_python_source(src):
    lines = src.split('\n')
    if lines:
        leading_spaces = len(lines[0]) - len(lines[0].lstrip())
        return '\n'.join(line[leading_spaces:] for line in lines)
    else:
        return src