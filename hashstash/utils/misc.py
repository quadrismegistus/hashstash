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


def progress_bar(iterr, progress=True,**kwargs):
    global current_depth

    if not progress:
        yield from iterr
    else:
        try:
            from tqdm import tqdm
            current_depth+=1
            
            class ColoredTqdm(tqdm):
                def __init__(self, *args, desc=None, **kwargs):
                    self.green = '\033[32m'
                    self.reset = '\033[32m'
                    desc = f"{self.green}{log_prefix_str(desc,reset=True)}{self.reset}"
                    super().__init__(*args, desc=desc, **kwargs)
                    

            yield from ColoredTqdm(iterr, position=0, leave=False, **kwargs)
        except ImportError:
            yield from progress_bar(iterr, progress=False, **kwargs)
        finally:
            current_depth-=1

def get_fn_ext(fn):
    # without period
    return fn.split('.')[-1]