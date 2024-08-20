from ..utils import *
from ..serializers import *
import pandas as pd
import numpy as np
from tqdm import tqdm
import random

@fcache
@log.info
def get_sonnets():
    try:
        import prosodic as p
        sonnets_file = '/Users/ryan/github/prosodic/corpora/corppoetry_en/en.shakespeare.txt'
        sonnets = []
        with open(sonnets_file) as f, p.logmap.quiet():
            for sonnet in f.read().strip().split('\n\n'):
                try:
                    sonnets.append(p.Text(sonnet))
                except Exception:
                    pass
        return sonnets
    except Exception as e:
        logger.warning(f'Prosodic not installed, will not generate prosodic data. Error: {e}')
        return None


def get_random_sonnet():
    """Get a random full sonnet"""
    return random.choice(get_sonnets())
        

def generate_primitive():
    primitives = [
        lambda: random.randint(-1000000, 1000000),
        lambda: random.random(),
        lambda: "".join(
            random.choices(
                string.ascii_letters + string.digits, k=random.randint(1, 100)
            )
        ),
        lambda: random.choice([True, False]),
        lambda: None,
    ]
    return random.choice(primitives)()


def generate_list(depth: int, max_length: int = 10) -> List[Any]:
    return [generate_data(depth - 1) for _ in range(random.randint(0, max_length))]


def generate_dict(depth: int, max_keys: int = 10) -> Dict[str, Any]:
    return {
        f"key_{i}": generate_data(depth - 1) for i in range(random.randint(0, max_keys))
    }


def generate_numpy_array(max_dim: int = 3, max_size: int = 100) -> np.ndarray:
    shape = tuple(
        random.randint(1, max_size) for _ in range(random.randint(1, max_dim))
    )
    return np.random.rand(*shape)


def generate_pandas_dataframe(max_rows: int = 100, max_cols: int = 10) -> pd.DataFrame:
    rows = random.randint(1, max(2, max_rows))
    cols = random.randint(1, max(2, max_cols))
    data = {
        f"col_{i}": [generate_primitive() for _ in range(rows)] for i in range(cols)
    }
    odf=pd.DataFrame(data)
    return odf


def generate_pandas_series(max_length: int = 100) -> pd.Series:
    length = random.randint(1, max(2, max_length))
    data = [generate_primitive() for _ in range(length)]
    return pd.Series(data)

@log.debug
def generate_data(
    target_size: int,
    data_types=[
        "primitive",
        "list",
        "dict",
        "numpy",
        "pandas_df",
        "pandas_series",
        "prosodic_text",
        "prosodic_line",
    ],
) -> Dict[str, Any]:
    data = {}
    current_size = 0
    if not get_sonnets():
        data_types = [x for x in data_types if not x.startswith('prosodic')]

    while current_size < target_size:
        remaining_size = target_size - current_size
        choice = random.choice(data_types)

        if choice == "primitive":
            key = f"primitive_{len(data)}"
            value = generate_primitive()
        elif choice == "list":
            key = f"list_{len(data)}"
            value = generate_list(depth=2, max_length=min(remaining_size // 10, 1000))
        elif choice == "dict":
            key = f"dict_{len(data)}"
            value = generate_dict(depth=2, max_keys=min(remaining_size // 20, 100))
        elif choice == "numpy":
            key = f"numpy_{len(data)}"
            value = generate_numpy_array(max_dim=2, max_size=int(remaining_size**0.5))
        elif choice == "pandas_df":
            key = f"pandas_df_{len(data)}"
            value = generate_pandas_dataframe(
                max_rows=max(1, min(remaining_size // 100, 1000)),
                max_cols=max(1, min(remaining_size // 1000, 50)),
            )
        elif choice=='pandas_series':
            key = f"pandas_series_{len(data)}"
            value = generate_pandas_series(max_length=min(remaining_size // 10, 10000))
        elif choice=='prosodic_text':
            key = f'prosodic_text_{len(data)}'
            value = get_random_sonnet()
        else:
            continue

        data[key] = value
        current_size = len(serialize(data).encode())

    return data


def generate_complex_data(size: int) -> Dict[str, Any]:
    return {
        "nested_structure": generate_data(depth=5),
        "dataframe": generate_pandas_dataframe(
            max_rows=size // 100, max_cols=size // 1000
        ),
        "numpy_array": generate_numpy_array(max_size=size // 100),
        "series": generate_pandas_series(max_length=size // 10),
        "large_list": generate_list(depth=2, max_length=size // 10),
        "large_dict": generate_dict(depth=2, max_keys=size // 10),
    }



@log.debug
def generate_data_simple(
    target_size: int,
    data_types=[
        "primitive",
        "list",
        "dict",
        "numpy",
        "pandas_df",
        "pandas_series",
        "prosodic_text",
        "prosodic_line",
    ],
) -> Dict[str, Any]:
    data = {}
    if not get_sonnets():
        data_types = [x for x in data_types if not x.startswith('prosodic')]

    choice = random.choice(data_types)

    if choice == "primitive":
        return generate_primitive()
    elif choice == "list":
        return generate_list_simple(min(target_size // 10, 1000))
    elif choice == "dict":
        return generate_dict_simple(min(target_size // 20, 100))
    elif choice == "numpy":
        return generate_numpy_array(max_dim=2, max_size=int(target_size**0.5))
    elif choice == "pandas_df":
        return generate_pandas_dataframe(
            max_rows=max(1, min(target_size // 100, 1000)),
            max_cols=max(1, min(target_size // 1000, 50)),
        )
    elif choice=='pandas_series':
        return generate_pandas_series(max_length=min(target_size // 10, 10000))
    elif choice=='prosodic_text':
        return get_random_sonnet()
    else:
        return

def generate_list_simple(max_length: int = 10) -> List[Any]:
    return [generate_primitive() for _ in range(random.randint(0, max_length))]


def generate_dict_simple(max_keys: int = 10) -> Dict[str, Any]:
    return {
        f"key_{i}": generate_list_simple(max_length=max_keys) for i in range(random.randint(0, max_keys))
    }
