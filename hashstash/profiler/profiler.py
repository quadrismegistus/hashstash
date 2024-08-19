from ..utils import *
from ..serialize import *
import pandas as pd
import numpy as np
from tqdm import tqdm


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


def generate_data(
    target_size: int,
    data_types=[
        "primitive",
        "list",
        "dict",
        "numpy",
        "pandas_df",
        "pandas_series",
    ],
) -> Dict[str, Any]:
    data = {}
    current_size = 0

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
        else:  # pandas_series
            key = f"pandas_series_{len(data)}"
            value = generate_pandas_series(max_length=min(remaining_size // 10, 10000))

        data[key] = value
        current_size = len(serialize(data))

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
