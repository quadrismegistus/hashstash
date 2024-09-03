from . import *

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


def generate_complex_data(size: int) -> Dict[str, Any]:
    return {
        "nested_structure": generate_data(size=size, data_type='dict'),
        "dataframe": generate_data(size=size, data_type='pandas_df'),
    }



@log.debug
def generate_data(
    size: Union[int, List[int]],
    data_type: str = None,
    depth: int = 1,
) -> Any:
    target_size = size
    if isinstance(target_size, list):
        target_size = target_size[0]  # Take the first element if it's a list

    if depth <= 0:
        return generate_primitive()

    choice = data_type

    if choice == "primitive":
        return generate_primitive()
    elif choice == "list":
        return generate_list(min(target_size // 10, 1000))
    elif choice == "dict":
        return generate_dict(target_size)
    elif choice == "pandas_df" or choice == "meta_df":
        df = generate_data_dataframe(target_size)
        return MetaDataFrame(df) if choice == "meta_df" else (df.df if isinstance(df, MetaDataFrame) else df)
    else:
        assert False, f"Invalid data type: {choice}"


@stashed_dataframe(append_mode=False)
def generate_data_dataframe(size: int, size_factor=1) -> 'DataFrame':
    try:
        import pandas as pd
        import numpy as np
    except ImportError:
        raise ImportError("Pandas and NumPy are required for this function.")

    def get_size(df):
        return bytesize(df)

    size = int(size*size_factor)
    # Estimate initial number of rows and columns
    estimated_rows = max(1, size // 100)  # Assume average of 100 bytes per row
    num_columns = min(10, size // 1000)  # Limit initial columns, but ensure at least 1

    column_types = [
        ('int', np.random.randint, (-1000000, 1000000, estimated_rows)),
        ('float', np.random.random, (estimated_rows,)),
        ('str', lambda n: np.array([''.join(np.random.choice(list(string.ascii_letters + string.digits), size=20)) for _ in range(n)]), (estimated_rows,)),
        ('bool', np.random.choice, ([True, False], estimated_rows)),
    ]

    # Generate data for all columns at once
    data = {'id': np.arange(estimated_rows)}
    for i in range(1, num_columns):
        col_type, col_generator, args = random.choice(column_types)
        data[f'{col_type}_{i}'] = col_generator(*args)

    df = pd.DataFrame(data)
    current_size = get_size(df)

    # Add more data if needed
    while current_size < size:
        rows_to_add = max(1, (size - current_size) // 100)
        new_data = {'id': np.arange(len(df), len(df) + rows_to_add)}
        for col in df.columns[1:]:
            col_type = col.split('_')[0]
            generator, args = next((gen, args) for t, gen, args in column_types if t == col_type)
            new_args = list(args[:-1]) + [rows_to_add] if isinstance(args, tuple) else args
            new_data[col] = generator(*new_args)
        
        df = pd.concat([df, pd.DataFrame(new_data)], ignore_index=True)
        current_size = get_size(df)

    return df

def generate_list(max_length: int = 10) -> List[Any]:
    return [generate_primitive() for _ in range(random.randint(0, max_length))]


def generate_dict_approx(max_keys: int = 10) -> Dict[str, Any]:
    return {
        f"key_{uuid.uuid4()}": generate_list(max_length=max_keys) for i in range(random.randint(0, max_keys))
    }

@stashed_result
def generate_dict(target_size: int, step_keys=10) -> Dict[str, Any]:
    data = {}
    current_size = bytesize(data)
    while current_size < target_size:
        data = {**data, **generate_dict_approx(max_keys=step_keys)}
        current_size = bytesize(data)
    return data


def generate_dataset(iterations, size, size_factor=.666, **kwargs):
    obj = generate_data_dataframe(size, size_factor=size_factor)
    obj_s = serialize(obj)
    return [obj_s for _ in range(iterations)]

def get_dataset(iterations, size):
    l = generate_dataset(iterations, size)
    l = [[x] if not isinstance(x,list) else x for x in l]
    return [xx for x in l for xx in x]