# HashStash

HashStash is a versatile caching library for Python that supports multiple storage engines, serializers, and encoding options. It provides a simple dictionary-like interface for caching data with various backend options. HashStash is designed to be easy to use, flexible, and efficient.

[![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/quadrismegistus/hashstash/blob/main/README.ipynb)

## Features

### Convenient usage
- Dictionary-like interface, except absolutely anything can be either a key or value (even unhashable entities like sets or unpicklable entities like lambdas, local functions, etc)

- Multiprocessing support: connection pooling, multiprocessing locks, parallelize operations as much as a given engine allows

- Function decorators like `@stashed_result`, which cache the results of function calls

- Context managers for temporary caches

- Easy dataframe assembly from cached contents

### Multiple storage engines

- File-based
    - "__pairtree__" (no dependencies, no database; just organized folder and file structure; very fast)
    - "__[lmdb](https://pypi.org/project/lmdb/)__" (single file, very efficient, slightly faster than pairtree)
    - "__[diskcache](https://pypi.org/project/diskcache/)__" (similar to pairtree, but slower)
    - "__sqlite__" (using [sqlitedict](https://pypi.org/project/sqlitedict/))

- Server-based
    - "__redis__" (using [redis-py](https://pypi.org/project/redis/))
    - "__mongo__" (using [pymongo](https://pypi.org/project/pymongo/))

- In-memory
    - "__memory__" (shared memory, using [ultradict](https://pypi.org/project/ultradict/))

### Multiple serializers

- Transportable between Python versions
    - "__hashstash__"
        - Custom, no dependencies
        - Can serialize nearly anything, even lambdas or functions defined within functions
        - Serializes pandas dataframes using pyarrow if available
        - Faster than jsonpickle but with larger file sizes
        - Mostly JSON-based, with some binary data
    - "__[jsonpickle](https://pypi.org/project/jsonpickle/)__"
        - Flexible, battle-tested, but slowest

- Not transportable between Python versions
    - "__pickle__"
        - Standard library
        - By far the fastest
        - But dangerous to use when sharing data across projects or Python versions 

### Compression and encoding options
- External compressors (with depedencies):
    - "__[lz4](<https://pypi.org/project/python-lz4/)>)__" (fastest)
    - "__[blosc](https://pypi.org/project/blosc/)__"

- Built-in compressors (no dependencies):
    - "__zlib__"
    - "__gzip__"
    - "__bz2__" (smallest file size, but slowest)

## Installation

HashStash requires no dependencies by default, but you can install optional dependencies to get the best performance.

* Default installation: `pip install hashstash`

* Installation with only the optimal engine (lmdb), compressor (lz4), and dataframe serializer (pandas + pyarrow): `pip install hashstash[best]`

* Full installation with all optional dependencies: `pip install hashstash[all]`

* Installation with specific optional dependencies:

    - For pandas dataframe serialization and support: `pip install hashstash[dataframe]`

    - For file-based engines: `pip install hashstash[filebased]`

    - For server-based engines: `pip install hashstash[servers]`

    - For all engine types: `pip install hashstash[engines]`

    - For specific engines:
        - `pip install hashstash[redis]`
        - `pip install hashstash[mongo]`
        - `pip install hashstash[lmdb]`
        - `pip install hashstash[sqlite]`
        - `pip install hashstash[diskcache]`
        - `pip install hashstash[memory]`

    - For development: `pip install hashstash[dev]`

Note: You can combine multiple optional dependencies, e.g., `pip install hashstash[dataframe,filebased]`

```python
!pip install -qU hashstash[best]
```

## Usage

Here's a quick example of how to use HashStash. 

### Creating a stash

```python
from hashstash import HashStash

# Create a stash instance
stash = HashStash()

# or customize:
stash = HashStash(
    # naming
    root_dir="project_stash",    # root directory of the stash (default: default_stash)
                                 # if not an absolute path, will be ~/.cache/hashstash/[root_dir]
    dbname="sub_stash",          # name of "database" or subfolder (default: main)
    
    # engines
    engine="pairtree",           # or lmdb, sqlite, diskcache, redis, mongo, or memory
    serializer="hashstash",      # or jsonpickle or pickle
    compress='lz4',              # or blosc, bz2, gzip, zlib, or raw
    b64=True,                    # base64 encode keys and values

    # storage options
    append_mode=False,           # store all versions of a key/value pair
    clear=True                   # clear on init
)

# show stash type and path
print(stash)

# show stash config
stash.to_dict()
```

↓

    PairtreeHashStash(~/.cache/hashstash/project_stash/sub_stash/pairtree.hashstash.lz4+b64/data.db)

    {'root_dir': '/Users/ryan/.cache/hashstash/project_stash',
     'dbname': 'sub_stash',
     'engine': 'pairtree',
     'serializer': 'hashstash',
     'compress': 'lz4',
     'b64': True,
     'append_mode': False,
     'is_function_stash': False,
     'is_tmp': False,
     'filename': 'data.db'}

### Stashing objects

Literally anything can be a key or value, including lambdas, local functions, sets, dataframes, dictionaries, etc:

```python
# traditional dictionary keys,,,
stash["bad"] = "cat"                 # string key
stash[("bad","good")] = "cat"        # tuple key

# ...unhashable keys...
stash[{"goodness":"bad"}] = "cat"    # dict key
stash[["bad","good"]] = "cat"        # list key
stash[{"bad","good"}] = "cat"        # set key

# ...func keys...
def func_key(x): pass                
stash[func_key] = "cat"              # function key
lambda_key = lambda x: x
stash[lambda_key] = "cat"           # lambda key

# ...very unhashable keys...
import pandas as pd
df_key = pd.DataFrame(                  
    {"name":["cat"], 
     "goodness":["bad"]}
)
stash[df_key] = "cat"                # dataframe key  

# all should equal "cat":
(
    stash["bad"],
    stash[("bad","good")],
    stash[{"goodness":"bad"}],
    stash[["bad","good"]],
    stash[{"bad","good"}],
    stash[func_key],
    stash[lambda_key],
    stash[df_key]
)
```

↓

    ('cat', 'cat', 'cat', 'cat', 'cat', 'cat', 'cat', 'cat')

### Works like a dictionary

HashStash fully implements the dictionary's `MutableMapping` interface, providing all its methods, including:

```python
# get()
assert stash.get(df_key) == "cat"
assert stash.get('fake_key') == None

# __contains__
assert df_key in stash

# __len__
assert len(stash) == 8   # from earlier

# keys()
from hashstash import *
for i,key in enumerate(stash.keys()): 
    pass

# values()
for value in stash.values():
    assert value == "cat"

# items()
for i, (key, value) in enumerate(stash.items()):
    print(f'Item #{i+1}:\n{key} >>> {value}\n')

```

↓

    Item #1:
    {'good', 'bad'} >>> cat
    
    Item #2:
    {'goodness': 'bad'} >>> cat
    
    Item #3:
    bad >>> cat
    
    Item #4:
      name goodness
    0  cat      bad >>> cat
    
    Item #5:
    ('bad', 'good') >>> cat
    
    Item #6:
    ['bad', 'good'] >>> cat
    
    Item #7:
    <function func_key at 0x110022a70> >>> cat
    
    Item #8:
    <function <lambda> at 0x12546a680> >>> cat
    

Other dictionary functions:

```python
# pop()
assert stash.pop(df_key) == "cat"
assert df_key not in stash

# setdefault()
assert stash.setdefault(df_key, "new_cat_default") == "new_cat_default"
assert stash.get(df_key) == "new_cat_default"

# update()
another_dict = {'new_key_of_badness': 'cat'}
stash.update(another_dict)
assert stash['new_key_of_badness'] == "cat"

# update() with another stash
another_stash = HashStash(engine='memory').clear()
another_stash[[1,2,3]] = "cat"
stash.update(another_stash)
assert stash[[1,2,3]] == "cat"
```

#### Under the hood

You can also iterate the keys and values as actually exist in the data store, i.e. serialized encoded:

- `_keys()`: Return an iterator over the encoded keys

- `_values()`: Return an iterator over the encoded values

- `_items()`: Return an iterator over the encoded key-value pai

These methods are used internally and not necessary to use.

```python
print('\nIterating over .items():')
for orig_key,orig_value in stash.items():
    print(orig_key, 'is the original (un-encoded, un-compressed, un-serialized) key for the original value of', orig_value)
    break

print('\nIterating over ._items():')
for encoded_key,encoded_value in stash._items():
    print(encoded_key, 'is the serialized, compressed, and encoded key for', encoded_value)
    decoded_key = stash.decode_key(encoded_key)
    decoded_value = stash.decode_value(encoded_value)
    print(decoded_key, 'is the decoded, uncompressed, and deserialized key for', decoded_value)
    break

assert orig_key == decoded_key
assert orig_value == decoded_value
```

↓

    
    Iterating over .items():
    {'good', 'bad'} is the original (un-encoded, un-compressed, un-serialized) key for the original value of cat
    
    Iterating over ._items():
    b'NwAAAPETeyJfX3B5X18iOiAiYnVpbHRpbnMuc2V0IiwgIl9fZGF0YRwA8AFbImdvb2QiLCAiYmFkIl19' is the serialized, compressed, and encoded key for b'BQAAAFAiY2F0Ig=='
    {'good', 'bad'} is the decoded, uncompressed, and deserialized key for cat

### Stashing function results

HashStash provides two ways of stashing results.

First, here's an expensive function:

```python
# Here's an expensive function

num_times_computed = 0

def expensive_computation(names,goodnesses=['good']):
    import random
    global num_times_computed
    num_times_computed += 1
    print(f'Executing expensive_computation time #{num_times_computed}')
    ld=[]
    for n in range(1_000_000):
        d={}
        d['name']=random.choice(names)
        d['goodness']=random.choice(goodnesses)
        d['random']=random.random()
        ld.append(d)
    return random.sample(ld,k=10)

names = ['cat', 'dog']
goodnesses=['good','bad']

# execute 2 times -- different results
unstashed_result1 = expensive_computation(names, goodnesses=goodnesses)
unstashed_result2 = expensive_computation(names, goodnesses=goodnesses)
```

↓

    Executing expensive_computation time #1
    Executing expensive_computation time #2

#### Method 1: Stashing function results via `stash.run()`

```python
## set up a stash to run the function in
functions_stash = HashStash('functions_stash', clear=True)

# execute time #3
stashed_result1 = functions_stash.run(expensive_computation, names, goodnesses=goodnesses)

# calls #4-5 will not execute but return stashed result
stashed_result2 = functions_stash.run(expensive_computation, names, goodnesses=goodnesses)
stashed_result3 = functions_stash.run(expensive_computation, names, goodnesses=goodnesses)
assert stashed_result1 == stashed_result2 == stashed_result3
```

↓

    Executing expensive_computation time #3

#### Method 2: Using function decorator `@stash.stashed_result`

```python
from hashstash import stashed_result

@functions_stash.stashed_result  # or @stashed_result("functions_stash") [same HashStash call args/kwargs]
def expensive_computation2(names, goodnesses=['good']):
    return expensive_computation(names, goodnesses=goodnesses)

# will run once
stashed_result4 = expensive_computation2(names, goodnesses=goodnesses)

# then cached even when calling it normally
stashed_result5 = expensive_computation2(names, goodnesses=goodnesses)
stashed_result6 = expensive_computation2(names, goodnesses=goodnesses)
assert stashed_result4 == stashed_result5 == stashed_result6
```

↓

    Executing expensive_computation time #4

#### Accessing function result stash
Once a function is stashed via either the methods above you can access its stash as an attribute of the function:

```python
# function now has .stash attribute, from either method
func_stash = expensive_computation.stash
func_stash2 = expensive_computation2.stash
assert len(func_stash) == len(func_stash2)
print(f'Function results cached in {func_stash}\n')

# can iterate over its results normally. Keys are: (args as tuple, kwargs as dict)
func_stash = func_stash2
for key, value in func_stash.items():
    args, kwargs = key
    print(f'Stashed key = {key}')
    print(f'Called args: {args}')
    print(f'Called kwargs: {kwargs}')
    print(f'\nStashed value = {value}')

# you can get result via normal get
stashed_result7 = func_stash.get(((names,), {'goodnesses':goodnesses}))

# or via special get_func function which accepts function call syntax
stashed_result8 = func_stash.get_func(names, goodnesses=goodnesses)

assert stashed_result7 == stashed_result8 == stashed_result5 == stashed_result6
```

↓

    Function results cached in LMDBHashStash(~/.cache/hashstash/functions_stash/lmdb.hashstash.lz4/stashed_result/__main__.expensive_computation/lmdb.hashstash.lz4/data.db)
    
    Stashed key = ((['cat', 'dog'],), {'goodnesses': ['good', 'bad']})
    Called args: (['cat', 'dog'],)
    Called kwargs: {'goodnesses': ['good', 'bad']}
    
    Stashed value = [{'name': 'dog', 'goodness': 'good', 'random': 0.25762423037748594}, {'name': 'dog', 'goodness': 'good', 'random': 0.29385673877094953}, {'name': 'cat', 'goodness': 'bad', 'random': 0.5878139714240894}, {'name': 'dog', 'goodness': 'bad', 'random': 0.1936244273282528}, {'name': 'dog', 'goodness': 'good', 'random': 0.5884200909803989}, {'name': 'cat', 'goodness': 'good', 'random': 0.31725362322880646}, {'name': 'cat', 'goodness': 'good', 'random': 0.9938326430071288}, {'name': 'cat', 'goodness': 'bad', 'random': 0.20807352833935855}, {'name': 'dog', 'goodness': 'good', 'random': 0.4892373539721653}, {'name': 'cat', 'goodness': 'good', 'random': 0.9858799354889001}]

### Assembling DataFrames

HashStash can assemble DataFrames from cached contents, even nested ones. First, examples from earlier:

```python
# assemble list of flattened dictionaries from cached contents
func_stash.ld                # or stash.assemble_ld()

# assemble dataframe from flattened dictionaries of cached contents
print(func_stash.df)         # or stash.assemble_df()
```

↓

      name goodness    random
    0  dog     good  0.257624
    1  dog     good  0.293857
    2  cat      bad  0.587814
    3  dog      bad  0.193624
    4  dog     good  0.588420
    5  cat     good  0.317254
    6  cat     good  0.993833
    7  cat      bad  0.208074
    8  dog     good  0.489237
    9  cat     good  0.985880

Nested data flattening:

```python
# can also work with nested data
nested_data_stash = HashStash(engine='memory', dbname='assembling_dfs')

# populate stash with random animals
import random
for n in range(100):
    nested_data_stash[f'Animal {n+1}'] = {
        'name': (cat_or_dog := random.choice(['cat', 'dog'])), 
        'goodness': (goodness := random.choice(['good', 'bad'])),
        'etc': {
            'age': random.randint(1, 10),
            'goes_to':{
                'heaven':cat_or_dog=='dog' or goodness=='good',
            }
        }
    }

# assemble dataframe from flattened dictionaries of cached contents
print(nested_data_stash.df)         # or stash.assemble_df()
```

↓

               name goodness  etc.age  etc.goes_to.heaven
    _key                                                 
    Animal 1    cat      bad        5               False
    Animal 2    dog     good       10                True
    Animal 3    cat     good        8                True
    Animal 4    cat     good        6                True
    Animal 5    cat      bad        6               False
    ...         ...      ...      ...                 ...
    Animal 96   cat      bad        9               False
    Animal 97   dog      bad        5                True
    Animal 98   dog     good        3                True
    Animal 99   dog     good        8                True
    Animal 100  cat     good        9                True
    
    [100 rows x 4 columns]

### Append mode

Keep track of all versions of a key/value pair. All engines can track version number; "pairtree" tracks timestamp as well.

```python
append_stash = HashStash("readme_append_mode", engine='pairtree', append_mode=True, clear=True)
key = {"name":"cat"}
append_stash[key] = {"goodness": "good"}
append_stash[key] = {"goodness": "bad"}

print(f'Latest value: {append_stash.get(key)}')
print(f'All values: {append_stash.get_all(key)}')
print(f'All values with metadata: {append_stash.get_all(key, with_metadata=True)}')
```

↓

    Latest value: {'goodness': 'bad'}
    All values: [{'goodness': 'good'}, {'goodness': 'bad'}]
    All values with metadata: [{'_version': 1, '_timestamp': 1725646207.325359, '_value': {'goodness': 'good'}}, {'_version': 2, '_timestamp': 1725646207.325587, '_value': {'goodness': 'bad'}}]

Can also get metadata on dataframe:

```python
print(append_stash.assemble_df(with_metadata=True))
```

↓

                          name goodness
    _version _timestamp                
    1        1.725646e+09  cat     good
    2        1.725646e+09  cat      bad

### Temporary Caches

HashStash provides a `tmp` method for creating temporary caches that are automatically cleaned up. The temporary cache is automatically cleared and removed after the with block:

```python
with stash.tmp() as tmp_stash:
    tmp_stash["key"] = "value"
    print("key" in tmp_stash)
    
print("key" in tmp_stash)
```

↓

    True
    False

### Utilities

#### Serialization

HashStash supports multiple serialization methods:

- `serialize`: Serializes Python objects
- `deserialize`: Deserializes data back into Python objects

```python
from hashstash import serialize, deserialize

data = pd.DataFrame({"name": ["cat", "dog"], "goodness": ["good", "bad"]})
serialized_data = serialize(data, serializer="hashstash") # or jsonpickle or pickle
deserialized_data = deserialize(serialized_data, serializer="hashstash")

data.equals(deserialized_data)
```

↓

    True

### Encoding and Compression

HashStash provides functions for encoding and compressing data:

- `encode`: Encodes and optionally compresses data
- `decode`: Decodes and decompresses data

These functions are used internally by HashStash but can also be used directly:

```python
from hashstash import encode, decode

data = b"Hello, World!"
encoded_data = encode(data, compress='lz4', b64=True)
decoded_data = decode(encoded_data, compress='lz4', b64=True)

data == decoded_data
```

↓

    True

## Profiling

### Engines

LMDB is the fastest engine, followed by the custom "pairtree" implementation.

![Engines](./figures/fig.comparing_engines.png)

### Serializers

Pickle is by far the fastest serializer, but it is not transportable between Python versions. HashStash is generally faster than jsonpickle, and can serialize more data types (including lambdas and functions within functions), but it produces larger file sizes.

![Serializers](./figures/fig.comparing_serializers_size_speed.png)

### Encodings

LZ4 is the fastest compressor, but it requires an external dependency. BZ2 is the slowest, but it provides the best compression ratio.

![Compressors](./figures/fig.comparing_encodings_size_speed.png)

### All together

LMDB engine, with pickle serializer, with no compression (raw) or LZ4 or blosc compression is the fastest combination of parameters; followed by pairtree with the same. 

![All together](./figures/fig.comparing_engines_serializers_encodings.png)

## Development

### Tests

To run the tests, clone this repository and run  `pytest` in the root project directory.

### Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

### License

This project is licensed under the GNU License.
