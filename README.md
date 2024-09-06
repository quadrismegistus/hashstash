# HashStash

HashStash is a versatile caching library for Python that supports multiple storage engines, serializers, and encoding options. It provides a simple dictionary-like interface for caching data with various backend options. HashStash is designed to be easy to use, flexible, and efficient.

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
)

# clear for this readme
stash.clear()

# show stash config
stash
```

<pre>PairtreeHashStash</pre><table border="1" class="dataframe"><thead><tr><th>Config</th><th>Param</th><th>Value</th></tr></thead><tbody><tr><td><b>Path</b></td><td>Root Dir</td><td><i>/Users/ryan/.cache/hashstash/project_stash</i></td></tr><tr><td><b></b></td><td>Dbname</td><td><i>sub_stash</i></td></tr><tr><td><b></b></td><td>Filename</td><td><i>data.db</i></td></tr><tr><td><b>Engine</b></td><td>Engine</td><td><i>pairtree</i></td></tr><tr><td><b></b></td><td>Serializer</td><td><i>hashstash</i></td></tr><tr><td><b></b></td><td>Compress</td><td><i>lz4</i></td></tr><tr><td><b></b></td><td>B64</td><td><i>True</i></td></tr></tbody></table>

### Stashing objects

Literally anything can be a key or value, including lambdas, local functions, sets, dataframes, dictionaries, etc:

```python
# traditional dictionary keys,,,
string_key = "bad"
tuple_key = ("bad","good")

# ...unhashable keys...
dict_key = {"goodness":"bad"}
list_key = ["bad","good"]
set_key = {"bad","good"}

# ...func keys...
def func_key(x): pass
lambda_key = lambda x: x

# ...very unhashable keys...
import pandas as pd
df_key = pd.DataFrame({"name":["cat"], "goodness":["bad"]})

# ...can all be assigned to a value on a stash:
all_keys = [string_key, tuple_key, dict_key, list_key, set_key, func_key, lambda_key, df_key]
for key in all_keys:
    stash[key] = "cat"

# all should equal "cat":
(
    "cat"
    == stash[string_key]
    == stash[tuple_key]
    == stash[dict_key]
    == stash[list_key]
    == stash[set_key]
    == stash[func_key]
    == stash[lambda_key]
    == stash[df_key]
)
```

↓

    True

### Works like a dictionary

HashStash fully implements the dictionary's `MutableMapping` interface, providing all its methods, including:

```python
# get()
assert stash.get(df_key) == "cat"
assert stash.get('fake_key') == None

# __contains__
assert df_key in stash

# __len__
assert len(stash) == len(all_keys) # from earlier

# keys()
for i,key in enumerate(stash.keys()): 
    print(f'Key #{i+1}: {key} ({type(key).__name__})')

# values()
for value in stash.values():
    assert value == "cat"

# items()
for key, value in stash.items(): pass

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

↓

    Key #1: {'good', 'bad'} (set)
    Key #2: <function func_key at 0x116acdd80> (function)
    Key #3: {'goodness': 'bad'} (dict)
    Key #4: bad (str)
    Key #5:   name goodness
    0  cat      bad (DataFrame)
    Key #6: ('bad', 'good') (tuple)
    Key #7: ['bad', 'good'] (list)
    Key #8: <function <lambda> at 0x12008e710> (function)

#### Under the hood

HashStash adds dictionary-like methods to the base `MutableMapping` interface, including:

- `_keys()`: Return an iterator over the encoded keys

- `_values()`: Return an iterator over the encoded values

- `_items()`: Return an iterator over the encoded key-value pairs

```python
print('\nIterating over items:')
for orig_key,orig_value in stash.items():
    print(orig_key, 'is the original (un-encoded, un-compressed, un-serialized) key for the original value of', orig_value)
    break

print('Iterating over encoded items:')
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

    
    Iterating over items:
    {'good', 'bad'} is the original (un-encoded, un-compressed, un-serialized) key for the original value of cat
    Iterating over encoded items:
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
expensive_computation2(names, goodnesses=goodnesses)

# then cached even when calling it normally
stashed_result3 = expensive_computation2(names, goodnesses=goodnesses)
stashed_result4 = expensive_computation2(names, goodnesses=goodnesses)
assert stashed_result3 == stashed_result4
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
print(f'Function results cached in {func_stash}')

# can iterate over its results normally. Keys are: (args as tuple, kwargs as dict)
for key, value in func_stash.items():
    args, kwargs = key
    print(f'Stashed key = {key}')
    print(f'Called args: {args}')
    print(f'Called kwargs: {kwargs}')
    print(f'Stashed value = {value}')

# you can get result via normal get
stashed_result5 = func_stash.get(((names,), {'goodnesses':goodnesses}))

# or via special get_func function which accepts function call syntax
stashed_result6 = func_stash.get_func(names, goodnesses=goodnesses)

assert stashed_result5 == stashed_result6 == stashed_result2 == stashed_result1
```

↓

    Function results cached in LMDBHashStash(~/.cache/hashstash/functions_stash/lmdb.hashstash.lz4/stashed_result/__main__.expensive_computation/lmdb.hashstash.lz4/data.db)
    Stashed key = ((['cat', 'dog'],), {'goodnesses': ['good', 'bad']})
    Called args: (['cat', 'dog'],)
    Called kwargs: {'goodnesses': ['good', 'bad']}
    Stashed value = [{'name': 'cat', 'goodness': 'bad', 'random': 0.4937376541904154}, {'name': 'cat', 'goodness': 'bad', 'random': 0.7268348058953994}, {'name': 'cat', 'goodness': 'good', 'random': 0.35729538077994716}, {'name': 'cat', 'goodness': 'good', 'random': 0.725394281859107}, {'name': 'dog', 'goodness': 'bad', 'random': 0.06812236424848028}, {'name': 'cat', 'goodness': 'bad', 'random': 0.7554594874500936}, {'name': 'cat', 'goodness': 'good', 'random': 0.38011863168892035}, {'name': 'cat', 'goodness': 'bad', 'random': 0.2614406849947434}, {'name': 'cat', 'goodness': 'bad', 'random': 0.5981626433377101}, {'name': 'dog', 'goodness': 'bad', 'random': 0.6349477055444926}]

### Assembling DataFrames

HashStash can assemble DataFrames from cached contents, even nested ones:

```python
# assemble list of flattened dictionaries from cached contents
func_stash.ld         # or stash.assemble_ld()

# assemble dataframe from flattened dictionaries of cached contents
func_stash.df         # or stash.assemble_df()
```

<div>

<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>name</th>
      <th>goodness</th>
      <th>random</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>cat</td>
      <td>bad</td>
      <td>0.493738</td>
    </tr>
    <tr>
      <th>1</th>
      <td>cat</td>
      <td>bad</td>
      <td>0.726835</td>
    </tr>
    <tr>
      <th>2</th>
      <td>cat</td>
      <td>good</td>
      <td>0.357295</td>
    </tr>
    <tr>
      <th>3</th>
      <td>cat</td>
      <td>good</td>
      <td>0.725394</td>
    </tr>
    <tr>
      <th>4</th>
      <td>dog</td>
      <td>bad</td>
      <td>0.068122</td>
    </tr>
    <tr>
      <th>5</th>
      <td>cat</td>
      <td>bad</td>
      <td>0.755459</td>
    </tr>
    <tr>
      <th>6</th>
      <td>cat</td>
      <td>good</td>
      <td>0.380119</td>
    </tr>
    <tr>
      <th>7</th>
      <td>cat</td>
      <td>bad</td>
      <td>0.261441</td>
    </tr>
    <tr>
      <th>8</th>
      <td>cat</td>
      <td>bad</td>
      <td>0.598163</td>
    </tr>
    <tr>
      <th>9</th>
      <td>dog</td>
      <td>bad</td>
      <td>0.634948</td>
    </tr>
  </tbody>
</table>
</div>

### Append mode

Keep track of all versions of a key/value pair. All engines can track version number; "pairtree" tracks timestamp as well.

```python
stash = HashStash("readme_append_mode", engine='pairtree', append_mode=True).clear()
stash["cat"] = {"goodness": "good"}
stash["cat"] = {"goodness": "bad"}
stash.get_all("cat")
```

↓

    [{'goodness': 'good'}, {'goodness': 'bad'}]

`.get()` will always return latest version:

```python
stash.get("cat")
```

↓

    {'goodness': 'bad'}

`with_metadata=True` will include version number, and timestamp if using pairtree engine:

```python
stash.get_all("cat", with_metadata=True)
```

↓

    [{'_version': 1,
      '_timestamp': 1725645080.660893,
      '_value': {'goodness': 'good'}},
     {'_version': 2,
      '_timestamp': 1725645080.661101,
      '_value': {'goodness': 'bad'}}]

You can also include metadata in the assembled dataframe:

```python
stash.assemble_df(with_metadata=True)
```

<div>

<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th></th>
      <th></th>
      <th>goodness</th>
    </tr>
    <tr>
      <th>_key</th>
      <th>_version</th>
      <th>_timestamp</th>
      <th></th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th rowspan="2" valign="top">cat</th>
      <th>1</th>
      <th>1.725645e+09</th>
      <td>good</td>
    </tr>
    <tr>
      <th>2</th>
      <th>1.725645e+09</th>
      <td>bad</td>
    </tr>
  </tbody>
</table>
</div>

### Temporary Caches

HashStash provides a `tmp` method for creating temporary caches that are automatically cleaned up. The temporary cache is automatically cleared and removed after the with block:

```python
stash = HashStash()

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
