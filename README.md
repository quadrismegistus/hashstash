# HashStash

HashStash is a versatile and efficient caching library for Python that supports multiple storage engines and encoding options. It provides a simple dictionary-like interface for caching data with various backend options.

## Features

- Multiple storage engines: file (pairtree), sqlite, memory, shelve, redis, diskcache, lmdb, and dataframe
- Compression and base64 encoding options
- Configurable serialization methods
- Simple dictionary-like interface
- Efficient storage and retrieval of cached data
- Support for any serializable Python object
- Performance profiling tools
- Temporary cache creation

## Installation

You can install HashStash using pip:

```bash
pip install hashstash
```

## Usage

Here's a quick example of how to use HashStash:

```python
from hashstash import HashStash

# Create a cache instance
cache = HashStash() # default: engine is pairtree, serializer is hashstash
cache = HashStash(
    name="named_cache",
    engine="sqlitedict",
    serializer="jsonpickle",
    
)

# Store key-value pairs
cache["string_key"] = "Hello, World!"
cache[42] = {"answer": "to life, the universe, and everything"}
cache[("tuple", "key")] = [1, 2, 3, 4, 5]

# Retrieve values
print(cache["string_key"])  # Output: Hello, World!
print(cache[42])  # Output: {'answer': 'to life, the universe, and everything'}
print(cache[("tuple", "key")])  # Output: [1, 2, 3, 4, 5]

# Check if a key exists
if "string_key" in cache:
    print("Key exists!")

# Get a value with a default
value = cache.get("non_existent_key", "default_value")
print(value)  # Output: default_value

# Iterate over keys, values, and items
print("Keys:", list(cache.keys()))
print("Values:", list(cache.values()))
print("Items:", list(cache.items()))

# Update the cache with another dictionary
cache.update({"new_key": "new_value", "another_key": 123})

# Remove and return a value
popped_value = cache.pop("new_key")
print("Popped value:", popped_value)  # Output: new_value

# Remove and return the last inserted item
last_item = cache.popitem()
print("Last item:", last_item)

# Get the number of items in the cache
print("Cache size:", len(cache))

# Clear the cache
cache.clear()

# Use the cache as a context manager
with cache:
    cache["temporary"] = "This value exists only within the context"
    print(cache["temporary"])
# The 'temporary' key is automatically removed when exiting the context

# Create a temporary cache
with cache.tmp() as temp_cache:
    temp_cache["temp_key"] = "This is a temporary value"
    print(temp_cache["temp_key"])
# The temporary cache is automatically cleared and removed after the with block

# Profile the cache performance
results = cache.profiler.profile(size=[1000, 10000], iterations=3)
print(results)

# Convert cache contents to a list of dictionaries or a DataFrame
ld = cache.ld
df = cache.df

# Create a sub-cache
sub_cache = cache.sub(dbname="subcache")
sub_cache["sub_key"] = "This is in the sub-cache"

# Use the stashed_result decorator to cache function results
@cache.stashed_result
def expensive_computation(x, y):
    # Some time-consuming calculation
    return x + y

result1 = expensive_computation(5, 10)  # Computes and caches the result
result2 = expensive_computation(5, 10)  # Returns the cached result
```

## Supported Engines

HashStash supports the following storage engines:

1. File (`engine="pairtree"`)
2. SQLite (`engine="sqlite"`)
3. In-memory (`engine="memory"`)
4. Shelve (`engine="shelve"`)
5. Redis (`engine="redis"`)
6. DiskCache (`engine="diskcache"`)
7. LMDB (`engine="lmdb"`)
8. DataFrame (`engine="dataframe"`)

Each engine has its own characteristics and is suitable for different use cases. Choose the engine that best fits your needs.

## Configuration

HashStash provides a global configuration class that allows you to set default values for all HashStash instances:

```python
from hashstash import config

config.set_serializer("jsonpickle")
config.set_engine("sqlite")
config.enable_compression()
config.enable_b64()
```

## API

### `HashStash(name: str = DEFAULT_NAME, engine: ENGINE_TYPES = DEFAULT_ENGINE_TYPE, compress: bool = None, b64: bool = None, serializer: SERIALIZER_TYPES = None, *args, **kwargs)`

Create a new HashStash instance.

- `name`: The name of the cache (default: "unnamed")
- `engine`: The type of cache to create (one of the supported engines)
- `compress`: Whether to use compression (default: None, uses global config)
- `b64`: Whether to use base64 encoding (default: None, uses global config)
- `serializer`: The serialization method to use (default: None, uses global config)
- `*args`, `**kwargs`: Additional arguments to pass to the cache constructor

### Methods

HashStash implements the `MutableMapping` interface, providing the following methods:

- `__setitem__(key: str, value: Any)`: Set an item in the cache
- `__getitem__(key: str) -> Any`: Get an item from the cache
- `__contains__(key: str) -> bool`: Check if a key exists in the cache
- `get(key: str, default: Any = None) -> Any`: Get an item with a default value
- `clear() -> None`: Clear all items from the cache
- `__len__() -> int`: Return the number of items in the cache
- `__iter__()`: Iterate over all keys in the cache

Additional methods:

- `keys()`: Return an iterator over the cache keys
- `values()`: Return an iterator over the cache values
- `items()`: Return an iterator over the cache key-value pairs
- `update(other=None, **kwargs)`: Update the cache with key-value pairs from another dictionary or keyword arguments
- `setdefault(key, default=None)`: Set a key with a default value if it doesn't exist, and return the value
- `pop(key, default=None)`: Remove and return the value for a key, or return the default if the key doesn't exist
- `popitem()`: Remove and return a (key, value) pair from the cache

## Performance Profiling

HashStash includes a performance profiling tool to help you evaluate different cache configurations. You can use it as follows:

```python
from hashstash import HashStash

# Create a cache instance
cache = HashStash(name="profile_cache", engine="pairtree")

# Run the profiler
results = cache.profile(
    size=[1000, 10000, 100000],
    iterations=5
)

# The results are returned as a pandas DataFrame
print(results)
```

You can also use the `HashStashProfiler` class directly for more advanced profiling options.

## Temporary Caches

HashStash provides a `tmp` method for creating temporary caches that are automatically cleaned up:

```python
from hashstash import HashStash

cache = HashStash(name="my_cache", engine="pairtree")

with cache.tmp() as temp_cache:
    # Use temp_cache as a temporary cache
    temp_cache["key"] = "value"
    print(temp_cache["key"])  # Output: value

# The temporary cache is automatically cleared and removed after the with block
```

To expand on the utilities section of the README, I'll add more details about the available utility functions and their usage. Here's an updated version of the Utilities section:

## Utilities

HashStash provides various utility functions for encoding, serialization, and more. These utilities are used internally but can also be helpful for advanced users. Some key utilities include:

### Caching Function Results

- `stashed_result`: A decorator for caching function results. This can be used to automatically cache the output of functions, improving performance for repeated calls with the same inputs.

Example usage:
```python
from hashstash import stashed_result

@stashed_result(name="my_cached_function")
def expensive_computation(x, y):
    # Some time-consuming calculation
    return x + y

# The first call will compute and cache the result
result1 = expensive_computation(5, 10)

# Subsequent calls with the same arguments will return the cached result
result2 = expensive_computation(5, 10)  # This will be much faster
```

### Encoding and Compression

HashStash provides functions for encoding and compressing data:

- `encode`: Encodes and optionally compresses data
- `decode`: Decodes and decompresses data

These functions are used internally by HashStash but can also be used directly:

```python
from hashstash.utils import encode, decode

data = "Hello, World!"
encoded_data = encode(data, compress=True, b64=True)
decoded_data = decode(encoded_data, compress=True, b64=True)
```

### Serialization

HashStash supports multiple serialization methods:

- `serialize`: Serializes Python objects
- `deserialize`: Deserializes data back into Python objects

Example:
```python
from hashstash.utils import serialize, deserialize

data = {"name": "John", "age": 30}
serialized_data = serialize(data, serializer="jsonpickle")
deserialized_data = deserialize(serialized_data, serializer="jsonpickle")
```

## Running Tests

To run the tests, use the following command:

```bash
pytest
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the GNU License.