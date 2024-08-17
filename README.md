# HashStash

HashStash is a versatile and efficient caching library for Python that supports multiple storage engines and encoding options. It provides a simple dictionary-like interface for caching data with various backend options.

## Features

- Multiple storage engines: file, sqlite, memory, shelve, redis, pickledb, diskcache, and lmdb
- Compression and base64 encoding options
- Simple dictionary-like interface
- Efficient storage and retrieval of cached data
- Support for any JSON-serializable Python object
- Performance profiling tools

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
cache = HashStash(name="my_cache", engine="file", compress=True, b64=True)

# Store a value
cache["my_key"] = {"name": "John", "age": 30}

# Retrieve a value
data = cache["my_key"]
print(data)  # Output: {'name': 'John', 'age': 30}

# Check if a key exists
if "my_key" in cache:
    print("Key exists!")

# Get a value with a default
value = cache.get("non_existent_key", "default_value")
print(value)  # Output: default_value

# Clear the cache
cache.clear()
```

## Supported Engines

HashStash supports the following storage engines:

1. File (`engine="file"`)
2. SQLite (`engine="sqlite"`)
3. In-memory (`engine="memory"`)
4. Shelve (`engine="shelve"`)
5. Redis (`engine="redis"`)
6. PickleDB (`engine="pickledb"`)
7. DiskCache (`engine="diskcache"`)
8. LMDB (`engine="lmdb"`)

Each engine has its own characteristics and is suitable for different use cases. Choose the engine that best fits your needs.

## API

### `HashStash(name: str = DEFAULT_NAME, engine: ENGINE_TYPES = DEFAULT_ENGINE_TYPE, *args, **kwargs)`

Create a new HashStash instance.

- `name`: The name of the cache (default: "unnamed")
- `engine`: The type of cache to create (one of the supported engines)
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
cache = HashStash(name="profile_cache", engine="file")

# Run the profiler
results = cache.profile(
    engine=["file", "sqlite", "memory"],
    compress=[True, False],
    b64=[True, False],
    size=[1000, 10000, 100000],
    iterations=5
)

# The results are returned as a pandas DataFrame
print(results)
```

For more detailed usage of the profiler, refer to the `performance.py` file.

## Utilities

HashStash provides various utility functions for encoding, serialization, and more. These utilities are used internally but can also be helpful for advanced users. Some key utilities include:

- `cached_result`: A decorator for caching function results
- `Encoder` and `Decoder` classes for handling data encoding and decoding
- `Serializer` and `Deserializer` classes for object serialization

## Running Tests

To run the tests, use the following command:

```bash
pytest
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the GNU License.