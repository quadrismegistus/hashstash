# FileHashCache

FileHashCache is a dead simple, file-based caching library for Python that uses a hashed directory structure and compressed JSON contents for efficient storage and retrieval of cached data.

## Features

- Simple dictionary-like interface
- File-based storage for persistence
- Hashed directory structure for efficient file organization
- Compressed JSON storage for space efficiency
- Supports any JSON-serializable Python object

## Installation

You can install FileHashCache using pip:

```bash
pip install filehashcache
```

## Usage

Here's a quick example of how to use FileHashCache:

```python
from filehashcache import FileHashCache

# Create a cache instance
cache = FileHashCache(".cache")

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

# Get the number of items in the cache
print(len(cache))  # Output: 0
```

## API

### `FileHashCache(root_dir: str = ".cache")`

Create a new FileHashCache instance.

- `root_dir`: The root directory for storing cached files (default: ".cache")

### Methods

- `__setitem__(key: str, value: Any)`: Set an item in the cache
- `__getitem__(key: str) -> Any`: Get an item from the cache
- `__contains__(key: str) -> bool`: Check if a key exists in the cache
- `get(key: str, default: Any = None) -> Any`: Get an item with a default value
- `clear() -> None`: Clear all items from the cache
- `__len__() -> int`: Return the number of items in the cache
- `__iter__()`: Iterate over all keys in the cache

## How it works

FileHashCache uses a two-level directory structure based on the MD5 hash of the cache key. This helps distribute files evenly across directories, improving performance for large numbers of cached items.

Cached values are serialized to JSON, compressed using zlib, and encoded with base64 before being stored on disk. This process is reversed when retrieving cached items.

## Performance

In a sample test, FileHashCache demonstrated significant space savings:

- Raw size: 258.55 MB
- Cached size: 172.37 MB
- Compression ratio: 66.67%
- Space saved: 86.18 MB

This shows that FileHashCache can effectively reduce storage requirements while maintaining fast access to cached data.

## Running tests

To run the tests, use the following command:

```bash
pytest
```

## License

This project is licensed under the GNU License.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.