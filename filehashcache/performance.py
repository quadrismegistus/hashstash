import os
import json
import random
import time
import statistics
import pandas as pd

def generate_data(size):
    return {
        "string": "".join(random.choices("abcdefghijklmnopqrstuvwxyz", k=size // 2)),
        "number": random.randint(1, 1000000),
        "list": [random.randint(1, 1000) for _ in range(size // 20)],
        "nested": {f"key_{i}": {"value": random.random()} for i in range(size // 200)}
    }

def run_performance_tests(cls, root_dir: str = ".cache_test", sizes: list = None, iterations: int = 5):
    if sizes is None:
        sizes = [1000, 10000, 100000, 1000000, 10000000, 50000000, 100_000_000]

    cache = cls(root_dir)
    results = []

    print("\nRunning compression and speed tests...")
    for size in sizes:
        print(f"Testing size: {size:,d} bytes")
        for i in range(iterations):
            print(f"  Iteration {i+1}/{iterations}", end="\r")
            data = generate_data(size)
            raw_size = len(json.dumps(data).encode())
            
            # Measure write speed
            start_time = time.time()
            cache[f"test_data_{size}_{i}"] = data
            write_time = time.time() - start_time
            
            file_path = cache._get_file_path(f"test_data_{size}_{i}")
            cached_size = os.path.getsize(file_path)
            
            # Measure read speed
            start_time = time.time()
            _ = cache[f"test_data_{size}_{i}"]
            read_time = time.time() - start_time
            
            results.append({
                'Size': size,
                'Raw Size (MB)': raw_size / 1024 / 1024,
                'Cached Size (MB)': cached_size / 1024 / 1024,
                'Space Saved (MB)': (raw_size - cached_size) / 1024 / 1024,
                'Write Speed (MB/s)': raw_size / write_time / 1024 / 1024,
                'Read Speed (MB/s)': raw_size / read_time / 1024 / 1024,
                'Write Time (s)': write_time,
                'Read Time (s)': read_time
            })
        print()  # New line after each size test

    df = pd.DataFrame(results)
    
    print("\nCompression Statistics:")
    print(df.groupby('Size').mean().round(2))

    print("\nGlobal Averages:")
    global_stats = df.mean()
    print(f"Compression Ratio: {(1 - global_stats['Cached Size (MB)'] / global_stats['Raw Size (MB)']) * 100:.2f}%")
    print(f"Write Speed: {global_stats['Write Speed (MB/s)']:.2f} MB/s")
    print(f"Read Speed: {global_stats['Read Speed (MB/s)']:.2f} MB/s")

    # Clean up the test cache
    cache.clear()
    os.rmdir(root_dir)

    return df