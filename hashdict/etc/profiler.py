from ..hashdict import *

class Profiler:
    @staticmethod
    def profile_cache(
        cache_cls,
        size: int = DEFAULT_DATA_SIZE,
        verbose: bool = False,
        iter=None,
        name='profile_cache',
    ):
        cache = cache_cls(name=name)
        data = Profiler.generate_data(size)
        raw_size = len(json.dumps(data).encode())
        cache_key = f"test_data_{size}_{random.random()}"

        # Encode value to get cached size
        encoded_value = cache.encode(data)
        cached_size = len(encoded_value)

        results = []
        common_data = {
            "Encoding": Profiler.get_encoding_str(cache.compress, cache.b64),
            "Engine": cache.engine,
            "Size (B)": int(size),
            "Raw Size (B)": raw_size,
            "Cached Size (B)": cached_size,
            "Compression Ratio (%)": cached_size / raw_size * 100,
            "Iteration": iter if iter else 0,
        }

        def add_result(operation, time_taken, additional_data=None):
            result = {
                "Operation": operation,
                "Time (s)": time_taken,
                "Rate (it/s)": 1 / time_taken,
                "Speed (MB/s)": raw_size / time_taken / 1024 / 1024,
                **common_data
            }
            if additional_data:
                result.update(additional_data)
            results.append(result)

        # Measure key encoding speed
        start_time = time.time()
        encoded_key = cache.encode(cache_key)
        key_encode_time = time.time() - start_time
        add_result("Encode Key", key_encode_time)

        # ... (include all the other profiling steps)

        if verbose:
            print(results)
        return results

    @staticmethod
    def generate_data(size):
        return {
            "string": "".join(
                random.choices("abcdefghijklmnopqrstuvwxyz", k=size // 2)
            ),
            "number": random.randint(1, 1000000),
            "list": [random.randint(1, 1000) for _ in range(size // 20)],
            "nested": {
                f"key_{i}": {"value": random.random()} for i in range(size // 200)
            },
        }

    @staticmethod
    def get_encoding_str(compress: bool = None, b64: bool = None):
        encodings = []
        if compress:
            encodings.append("zlib")
        if b64:
            encodings.append("b64")
        return "+".join(encodings) if encodings else "raw"