from hashstash import *
import os
import pytest
from unittest.mock import Mock, patch
logger.setLevel(logging.CRITICAL+1)


# Test stashed_result decorator
def test_stashed_result():
    @stashed_result
    def example_function(x, y):
        return x + y

    # First call should execute the function
    result1 = example_function(2, 3)
    assert result1 == 5

    # Second call with same arguments should return cached result
    result2 = example_function(2, 3)
    assert result2 == 5

    # Call with different arguments should execute the function
    result3 = example_function(3, 4)
    assert result3 == 7

# module-level (importable, closure-free) so its cache identity is stable;
# the counter lives in a module global that is not part of the function's identity
_force_counter = {"n": 0}


def _incrementing_function():
    _force_counter["n"] += 1
    return _force_counter["n"]


def test_stashed_result_force():
    _force_counter["n"] = 0
    stash = Stash(engine="memory", dbname="wrappers_force_test").clear()
    incrementing_function = stash.stashed_result(_incrementing_function)

    # First call
    assert incrementing_function() == 1

    # Second call (should be cached)
    assert incrementing_function() == 1

    # Forced call
    assert incrementing_function(_force=True) == 2

# Test retry_patiently decorator
def test_retry_patiently():
    @retry_patiently(max_retries=3, base_delay=0.1)
    def failing_function():
        raise ValueError("Temporary error")

    with pytest.raises(ValueError):
        failing_function()

def test_retry_patiently_success():
    counter = 0

    @retry_patiently(max_retries=3, base_delay=0.1)
    def eventually_succeeding_function():
        nonlocal counter
        counter += 1
        if counter < 3:
            raise ValueError("Temporary error")
        return "Success"

    result = eventually_succeeding_function()
    assert result == "Success"
    assert counter == 3

# Test parallelized decorator
def test_parallelized():
    with HashStash().tmp() as tmp:
        @parallelized(stash=tmp)
        def parallel_function(x):
            return x * 2

        result = parallel_function([1, 2, 3, 4]).results
        assert result == [2, 4, 6, 8]


def test_parallelized_with_stashed_result():
    with Stash().tmp() as tmp:

        @parallelized(stash=tmp)
        def parallel_stashed_function(x):
            return x * 2

        # First call
        result1 = parallel_stashed_function([1, 2, 3, 4]).results
        assert result1 == [2, 4, 6, 8]

        # Second call (should be cached)
        result2 = parallel_stashed_function([1, 2, 3, 4]).results
        assert result2 == [2, 4, 6, 8]

        # Different input
        result3 = parallel_stashed_function([5, 6, 7, 8]).results
        assert result3 == [10, 12, 14, 16]

def test_parallelized_with_stashed_result_single_input():
    with Stash().tmp() as tmp:

        @tmp.stashed_result
        @parallelized
        def parallel_stashed_function(x):
            return x * 2

        # First call
        result1 = parallel_stashed_function(5)
        assert result1 == 10

        # Second call (should be cached)
        result2 = parallel_stashed_function(5)
        assert result2 == 10

        # Different input
        result3 = parallel_stashed_function(7)
        assert result3 == 14

if __name__ == "__main__":
    pytest.main()