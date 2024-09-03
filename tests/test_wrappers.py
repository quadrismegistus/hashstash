from hashstash import *
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

counter = 0
tmp = Stash(engine='memory').clear()

@tmp.stashed_result
def incrementing_function():
    global counter
    counter += 1
    return counter
logger.setLevel(logging.DEBUG)
def test_stashed_result_force():
    global counter
    counter = 0  # Reset counter at the start of the test
    
    # First call
    result1 = incrementing_function()
    assert result1 == 1

    # Second call (should be cached)
    result2 = incrementing_function()
    assert result2 == 1

    # Forced call
    result3 = incrementing_function(_force=True)
    assert result3 == 2

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

        result = parallel_function([1, 2, 3, 4])
        assert result == [2, 4, 6, 8]

def test_parallelized_single_input():
    logger.setLevel(logging.INFO)
    with Stash().tmp() as tmp:
        @parallelized(stash=tmp)
        def parallel_function(x):
            return x * 2

        result = parallel_function(5)
        assert result == 10

def test_parallelized_with_stashed_result():
    logger.setLevel(logging.INFO)
    with Stash().tmp() as tmp:

        @parallelized(stash=tmp)
        def parallel_stashed_function(x):
            return x * 2

        # First call
        result1 = parallel_stashed_function([1, 2, 3, 4])
        assert result1 == [2, 4, 6, 8]

        # Second call (should be cached)
        result2 = parallel_stashed_function([1, 2, 3, 4])
        assert result2 == [2, 4, 6, 8]

        # Different input
        result3 = parallel_stashed_function([5, 6, 7, 8])
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