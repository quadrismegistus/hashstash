from hashstash import *
import pytest
from unittest.mock import patch, MagicMock

def square(x):
    return x * x

def multiply(x, y):
    return x * y

def test_pmap_basic():
    result = list(pmap(square, objects=[1, 2, 3, 4], num_proc=2))
    assert result == [1, 4, 9, 16]

def test_pmap_with_options():
    result = list(pmap(multiply, objects=[2, 3, 4], options=[{'y': 3}, {'y': 4}, {'y': 5}], num_proc=2))
    assert result == [6, 12, 20]

def test_pmap_single_process():
    result = list(pmap(square, objects=[1, 2, 3, 4], num_proc=1))
    assert result == [1, 4, 9, 16]

def test_pmap_unordered():
    result = set(pmap(square, objects=[1, 2, 3, 4], num_proc=2, ordered=False))
    assert result == {1, 4, 9, 16}

def test_pmap_common_args():
    result = list(pmap(multiply, objects=[2, 3, 4], num_proc=2, y=5))
    assert result == [10, 15, 20]

def test_pmap_empty_input():
    with pytest.raises(ValueError):
        list(pmap(square))

def test_pmap_mismatched_lengths():
    with pytest.raises(ValueError):
        list(pmap(square, objects=[1, 2, 3], options=[{}, {}]))

def slow_square(x):
    time.sleep(1)
    return x * x

def test_pmap_keyboard_interrupt():
    with pytest.raises(KeyboardInterrupt):
        result = pmap(slow_square, objects=[1, 2, 3, 4], num_proc=2)
        next(result)  # Start the generator
        raise KeyboardInterrupt()

def failing_function(x):
    if x == 2:
        raise ValueError("Simulated error")
    return x * x

def test_pmap_exception_handling():
    with pytest.raises(ValueError):
        list(pmap(failing_function, objects=[1, 2, 3, 4], num_proc=2))

@pytest.fixture
def mock_log_prefix_str():
    with patch('hashstash.utils.pmap.log_prefix_str', return_value='Test') as mock:
        yield mock

@pytest.fixture
def mock_tqdm():
    with patch('hashstash.utils.pmap.tqdm', create=True) as mock:
        yield mock

def test_progress_bar_without_progress():
    test_iter = range(5)
    result = list(progress_bar(test_iter, progress=False))
    
    assert result == list(test_iter)

def test_progress_bar_tqdm_not_available(mock_log_prefix_str):
    with patch.dict('sys.modules', {'tqdm': None}):
        test_iter = range(5)
        result = list(progress_bar(test_iter, progress=True))
        
        assert result == list(test_iter)

def test_progress_bar_current_depth(mock_log_prefix_str):
    with patch('hashstash.utils.pmap.tqdm', create=True) as mock_tqdm:
        from hashstash.utils.pmap import current_depth
        
        initial_depth = current_depth
        test_iter = range(5)
        mock_tqdm.return_value = MagicMock(__iter__=lambda self: iter(test_iter))
        
        list(progress_bar(test_iter, progress=True))
        
        assert current_depth == initial_depth


@patch('hashstash.utils.pmap.ProcessPoolExecutor')
def test_pmap_multi_process(mock_executor):
    def square(x):
        return x ** 2
    
    mock_future = MagicMock()
    mock_future.result.side_effect = [i**2 for i in range(5)]
    mock_submit = MagicMock(return_value=mock_future)
    mock_executor.return_value.__enter__.return_value.submit = mock_submit
    
    result = list(pmap(square, objects=range(5), num_proc=2, progress=False))
    assert result == [0, 1, 4, 9, 16]
    assert mock_submit.call_count == 5

def test_pmap_single_process():
    def square(x):
        return x ** 2
    
    result = list(pmap(square, objects=range(5), num_proc=1, progress=False))
    assert result == [0, 1, 4, 9, 16]

def test_pmap_with_options():
    def power(x, n):
        return x ** n
    
    result = list(pmap(power, objects=[2, 3, 4], options=[{'n': 2}, {'n': 3}, {'n': 2}], num_proc=1, progress=False))
    assert result == [4, 27, 16]

def test_pmap_error_handling():
    with pytest.raises(ValueError):
        list(pmap(lambda x: x, objects=[], options=[]))

    with pytest.raises(ValueError):
        list(pmap(lambda x: x, objects=[1, 2], options=[{}]))