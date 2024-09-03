from hashstash import *
import pytest
from unittest.mock import patch, MagicMock
from hashstash.utils import logs
from concurrent.futures import ProcessPoolExecutor
from hashstash.utils.pmap import pmap, _pmap_item, progress_bar
from hashstash.engines.base import HashStash
from hashstash.serializers.serializer import serialize, deserialize
logger.setLevel(logging.CRITICAL+1)

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
    res = list(pmap(failing_function, objects=[1, 2, 3, 4], num_proc=2))
    assert None in res

@pytest.fixture
def mock_log_prefix_str():
    with patch('hashstash.utils.logs.log_prefix_str', return_value='Test') as mock:
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
        from hashstash.utils.pmap import current_depth, progress_bar
        
        initial_depth = current_depth
        test_iter = range(5)
        mock_tqdm.return_value = MagicMock(__iter__=lambda self: iter(test_iter))
        
        list(progress_bar(test_iter, progress=True))
        
        assert current_depth == initial_depth

def square(x):
    return x ** 2


@patch('concurrent.futures.ProcessPoolExecutor')
def test_pmap_multi_process(mock_executor):
    mock_future = MagicMock()
    mock_future.result.side_effect = [i**2 for i in range(5)]
    mock_submit = MagicMock(return_value=mock_future)
    mock_executor.return_value.__enter__.return_value.submit = mock_submit
    
    result = list(pmap(square, objects=range(5), num_proc=2, progress=False, ordered=True))
    assert result == [0, 1, 4, 9, 16]

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

def test_pmap_with_stash():
    with HashStash().tmp() as stash:
        result = list(pmap(square, objects=[1, 2, 3], num_proc=1, stash=stash, progress=False))
        func_stash = square.stash
        assert result == [1, 4, 9]
        assert len(func_stash) == 3  # Check if 3 items were stored in the function's stash
        assert len(stash) == 0  # The main stash should remain empty
        
        # Check if the function's stash is a sub-stash of the main stash
        assert func_stash.parent == stash
        
        # Verify that the results are in the function's stash
        for i in [1, 2, 3]:
            assert func_stash.get_func(i) == i**2


def test_pmap_item_without_stash():
    item = stuff({
        "func": square,
        "args": [3],
        "kwargs": {},
    })
    result = _pmap_item(item)
    assert result == 9

def test_pmap_item_with_stash():
    with HashStash().tmp() as stash:
        
        item = stuff({
            "func": square,
            "args": [3],
            "kwargs": {},
            "stash": stash,
        })
        result = _pmap_item(item)
        
        assert result == 9
        assert len(stash.sub_function_results(square)) == 1  # Check if one item was stored in the stash

def test_pmap_with_total():
    result = list(pmap(square, objects=[1, 2, 3, 4], num_proc=2, total=3, progress=False))
    assert result == [1, 4, 9]  # Should only process the first 3 items

@pytest.mark.parametrize("num_proc", [1, 2, 4])
def test_pmap_different_num_proc(num_proc):
    result = list(pmap(square, objects=[1, 2, 3, 4], num_proc=num_proc, progress=False))
    assert result == [1, 4, 9, 16]

# def test_pmap_with_description():
#     with patch('hashstash.utils.pmap.progress_bar') as mock_progress_bar:
#         mock_progress_bar.side_effect = lambda iterr, **kwargs: iterr  # Simple pass-through for the iterator
#         list(pmap(square, objects=[1, 2, 3], num_proc=2, desc="Test", progress=True))
#         mock_progress_bar.assert_called_once()
#         assert 'desc' in mock_progress_bar.call_args[1]
#         assert mock_progress_bar.call_args[1]['desc'] == "Test [2x]"