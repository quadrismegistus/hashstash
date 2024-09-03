import pytest
from hashstash import *
logger.setLevel(logging.CRITICAL+1)

def test_function():
    pass

class TestClass:
    def test_method(self):
        pass

@pytest.fixture
def sample_objects():
    return {
        'function': test_function,
        'method': TestClass.test_method,
        'class': TestClass,
        'builtin': len,
    }

def test_get_obj_module(sample_objects):
    assert get_obj_module(sample_objects['function']) == 'test_addrs'
    assert get_obj_module(sample_objects['method']) == 'test_addrs'
    assert get_obj_module(sample_objects['class']) == 'test_addrs'
    assert get_obj_module(sample_objects['builtin']) == 'builtins'

def test_get_obj_addr(sample_objects):
    assert get_obj_addr(sample_objects['function']) == 'test_addrs.test_function'
    assert get_obj_addr(sample_objects['method']) == 'test_addrs.TestClass.test_method'
    assert get_obj_addr(sample_objects['class']) == 'test_addrs.TestClass'

def test_get_obj_name(sample_objects):
    assert get_obj_name(sample_objects['function']) == 'test_function'
    assert get_obj_name(sample_objects['method']) == 'test_method'
    assert get_obj_name(sample_objects['class']) == 'TestClass'
    assert get_obj_name(sample_objects['builtin']) == 'len'

def test_get_obj_nice_name(sample_objects):
    assert get_obj_nice_name(sample_objects['function']) == 'test_addrs.test_function'
    assert get_obj_nice_name(sample_objects['method']) == 'TestClass.test_method'
    assert get_obj_nice_name(sample_objects['class']) == 'test_addrs.TestClass'
    assert get_obj_nice_name(sample_objects['builtin']) == 'len'

def test_flexible_import():
    assert flexible_import('os.path.join') == __import__('os').path.join
    assert flexible_import('hashstash.utils.addrs.get_obj_addr') == get_obj_addr
    assert flexible_import(get_obj_addr) == get_obj_addr

def test_can_import_object():
    assert can_import_object('os.path.join') == True
    assert can_import_object('non_existent_module.function') == False
    assert can_import_object(get_obj_addr) == True
