import pytest
from hashstash import *
logger.setLevel(logging.CRITICAL+1)

def sample_function():
    pass

class SampleClass:
    def sample_method(self):
        pass

@pytest.fixture
def sample_objects():
    return {
        'function': sample_function,
        'method': SampleClass.sample_method,
        'class': SampleClass,
        'builtin': len,
    }

def test_get_obj_module(sample_objects):
    assert get_obj_module(sample_objects['function']) == 'test_addrs'
    assert get_obj_module(sample_objects['method']) == 'test_addrs'
    assert get_obj_module(sample_objects['class']) == 'test_addrs'
    assert get_obj_module(sample_objects['builtin']) == 'builtins'

def test_get_obj_addr(sample_objects):
    assert get_obj_addr(sample_objects['function']) == 'test_addrs.sample_function'
    assert get_obj_addr(sample_objects['method']) == 'test_addrs.SampleClass.sample_method'
    assert get_obj_addr(sample_objects['class']) == 'test_addrs.SampleClass'

def test_get_obj_name(sample_objects):
    assert get_obj_name(sample_objects['function']) == 'sample_function'
    assert get_obj_name(sample_objects['method']) == 'sample_method'
    assert get_obj_name(sample_objects['class']) == 'SampleClass'
    assert get_obj_name(sample_objects['builtin']) == 'len'

def test_get_obj_nice_name(sample_objects):
    assert get_obj_nice_name(sample_objects['function']) == 'test_addrs.sample_function'
    assert get_obj_nice_name(sample_objects['method']) == 'SampleClass.sample_method'
    assert get_obj_nice_name(sample_objects['class']) == 'test_addrs.SampleClass'
    assert get_obj_nice_name(sample_objects['builtin']) == 'len'

def test_flexible_import():
    assert flexible_import('os.path.join') == __import__('os').path.join
    assert flexible_import('hashstash.utils.addrs.get_obj_addr') == get_obj_addr
    assert flexible_import(get_obj_addr) == get_obj_addr

def test_can_import_object():
    assert can_import_object('os.path.join') == True
    assert can_import_object('non_existent_module.function') == False
    assert can_import_object(get_obj_addr) == True


def test_lambda_src():
    l1 = lambda x: x
    l2 = lambda x: (x+1)*x
    l3 = lambda x: ([x]*x)+1
    [(l4:=lambda x: ([x]))]
    [(l5:=lambda x: ([x])+1)]
    l6=[lambda x: [[x*2]]]

    assert get_lambda_src(l1) == 'lambda x: x'
    assert get_lambda_src(l2) == 'lambda x: (x+1)*x'
    assert get_lambda_src(l3) == 'lambda x: ([x]*x)+1'
    assert get_lambda_src(l4) == 'lambda x: ([x])'
    assert get_lambda_src(l5) == 'lambda x: ([x])+1'
    assert get_lambda_src(l6[0]) == 'lambda x: [[x*2]]'
