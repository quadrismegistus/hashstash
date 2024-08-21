from . import *

def get_obj_module(obj):
    if hasattr(obj, "__module__"): return obj.__module__
    if hasattr(obj, "__class__"): return get_obj_module(obj.__class__)
    return type(obj).__module__

def get_obj_addr(obj):
    if isinstance(obj, types.FunctionType):
        # Handle functions and unbound methods
        if hasattr(obj, '__qualname__'):
            return f"{obj.__module__}.{obj.__qualname__}"
        return f"{obj.__module__}.{obj.__name__}"
    
    if isinstance(obj, types.MethodType):
        # Handle bound methods
        return f"{obj.__module__}.{obj.__self__.__class__.__name__}.{obj.__name__}"
    
    if isinstance(obj, type):
        # Handle classes
        return f"{obj.__module__}.{obj.__name__}"
    
    module = get_obj_module(obj)
    name = get_obj_name(obj)
    
    # Remove any method name from the qualname
    if '.' in name:
        name = name.rsplit('.', 1)[0]
    
    return f'{module}.{name}'

def get_obj_name(obj):
    if isinstance(obj, type):
        return obj.__name__
    if hasattr(obj, '__class__'):
        return obj.__class__.__name__
    return type(obj).__name__

def get_obj_nice_name(obj):
    return '.'.join(get_obj_addr(obj).split('.')[-2:]) if get_obj_module(obj) != 'builtins' else get_obj_name(obj)




def get_function_str(func):
    try:
        source = inspect.getsource(func)
        lines = source.splitlines()
        # Find the function definition line
        func_start = next(
            (i for i, ln in enumerate(lines) if ln.lstrip().startswith("def ")), None
        )
        if func_start is not None:
            # Extract function body (including definition line)
            func_body = lines[func_start:]
            # Dedent the function body
            dedented_func = textwrap.dedent("\n".join(func_body))
            return dedented_func
        return ""  # Return empty string if function definition not found
    except Exception:
        return ""
