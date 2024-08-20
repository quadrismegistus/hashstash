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


def flexible_import(obj_or_path):
    if isinstance(obj_or_path, str):
        parts = obj_or_path.split('.')
        module = parts.pop(0)
        obj = importlib.import_module(module)
        
        for part in parts:
            try:
                obj = getattr(obj, part)
            except AttributeError:
                # If attribute lookup fails, try importing as a module
                try:
                    obj = importlib.import_module(f"{module}.{part}")
                    module = f"{module}.{part}"
                except ImportError:
                    print(f"Error importing {obj_or_path}: {part} not found in {module}")
                    return None
        return obj
    else:
        return flexible_import(get_obj_addr(obj_or_path))

def can_import_object(obj):
    return flexible_import(obj) is not None




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


def call_function_politely(func, *args, **kwargs):
    """
    Gracefully call a function with refined argument handling.
    
    :param func: The function to call
    :param args: Positional arguments
    :param kwargs: Keyword arguments
    :return: The result of the function call
    """
    
    # Handle partial functions
    if isinstance(func, partial):
        partial_args = func.args
        partial_kwargs = func.keywords or {}
        func = func.func
        args = partial_args + args
        kwargs = {**partial_kwargs, **kwargs}

    # Get the function signature
    try:
        sig = inspect.signature(func)
    except Exception:
        return func(*args,**kwargs)
    
    # Filter args and kwargs
    allowed_params = list(sig.parameters.keys())
    filtered_args = [arg for i, arg in enumerate(args) if i < len(allowed_params)]
    filtered_kwargs = {k: v for k, v in kwargs.items() if k in allowed_params}

    # Remove kwargs that are already satisfied by args
    for i, param in enumerate(allowed_params):
        if i < len(filtered_args) and param in filtered_kwargs:
            del filtered_kwargs[param]

    if filtered_args or filtered_kwargs:
        args = filtered_args
        kwargs = filtered_kwargs

    try:
        return func(*args, **kwargs)
    except Exception as e:
        if args and not kwargs:
            try:
                return func(args)
            except Exception as e2:
                pass
        elif kwargs and not args:
            try:
                return func(kwargs)
            except Exception as e2:
                pass

        logger.error(f"Error calling {func.__name__}(*args, **kwargs): {e}")
        raise e
    


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
