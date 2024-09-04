from . import *
BUILTIN_DECORATORS = {'property', 'classmethod', 'staticmethod'}


def get_obj_module(obj):
    if hasattr(obj, "__module__"): return obj.__module__
    if hasattr(obj, "__class__"): return get_obj_module(obj.__class__)
    return type(obj).__module__
def get_obj_addr(obj):
    if isinstance(obj, types.BuiltinFunctionType):
        return f"builtins.{obj.__name__}"
    
    obj = unwrap_func(obj)
    
    if isinstance(obj, (types.FunctionType, types.MethodType)):
        # Handle functions, unbound methods, and bound methods
        if hasattr(obj, '__qualname__'):
            # For unbound methods and functions
            return f"{obj.__module__}.{obj.__qualname__}"
        elif is_method(obj):
            # For bound methods
            class_name = obj.__self__.__class__.__name__
            return f"{obj.__module__}.{class_name}.{obj.__name__}"
        elif is_classmethod(obj):
            class_name = obj.__self__.__name__
            return f"{obj.__module__}.{class_name}.{obj.__name__}"
        else:
            return f"{obj.__module__}.{obj.__name__}"
    
    if isinstance(obj, type):
        # Handle classes
        return f"{obj.__module__}.{obj.__name__}"
    
    # Handle class methods
    if isinstance(obj, classmethod):
        func = obj.__func__
        class_name = func.__qualname__.split('.')[0]
        return f"{func.__module__}.{class_name}.{func.__name__}"
    
    module = get_obj_module(obj)
    name = get_obj_name(obj)
    
    # Remove any method name from the qualname
    if '.' in name:
        name = name.rsplit('.', 1)[0]
    
    return f'{module}.{name}'


def get_obj_name(obj):
    if isinstance(obj, (types.FunctionType, types.MethodType, types.BuiltinFunctionType)):
        return obj.__name__
    if isinstance(obj, type):
        return obj.__name__
    if hasattr(obj, '__class__'):
        return obj.__class__.__name__
    return type(obj).__name__

def get_obj_nice_name(obj):
    if get_obj_module(obj) == 'builtins':
        return get_obj_name(obj)
    return '.'.join(get_obj_addr(obj).split('.')[-2:])

def get_function_src(func):
    from .logs import log
    if func.__name__ == '<lambda>':
        return get_lambda_src(func)

    try:
        source = inspect.getsource(func)
        lines = source.splitlines()

        # Remove non-builtin decorators
        while lines and lines[0].strip().startswith('@'):
            decorator = lines[0].strip()[1:].split('(')[0]
            if decorator in BUILTIN_DECORATORS:
                break
            lines.pop(0)

        dedented_func = reformat_python_source("\n".join(lines))
        return dedented_func
    except Exception as e:
        log.error(e)
        return ""

    
def flexible_import(obj_or_path):
    from .logs import log
    if isinstance(obj_or_path, str):
        parts = obj_or_path.split('.')
        current = ''
        obj = None

        for i, part in enumerate(parts):
            current += part
            try:
                obj = importlib.import_module(current)
                current += '.'
            except ImportError:
                if i == 0:
                    log.warning(f"Could not import module {current}")
                    return None
                try:
                    obj = getattr(obj, part)
                except AttributeError:
                    log.debug(f"Could not find {part} in {current}")
                    return None
        return obj
    else:
        return flexible_import(get_obj_addr(obj_or_path))
    
def is_function(obj):
    return isinstance(obj, (types.FunctionType, types.LambdaType)) or (not inspect.isclass(obj) and callable(obj))

def get_class_src(cls):
    lines = [f"class {cls.__name__}:"]
    
    # Add class attributes
    for name, value in cls.__dict__.items():
        if name.startswith('__') and name.endswith('__'):
            continue
        if not is_function(value) and not is_function(unwrap_func(value)):
            lines.append(f"    {name} = {repr(value)}")
    
    # Add an empty line if there were class attributes
    if len(lines) > 1:
        lines.append("")
    
    # Add methods
    for name, value in cls.__dict__.items():
        if is_function(value) or is_function(unwrap_func(value)):
            try:
                func_source = get_function_src(value)
                # Remove any leading whitespace and add proper indentation
                func_lines = func_source.split('\n')
                func_lines = ["    " + line if line.strip() else line for line in func_lines]
                lines.extend(func_lines)
                lines.append("")  # Add an empty line after each method
            except OSError:
                log.warning(f"Could not get source for method {name}")
    
    src = "\n".join(lines)
    out = reformat_python_source(src)

    return out


def reformat_python_source(src):
    lines = src.split('\n')
    if lines:
        leading_spaces = len(lines[0]) - len(lines[0].lstrip())
        return '\n'.join(line[leading_spaces:] for line in lines)
    else:
        return src
    
def get_obj_src(obj):
    if isinstance(obj, types.FunctionType):
        return get_function_src(obj)
    if isinstance(obj, type):
        return get_class_src(obj)
    return ""
    
def get_lambda_src(obj):
    try:
        source = inspect.getsource(obj)
        # Extract just the lambda part
        lambda_pattern = r'lambda.*?:.*?(?=\n|$)'
        match = re.search(lambda_pattern, source)
        if match:
            source = match.group(0)
        else:
            # Fallback if regex fails
            source = f"lambda {inspect.signature(obj)}: ..."
    except Exception:
        # Fallback for cases where we can't get the source
        source = f"lambda {inspect.signature(obj)}: ..."
    return source

def can_import_object(obj):
    try:
        return flexible_import(obj) is not None and get_obj_module(obj)!='__main__'
    except ImportError:
        return False

def get_file_addr():
    """
    Get the address of the current file module.
    
    Returns:
        str: The full address of the current file module.
    """
    frame = inspect.currentframe()
    try:
        # Get the frame of the caller (one level up)
        caller_frame = frame.f_back
        if caller_frame:
            # Get the filename of the caller
            filename = caller_frame.f_code.co_filename
            # Get the module name
            module = inspect.getmodulename(filename)
            if module:
                # Get the full module path
                module_path = inspect.getmodule(caller_frame).__name__
                return module_path
            else:
                # If we can't get the module name, return the filename
                return filename
    finally:
        del frame  # Avoid reference cycles


def get_pytype(obj):
    if is_classmethod(obj):
        return 'classmethod'
    elif is_instancemethod(obj):
        return 'instancemethod'
    elif is_method(obj):
        return 'method'
    elif is_function(obj):
        return 'function'
    elif isinstance(obj, type):
        return 'class'
    elif hasattr(obj,'__dict__'):
        return 'instance'
    else:
        return get_obj_module(obj)

def is_class(obj):
    return inspect.isclass(obj)

def is_classmethod(obj):
    # Check if it's a classmethod directly
    if isinstance(obj, classmethod):
        return True
    
    # Check if it's a function with __self__ attribute (bound method)
    if hasattr(obj, '__self__'):
        return inspect.isclass(obj.__self__)
    
    # Check if it's a descriptor (like classmethod)
    if hasattr(obj, '__get__'):
        # Create a dummy class and see if calling __get__ returns a bound method
        class Dummy: pass
        bound = obj.__get__(None, Dummy)
        if isinstance(bound, types.MethodType):
            return inspect.isclass(bound.__self__)
    
    # If it's a function, check its __qualname__
    if isinstance(obj, types.FunctionType):
        # Check if the function is defined inside a class
        if '.' in obj.__qualname__:
            class_name, method_name = obj.__qualname__.rsplit('.', 1)
            if class_name in obj.__globals__:
                cls = obj.__globals__[class_name]
                if inspect.isclass(cls):
                    # Check if this function is the same as the class attribute
                    class_attr = getattr(cls, method_name, None)
                    if class_attr is obj:
                        # It's a class method if it's identical to the class attribute
                        return True
    
    return False
def is_instancemethod(obj):
    return not is_classmethod(obj) and hasattr(obj,'__self__') and obj.__self__ is not None

def is_method(func):
    """
    Check if a function object is a method by inspecting its first parameter.
    
    :param func: The function object to check
    :return: True if the function is likely a method, False otherwise
    """
    if not is_classmethod(func):
        if is_instancemethod(func):
            return True
        try:
            params = inspect.signature(func).parameters
            return len(params) > 0 and list(params.keys())[0] == 'self'    
        except Exception:
            return False
    else:
        return False
    


def unwrap_func(func):
    from .logs import log
    wrapped = getattr(func,'__wrapped__',None)
    _func_ = getattr(func,'__func__',None)
    if _func_ or wrapped:
        return unwrap_func(_func_ or wrapped)
    else:
        return func


def get_class_from_method(method):
    if is_classmethod(method) and hasattr(method,'__self__') and method.__self__:
        return method.__self__
    else:
        class_name = method.__qualname__.rsplit('.', 1)[0]
        return unwrap_func(method).__globals__.get(class_name,None)
    
def get_object_from_method(method):
    if is_method(method) and hasattr(method,'__self__') and method.__self__ is not None:
        return method.__self__
    else:
        return None
    
def call_function_politely(func, *args, **kwargs):
    sig = inspect.signature(func)
    params = sig.parameters
    
    if any(param.kind == inspect.Parameter.VAR_KEYWORD for param in params.values()):
        # If there's a **kwargs parameter, pass all keyword arguments
        return func(*args, **kwargs)
    else:
        # Otherwise, filter the kwargs as before
        allowed_params = set(params.keys())
        filtered_kwargs = {k: v for k, v in kwargs.items() if k in allowed_params}
        return func(*args, **filtered_kwargs)