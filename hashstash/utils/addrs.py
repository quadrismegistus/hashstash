from . import *
import re

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




def get_function_src(func):
    if func.__name__ == '<lambda>':
        return get_lambda_src(func)

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
        else:
            func_body = lines
        # Dedent the function body
        dedented_func = reformat_python_source("\n".join(func_body))
        return dedented_func
    except Exception as e:
        log.error(e)
        return ""



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
    


def get_class_src(cls):
    lines = [f"class {cls.__name__}:"]
    
    # Add class attributes
    for name, value in cls.__dict__.items():
        if name.startswith('__') and name.endswith('__'):
            continue
        if not inspect.isfunction(value):
            lines.append(f"    {name} = {repr(value)}")
    
    # Add an empty line if there were class attributes
    if len(lines) > 1:
        lines.append("")
    
    # Add methods
    for name, value in cls.__dict__.items():
        if inspect.isfunction(value):
            try:
                func_source = get_function_src(value)
                # Remove any leading whitespace and add proper indentation
                func_lines = func_source.split('\n')
                func_lines = ["    " + line if line.strip() else line for line in func_lines]
                lines.extend(func_lines)
                lines.append("")  # Add an empty line after each method
            except OSError:
                logger.warning(f"Could not get source for method {name}")
    
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
        print('LAMBDA SOURCE', source)
    except Exception:
        # Fallback for cases where we can't get the source
        source = f"lambda {inspect.signature(obj)}: ..."
    return source

def can_import_object(obj):
    return flexible_import(obj) is not None

