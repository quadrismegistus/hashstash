from . import *


## Logging setup
class ColoredFormatter(logging.Formatter):
    """Custom formatter with colors and module/function information."""
    
    COLORS = {
        'DEBUG': '\033[94m',    # Blue
        'INFO': '\033[92m',     # Green
        'WARNING': '\033[93m',  # Yellow
        'ERROR': '\033[91m',    # Red
        'CRITICAL': '\033[95m', # Magenta
        'RESET': '\033[0m',     # Reset
    }

    def format(self, record):
        log_fmt = f'{self.COLORS[record.levelname]}%(message)s{self.COLORS["RESET"]}'
        formatter = logging.Formatter(log_fmt, datefmt='%Y-%m-%d %H:%M:%S')
        return formatter.format(record)

def setup_logger(name, level=logging.INFO):
    """Function to setup a custom logger with color output."""
    logger = logging.getLogger(name)
    logger.setLevel(level)

    # Console Handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(ColoredFormatter())

    logger.addHandler(console_handler)
    return logger





# Setup the logger
logger = setup_logger('hashstash')
logger.setLevel(DEFAULT_LOG_LEVEL)



@contextmanager
def temporary_log_level(temp_level, only_sub=False):
    global logger
    original_level = logger.level
    if not only_sub or temp_level < original_level:
        logger.setLevel(temp_level)
    try:
        yield
    finally:
        logger.setLevel(original_level)

def get_function_call_str_l(_func, *args, **kwargs):
    from .addrs import get_obj_addr

    args = list(args)
    try:
        if args and getattr(_func, '__code__', None) and _func.__code__.co_varnames and _func.__code__.co_varnames[0] == 'self':
            args_str = ', '.join(map(repr, args[1:]))
        else:
            args_str = ', '.join(map(repr, args))
    except Exception:
        args_str = ', '.join(map(repr, args[1:]))
    def try_repr(x):
        try:
            return repr(x)
        except Exception as e:
            return get_obj_addr(x)
        
    kwargs_str = ', '.join(f'{k}={try_repr(v)}' for k, v in kwargs.items())
    params_str = ', '.join(filter(bool, [args_str, kwargs_str]))
    params_str = params_str.replace("\n", " ")
    return get_obj_addr(_func), params_str


def _cleanstr(x):
    x=str(x).replace("\n"," ")
    while '  ' in x: x=x.replace('  ',' ')
    return x.strip()

def get_function_call_str(func, *args, **kwargs):
    funcname, params_str = get_function_call_str_l(func,*args,**kwargs)
    params_str=params_str.replace("\n", " ")[:50].strip()
    return f'{funcname}({_cleanstr(params_str)})'

# Add a global variable to track the current depth
current_depth = 0
indenter = '    '
last_log_time = None

def log_wrapper(_func=None, level=logging.INFO):
    """Decorator to automatically log function calls with module and function name."""
    def decorator(func):

        @wraps(func)
        def wrapper(*args, **kwargs):
            global current_depth, last_log_time
            if level>=logger.level:
                funcname,params_str = get_function_call_str_l(func,*args,**kwargs)
                log_func(f'{funcname}(){"  <<<  "+params_str if params_str else ""}', level=level, incl_frame=False)
                current_depth += 1
            
            try:
                result = func(*args, **kwargs)
            except Exception as e:
                log.error(f"Error in {func.__name__}: {str(e)}")
                current_depth = 0
                raise e

            if level>=logger.level:
                current_depth -= 1
                # if result is not None: 
                if result is not None:
                    resx=repr(result).replace("\n", " ")
                    log_func(f'{funcname}()  >>>  {resx}', level=level, incl_frame=False)
                # log_func(f'>>> {str(result)[:100]}', level=level)

                if not current_depth:
                    last_log_time = None

            return result
        return wrapper
    
    if _func is None:
        return decorator
    return decorator(_func)

last_log_time = time.time()

def log_time_taken(reset=True):
    global last_log_time
    timenow = time.time()
    timetaken = timenow - (last_log_time if last_log_time else timenow)
    if reset: last_log_time = timenow
    return timetaken

def log_time_taken_str(reset=True):
    timetaken = log_time_taken(reset=reset)
    return f'[{timetaken:.2f}s] '# if timetaken > 0.1 else ''

def log_indent_str():
    global current_depth
    return indenter * current_depth

def log_prefix_str(message='', reset=True):
    return  f'{log_time_taken_str(reset=reset)}{log_indent_str()}{message}'


def log_func(*messages, level=logging.DEBUG, maxlen=None, incl_frame=True, incl_module=True, **kwargs):
    # Get caller information
    if logger.level > level: return
    message = ' '.join(str(x) for x in messages)
    caller_info = ''
    if incl_frame:
        frame = inspect.currentframe()
        while frame:
            if frame.f_code.co_filename not in {__file__,'__file__'}:
                caller_module = frame.f_globals['__name__']
                caller_func = frame.f_code.co_name
                caller_line = frame.f_lineno
                
                # Try to get the class name
                caller_class = None
                if 'self' in frame.f_locals:
                    caller_class = frame.f_locals['self'].__class__.__name__
                elif 'cls' in frame.f_locals:
                    caller_class = frame.f_locals['cls'].__name__
                
                if incl_module:
                    caller_info += f"{caller_module}."
                if caller_class:
                    caller_info += f"{caller_class}."
                caller_info += f"{caller_func}()"
                break
            frame = frame.f_back


        if caller_info:        
            message = f'{caller_info}: {message}'
    
    # Remove the pprint debug line
    logger.log(level, f"{log_prefix_str(message)}"[:maxlen])


class log:
    @staticmethod
    def log(_func=None, *args, level=logging.DEBUG, **kwargs):
        """Log a message or decorate a function with automatic logging."""
        if callable(_func):
            return log_wrapper(_func, level=level)
        else:
            log_func(_func, *args, level=level, **kwargs)

    @classmethod
    def debug(cls, _func=None, *args, **kwargs):
        return cls.log(_func, *args, level=logging.DEBUG, **kwargs)
    
    @classmethod
    def trace(cls, _func=None, *args, **kwargs):
        return cls.log(_func, *args, level=logging.DEBUG-1, **kwargs)
    
    @classmethod
    def info(cls, _func=None, *args, **kwargs):
        return cls.log(_func, *args, level=logging.INFO, **kwargs)
    
    @classmethod
    def warning(cls, _func=None, *args, **kwargs):
        return cls.log(_func, *args, level=logging.WARNING, **kwargs)
    warn = warning
    
    @classmethod
    def error(cls, _func=None, *args, **kwargs):
        return cls.log(_func, *args, level=logging.ERROR, **kwargs)
    
    @classmethod
    def critical(cls, _func=None, *args, **kwargs):
        return cls.log(_func, *args, level=logging.CRITICAL, **kwargs)

# if logger.level <= logging.DEBUG:
#     debug = debug
# else:
#     def debug_quiet(func_or_message):
#         if callable(func_or_message):
#             @wraps(func_or_message)
#             def wrapper(*args, **kwargs):
#                 return func_or_message(*args, **kwargs)
#             return wrapper
#         # Do nothing when called as a function
    
#     debug = debug_quiet



# @log.debug
# def ff(x):
#     log.debug('inner hello')
#     time.sleep(2)
#     return x*2

# from multiprocessing import Value

# _fn = Value('i', 0)
# def f(x):
#     global _fn
#     with _fn.get_lock():
#         _fn.value += 1
#     naptime=random.randint(1,3)
#     #print(f'startig process #{_fn.value}, sleeping for {naptime}')
#     for n in range(naptime):
#         #print('.',end='',flush=True)
#         time.sleep(1)
#     return x