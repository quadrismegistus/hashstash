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


# Add a global variable to track the current depth
current_depth = 0
indenter = ' | '
last_log_time = None

def log_wrapper(_func=None, level=logging.INFO):
    """Decorator to automatically log function calls with module and function name."""
    def decorator(func):

        @wraps(func)
        def wrapper(*args, **kwargs):
            global current_depth, last_log_time
            args = list(args)
            try:
                if args and getattr(func, '__code__', None) and func.__code__.co_varnames and func.__code__.co_varnames[0] == 'self':
                    args_str = ', '.join(map(repr, args[1:]))
                else:
                    args_str = ', '.join(map(repr, args))
            except Exception:
                args_str = ', '.join(map(repr, args[1:]))
            kwargs_str = ', '.join(f'{k}={v!r}' for k, v in kwargs.items() if v is not None)
            params_str = ', '.join(filter(bool, [args_str, kwargs_str]))
            if level>=logger.level:
                log_func(f'{get_obj_nice_name(func)}  <<<  ({params_str.replace("\n", " ")})', level=level)
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
                log_func(f'{get_obj_nice_name(func)}  >>>  {repr(result).replace("\n", " ")}', level=level)
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
    return f'[{timetaken:.2f}s]'

def log_indent_str():
    global current_depth
    return indenter * current_depth

def log_prefix_str(message='', reset=True):
    return  f'{log_time_taken_str(reset=reset)}{log_indent_str()}{" "+message if message else ""}'


def log_func(message, level=logging.DEBUG, maxlen=None):
    logger.log(level,log_prefix_str(message)[:maxlen])


class log:
    @staticmethod
    def log(_func=None, level=logging.DEBUG):
        """Decorator to automatically log function calls with module and function name."""
        if callable(_func):
            return log_wrapper(_func, level=level)
        else:
            log_func(_func)

    debug = partial(log, level=logging.DEBUG)
    info = partial(log, level=logging.INFO)
    warn = partial(log, level=logging.WARNING)
    warning = partial(log, level=logging.WARNING)
    error = partial(log, level=logging.ERROR)

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



@log.debug
def ff(x):
    log.debug('inner hello')
    time.sleep(2)
    return x*2

@log.debug
def f(x):
    def fff(y):
        log.debug('inner inner hello')
        time.sleep(1)
        return y*2
    log.debug('outer hello')
    # time.sleep(1)
    from hashstash import serialize
    return serialize(fff, 'jsonpickle_ext')
