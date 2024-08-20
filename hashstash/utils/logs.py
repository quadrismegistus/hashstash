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
indenter = '|  '
last_log_time = None

def log_wrapper(_func=None, level=logging.INFO):
    """Decorator to automatically log function calls with module and function name."""
    def decorator(func):

        @wraps(func)
        def wrapper(*args, **kwargs):
            global current_depth, last_log_time
            args = list(args)
            try:
                args_str = ', '.join(map(repr, args))
            except Exception:
                args_str = ', '.join(map(repr, args[1:]))
            kwargs_str = ', '.join(f'{k}={v!r}' for k, v in kwargs.items())
            params_str = ', '.join(filter(bool, [args_str, kwargs_str]))
            
            log_func(f'{get_obj_addr(func)}({params_str[:100]}) >>>', level=level)

            current_depth += 1
            result = func(*args, **kwargs)
            current_depth -= 1
            log_func(f'>>> {str(result)[:100]}', level=level)

            if not current_depth:
                last_log_time = None

            return result
        return wrapper
    
    if _func is None:
        return decorator
    return decorator(_func)


def log_func(message, level=logging.DEBUG):
    global current_depth, last_log_time
    indent = indenter * current_depth
    timenow = time.time()
    timetaken = timenow - (last_log_time if last_log_time else timenow)
    last_log_time = timenow

    logger.log(level, f'[{timetaken:.2f}s] {indent}{message}')


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
    log.debug('outer hello')
    time.sleep(1)
    return ff(x)*2
