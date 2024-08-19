from functools import partial, wraps
import logging
import time
import sys
from contextlib import contextmanager

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
        log_fmt = f'%(asctime)s  {self.COLORS[record.levelname]}%(message)s{self.COLORS["RESET"]}'
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
logger.setLevel('WARN')



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


def log(_func=None, level=logging.INFO):
    """Decorator to automatically log function calls with module and function name."""
    def decorator(func):

        @wraps(func)
        def wrapper(*args, **kwargs):
            # with temporary_log_level(level, only_sub=True):
            args_str = ', '.join(map(str, args))
            kwargs_str = ', '.join(f'{k}={v!r}' for k, v in kwargs.items())
            params_str = ', '.join(filter(bool, [args_str, kwargs_str]))
            
            msg1 = f'{func.__name__}({params_str})'
            timenow=time.time()
            # logger.log(level, msg1)
            result = func(*args, **kwargs)
    
            logger.log(level, f'{msg1}\n>>> {result}\n')
            return result
        return wrapper
    
    if _func is None:
        return decorator
    return decorator(_func)

# debug = log
#debug = partial(log, level=logging.DEBUG)

def debug_quiet(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        return func(*args, **kwargs)
    return wrapper

debug = debug_quiet