import time
import tracemalloc
from functools import wraps

from src.logger_config import *

def profile(fn):
    @wraps(fn)
    def inner(*args, **kwargs):
        fn_kwargs_str = ', '.join(f'{k}={v}' for k, v in kwargs.items())
        logging.info(f'\n{fn.__name__}()')

        t = time.perf_counter()
        tracemalloc.start()

        retval = fn(*args, **kwargs)

        elapsed = time.perf_counter() - t
        logging.info(f'Time   {elapsed:0.4f}')
        current, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        logging.info(f'Memory {peak / 1024:.2f} KB')
        return retval
    return inner