import time
import tracemalloc
from functools import wraps
import logging


def profile(fn):
    @wraps(fn)
    def inner(*args, **kwargs):
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
