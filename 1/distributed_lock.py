import redis
from functools import wraps
from datetime import timedelta


class SingleDecoratorException(Exception):
    pass


def single(max_processing_time: timedelta = None):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            r = redis.Redis(host="localhost", port=6379, db=0)

            lock_key = f"lock_{func.__name__}"

            acquired_lock = r.set(
                lock_key, "locked", nx=True, ex=max_processing_time.total_seconds()
            )

            if not acquired_lock:
                raise SingleDecoratorException(
                    f"Функция {func.__name__} уже выполняется."
                )

            try:
                return func(*args, **kwargs)
            finally:
                r.delete(lock_key)

        return wrapper

    return decorator
