from functools import wraps
import time


def skippable(func):
    """Decorator to skip a task if it fails. Does not stop the pipeline."""
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            print(f"Skipping task due to error: {e}")
    return wrapper


def retries(max_retries):
    """Decorator to retry a task a maximum number of times. retry_handler must return True to retry."""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(1, max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    task = args[0]
                    if not task.retry_handler(e) or attempt == max_retries:
                        raise e
        return wrapper
    return decorator
