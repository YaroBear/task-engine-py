from functools import wraps
import time


class RateLimit:
    """
    Rate limit configuration. rate is how quickly tokens are added back to the bucket, in rates per second. 
    Capacity is the maximum number of tokens that can be stored.
    """
    def __init__(self, key: str, capacity: int, rate: float):
        self.key = key
        self.capacity = capacity
        self.rate = rate


class TokenBucket:
    def __init__(self, rate, capacity):
        self.rate = rate
        self.capacity = capacity
        self.tokens = capacity
        self.last_refill = time.time()

    def get_tokens(self):
        now = time.time()
        elapsed = now - self.last_refill
        self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)
        self.last_refill = now
        return self.tokens

    def consume(self, tokens):
        if self.get_tokens() >= tokens:
            self.tokens -= tokens
            return True
        return False


class RateLimiter:
    def __init__(self, rate_limits):
        self.token_buckets = {}
        self.initialize_rate_limiters(rate_limits)

    def initialize_rate_limiters(self, rate_limits: list[RateLimit]):
        for rate_limit in rate_limits:
            self.token_buckets[rate_limit.key] = TokenBucket(rate=rate_limit.rate, capacity=rate_limit.capacity)

    def get_token_bucket(self, key):
        return self.token_buckets.get(key)


rate_limiter = None


def set_rate_limiter(limiter):
    global rate_limiter
    rate_limiter = limiter


def rate_limit(key):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            task = args[0]
            limiter = rate_limiter.get_token_bucket(key)
            if limiter:
                while not limiter.consume(1):
                    print(f"\033[38;2;255;165;0mRate limiting {task.__class__.__name__}\033[0m")
                    time.sleep(0.1)  # Wait for tokens to become available
            return func(*args, **kwargs)
        return wrapper
    return decorator
