from engine import Task, retries, PipelineExecutor, register
from engine.rate_limiter import RateLimit, RateLimiter, rate_limit, set_rate_limiter


@register()
class TakeFromTheCookieJar(Task):
    attempts = 0

    @retries(10)
    @rate_limit(key="take_cookie")
    def perform_task(self):
        self.attempts += 1
        if self.attempts == 10:
            print("Took 10 cookies from the cookie jar")
            return
        print("Taking a cookie from the cookie jar")
        raise Exception()

    def retry_handler(self, error):
        return True


if __name__ == "__main__":
    take_cookie_limiter = RateLimit(key="take_cookie", capacity=1, rate=1.0)

    set_rate_limiter(RateLimiter([take_cookie_limiter]))

    executor = PipelineExecutor()
    executor.run()
