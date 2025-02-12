import random
import redis
import time


class RateLimitExceed(Exception):
    pass


class RateLimiter:
    def __init__(self, redis_host="localhost", redis_port=6379, redis_db=0):
        self.redis = redis.Redis(host=redis_host, port=redis_port, db=redis_db)
        self.key = "rate_limiter:requests"
        self.limit = 5
        self.window = 3

    def test(self) -> bool:
        current_time = time.time()

        self.redis.ltrim(self.key, 0, self.limit - 1)
        self.redis.ltrim(self.key, 0, self.limit - 1)

        self.redis.lpush(self.key, current_time)

        self.redis.expire(self.key, self.window)

        request_count = self.redis.llen(self.key)

        if request_count > self.limit:
            return False
        else:
            return True


def make_api_request(rate_limiter: RateLimiter):
    if not rate_limiter.test():
        raise RateLimitExceed
    else:
        # какая-то бизнес логика
        pass


if __name__ == "__main__":
    rate_limiter = RateLimiter()

    for _ in range(50):
        time.sleep(random.randint(1, 2))

        try:
            make_api_request(rate_limiter)
        except RateLimitExceed:
            print("Rate limit exceed!")
        else:
            print("All good")
