# Uncomment for Challenge #7
import datetime
import random
from redis.client import Redis

from redisolar.dao.base import RateLimiterDaoBase
from redisolar.dao.redis.base import RedisDaoBase
from redisolar.dao.redis.key_schema import KeySchema

# Uncomment for Challenge #7
from redisolar.dao.base import RateLimitExceededException


class SlidingWindowRateLimiter(RateLimiterDaoBase, RedisDaoBase):
    """A sliding-window rate-limiter."""

    def __init__(
        self,
        window_size_ms: float,
        max_hits: int,
        redis_client: Redis,
        key_schema: KeySchema = None,
        **kwargs
    ):
        self.window_size_ms = window_size_ms
        self.max_hits = max_hits
        super().__init__(redis_client, key_schema, **kwargs)

    def hit(self, name: str):
        """Record a hit using the rate-limiter."""
        # START Challenge #7

        # Add an unique entry to the sorted set
        # eg. [timestamp]-[ip-address] or [timestamp]-[random-number]
        key = self.key_schema.sliding_window_rate_limiter_key(
            name=name,
            window_size_ms=self.window_size_ms,
            max_hits=self.max_hits,
        )

        # timestamp() returns the current time in "seconds" since the Epoch
        now = datetime.datetime.now().timestamp() * 1000
        p = self.redis.pipeline()
        # zadd map is member: score
        p.zadd(name=key, mapping={random.random(): now})

        # Remove all entries that are older than CURRENT_TIMESTAMP - WINDOW_SIZE
        p.zremrangebyscore(
            name=key,
            min=0,
            max=(now - self.window_size_ms),
        )
        # Count the elements in the sorted set
        p.zcard(key)

        # pipeline.execute() returns a list of the results of each command in the pipeline
        _, _, hit_count = p.execute()

        if hit_count > self.max_hits:
            raise RateLimitExceededException

        # END Challenge #7
