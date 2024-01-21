from redisolar.dao.base import CapacityDaoBase
from redisolar.dao.redis.base import RedisDaoBase
from redisolar.models import CapacityReport
from redisolar.models import MeterReading
from redisolar.models import SiteCapacityTuple


class CapacityReportDaoRedis(CapacityDaoBase, RedisDaoBase):
    """Persists and queries CapacityReports in Redis."""

    def update(self, meter_reading: MeterReading, **kwargs) -> None:
        # Uses the Redis client instance in self.redis by default.  If a
        # Pipeline object was provided, uses that instead -- Pipeline objects
        # offer the same Redis command API as normal Redis clients.
        client = kwargs.get("pipeline", self.redis)
        capacity_ranking_key = self.key_schema.capacity_ranking_key()
        # member(site_id): score(capacity)
        # sorted set can have different member with the same score
        # members with same score will be sorted based on the member name
        # eg. apple: 1.0 will come before banana: 1.0
        report = {meter_reading.site_id: meter_reading.current_capacity}
        client.zadd(capacity_ranking_key, report)

    def get_report(self, limit: int, **kwargs) -> CapacityReport:
        capacity_ranking_key = self.key_schema.capacity_ranking_key()
        p = self.redis.pipeline()
        p.zrange(capacity_ranking_key, 0, limit - 1, withscores=True)
        p.zrevrange(capacity_ranking_key, 0, limit - 1, withscores=True)
        low_capacity, high_capacity = p.execute()

        low_capacity_list = [
            SiteCapacityTuple(site_id=v[0], capacity=v[1]) for v in low_capacity
        ]
        high_capacity_list = [
            SiteCapacityTuple(site_id=v[0], capacity=v[1]) for v in high_capacity
        ]

        return CapacityReport(high_capacity_list, low_capacity_list)

    def get_rank(self, site_id: int, **kwargs) -> float:
        # START Challenge #4
        # Remove the following line after you have added code to
        # get the real rank.
        capacity_ranking_key = self.key_schema.capacity_ranking_key()
        # highest capacity returns 0
        rank = self.redis.zrevrank(capacity_ranking_key, site_id)

        return rank
        # END Challenge #4
