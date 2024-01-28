from typing import Dict
from typing import List
from typing import Set

from redis.client import Pipeline
from redisolar.dao.base import SiteGeoDaoBase
from redisolar.dao.base import SiteNotFound
from redisolar.dao.redis.base import RedisDaoBase
from redisolar.models import GeoQuery
from redisolar.models import Site
from redisolar.schema import FlatSiteSchema

CAPACITY_THRESHOLD = 0.2


class SiteGeoDaoRedis(SiteGeoDaoBase, RedisDaoBase):
    """SiteGeoDaoRedis persists and queries Sites in Redis."""

    def insert(self, site: Site, **kwargs):
        """Insert a Site into Redis."""
        hash_key = self.key_schema.site_hash_key(site.id)
        client: Pipeline = kwargs.get("pipeline", self.redis)
        client.hset(hash_key, mapping=FlatSiteSchema().dump(site))  # type: ignore

        if not site.coordinate:
            raise ValueError("Site coordinates are required for Geo insert")

        client.geoadd(  # type: ignore
            self.key_schema.site_geo_key(),
            site.coordinate.lng,
            site.coordinate.lat,
            site.id,
        )

    def insert_many(self, *sites: Site, **kwargs) -> None:
        """Insert multiple Sites into Redis."""
        for site in sites:
            self.insert(site, **kwargs)

    def find_by_id(self, site_id: int, **kwargs) -> Site:
        """Find a Site by ID in Redis."""
        hash_key = self.key_schema.site_hash_key(site_id)
        site_hash = self.redis.hgetall(hash_key)

        if not site_hash:
            raise SiteNotFound()

        return FlatSiteSchema().load(site_hash)

    def _find_by_geo(self, query: GeoQuery, **kwargs) -> Set[Site]:
        site_ids = self.redis.georadius(  # type: ignore
            self.key_schema.site_geo_key(),
            query.coordinate.lng,
            query.coordinate.lat,
            query.radius,
            query.radius_unit.value,
        )
        sites = [
            self.redis.hgetall(self.key_schema.site_hash_key(site_id))
            for site_id in site_ids
        ]
        return {FlatSiteSchema().load(site) for site in sites}

    def _find_by_geo_with_capacity(self, query: GeoQuery, **kwargs) -> Set[Site]:
        # START Challenge #5
        # Your task: Get the sites matching the GEO query.
        site_ids: List[str] = self.redis.georadius(
            name=self.key_schema.site_geo_key(),
            longitude=query.coordinate.lng,
            latitude=query.coordinate.lat,
            radius=query.radius,
            unit=query.radius_unit.value,
        )
        # END Challenge #5

        # START Challenge #5
        #
        # Your task: Populate a dictionary called "scores" whose keys are site
        # IDs and whose values are the site's capacity.
        #
        # Make sure to run any Redis commands against a Pipeline object
        # for better performance.
        p = self.redis.pipeline(transaction=False)
        for site_id in site_ids:
            capacity_ranking_key = self.key_schema.capacity_ranking_key()
            # CapacityReportDaoRedis.update() add the site_id (member) and capacity (score) to the sorted set
            p.zscore(name=capacity_ranking_key, value=site_id)
        capacity_list = p.execute()

        scores: Dict[str, float] = dict(zip(site_ids, capacity_list))
        # END Challenge #5

        # Delete the next lines after you've populated a `site_ids`
        # and `scores` variable.
        # site_ids: List[str] = []
        # scores: Dict[str, float] = {}

        for site_id in site_ids:
            if scores[site_id] and scores[site_id] > CAPACITY_THRESHOLD:
                p.hgetall(self.key_schema.site_hash_key(site_id))
        site_hashes = p.execute()

        return {FlatSiteSchema().load(site) for site in site_hashes}

    def find_by_geo(self, query: GeoQuery, **kwargs) -> Set[Site]:
        """Find Sites using a geographic query."""
        if query.only_excess_capacity:
            return self._find_by_geo_with_capacity(query)
        return self._find_by_geo(query)

    def find_all(self, **kwargs) -> Set[Site]:
        """Find all Sites."""
        site_ids = self.redis.zrange(self.key_schema.site_geo_key(), 0, -1)
        sites = set()

        # transaction=True (default) is used when we want to run multiple commands
        # without other clients writing to the same keys in between.
        # But, Redis doesn't rollback the transaction if one of the commands fails.
        # It just continues executing the rest of the commands.
        # -> Transaction in Redis can be misleading since it guarantees isolation, but not atomicity.
        p = self.redis.pipeline(transaction=False)

        for site_id in site_ids:
            key = self.key_schema.site_hash_key(site_id)
            p.hgetall(key)
        site_hashes = p.execute()

        # hgetall can return None if the key(sites:info:site_id) doesn't exist
        site_hashes = [site_hash for site_hash in site_hashes if site_hash]

        for site_hash in site_hashes:
            sites.add(FlatSiteSchema().load(site_hash))

        return sites
