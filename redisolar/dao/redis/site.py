from typing import Set

from redisolar.models import Site
from redisolar.dao.base import SiteDaoBase
from redisolar.dao.base import SiteNotFound
from redisolar.dao.redis.base import RedisDaoBase
from redisolar.schema import FlatSiteSchema


class SiteDaoRedis(SiteDaoBase, RedisDaoBase):
    """SiteDaoRedis persists Site models to Redis.

    This class allows persisting (and querying for) Sites in Redis.
    """

    def insert(self, site: Site, **kwargs):
        """Insert a Site into Redis."""
        # get hash key f"sites:info:{site_id}"
        hash_key = self.key_schema.site_hash_key(site.id)
        # get set key "sites:ids"
        site_ids_key = self.key_schema.site_ids_key()

        # pipeline is similar to Django's query object, avoiding multiple round trips to Redis
        # transaction is set to False to allow multiple commands to be executed
        client = kwargs.get("pipeline", self.redis)

        # Serializes the Site to a flat dictionary since Redis Hashes can't
        # contain nested values. (nested coord -> lat, lng)
        # , and upserts the dict into Redis.
        client.hset(hash_key, mapping=FlatSiteSchema().dump(site))

        # Adds the Site ID to the set of all Site IDs.
        client.sadd(site_ids_key, site.id)

    def insert_many(self, *sites: Site, **kwargs) -> None:
        for site in sites:
            self.insert(site, **kwargs)

    def find_by_id(self, site_id: int, **kwargs) -> Site:
        """Find a Site by ID in Redis."""
        hash_key = self.key_schema.site_hash_key(site_id)
        site_hash = self.redis.hgetall(hash_key)

        if not site_hash:
            raise SiteNotFound()

        return FlatSiteSchema().load(site_hash)

    def find_all(self, **kwargs) -> Set[Site]:
        """Find all Sites in Redis."""
        # START Challenge #1

        # get the IDs of all of the sites from that set. Then, for each site ID, get its hash and add the hash to the site_hashes variable.
        site_ids_key = self.key_schema.site_ids_key()
        site_ids = self.redis.smembers(site_ids_key)
        site_hashes = [
            self.redis.hgetall(self.key_schema.site_hash_key(site_id))
            for site_id in site_ids
        ]
        # END Challenge #1

        return {FlatSiteSchema().load(site_hash) for site_hash in site_hashes}
