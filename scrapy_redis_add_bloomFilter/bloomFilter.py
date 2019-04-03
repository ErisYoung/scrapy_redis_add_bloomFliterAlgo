"""
use bloom_fliter algorithm to reduce scrapy-redis's memory load
scrapy-redis:  1 hundred million fingerprint  ->  2GB
bloom-fliter:  1 hundred million fingerprint  ->  2^30b=128MB
"""

from redis import StrictRedis

BLOOMFILTER_BITS = 30
BLOOMFILTER_HASH_NUMBER = 6

class HashMap(object):
    def __init__(self, support_bits, seed):
        self.support_bits = support_bits
        self.seed = seed

    def hash(self, value):
        """
        hash value
        :param value: value
        :return: hashed value = offset
        """
        ret = 0
        for i in value:
            ret += self.seed * ret + ord(i)
        return (self.support_bits - 1) & ret


class BloomFiter(object):
    def __init__(self, redis_server, redis_key, bit=BLOOMFILTER_BITS, hash_number=BLOOMFILTER_HASH_NUMBER):
        """
        initialize BloomFilter
        :param redis_server:  redis-server
        :param redis_key:    key
        :param bit:   support_range = 2^bit
        :param hash_number:  default 6 ,the number of hash func
        """
        self.server = redis_server
        self.key = redis_key
        self.bit = bit
        self.hash_number = hash_number
        self.seeds = range(hash_number)
        self.m = 1 << self.bit
        self.maps = [HashMap(self.m, seed) for seed in self.seeds]

    def exists(self, value):
        """
        if value exists
        :param value:  value
        :return:
        """
        if not value:
            return False

        exists = 1
        for map in self.maps:
            offset = map.hash(value)
            exists = exists & self.server.getbit(self.key, offset)
        return exists

    def insert(self, value):
        """
        add value to redis_bloom
        :param value:
        :return:
        """
        for map in self.maps:
            offset = map.hash(value)
            self.server.setbit(self.key, offset, 1)


if __name__ == '__main__':
    conn = StrictRedis(host="localhost", port=6379)
    bf = BloomFiter(conn, 'testbf', 5, 6)
    bf.insert("hello")
    bf.insert("world")
    print(bool(bf.exists("hello")))
    print(bool(bf.exists("my friend")))
