"""
Module provides Redis backed service registry
"""

import json
import random
import redis

from error import ServiceNotAvailableError, \
    UnknownSocketTypeError, ServiceRegistrationError


class RedisServiceRegistry(object):
    """
    Redis backed service registry
    Provides methods for service registry, discovery and deregistry
    """

    DEFAULT_REDIS_CONFIG = {
        "host": '127.0.0.1',
        "port": 6379,
        "db": 9
    }
    STARTING_PORT = 9000
    PORT_BATCH_SIZE = 100
    MANDATORY_FIELDS = ['name', 'host', 'port', 'guid', 'functions',
                        'socket_type', 'connect_method']

    def __init__(self, **kwargs):

        cfg = {}
        for f in ["host", "port", "db"]:
            if f not in kwargs:
                cfg[f] = self.DEFAULT_REDIS_CONFIG[f]
            else:
                cfg[f] = kwargs[f]
        self._redis = redis.StrictRedis(**cfg)
        self._next_avaialble_port = None

    def register_service(self, service_map):

        for field in self.MANDATORY_FIELDS:
            if field not in service_map:
                raise ServiceRegistrationError('mandatory field: %s not '
                                               'provided' % field)

        services_key = RedisServiceRegistryKeys.services_key()
        service_guids_key = \
            RedisServiceRegistryKeys.service_guids_key(service_map['name'])
        service_instance_key = RedisServiceRegistryKeys.service_instance_key(
            service_map['name'], service_map['guid']
        )

        def _register_service(pipe):
            pipe.multi()
            pipe.sadd(services_key, service_map['name'])
            pipe.sadd(service_guids_key, service_map['guid'])
            pipe.hmset(service_instance_key, service_map)

        self._redis.transaction(_register_service, services_key,
                                service_guids_key, service_instance_key)

    def next_available_port(self, service_name, service_guid, host):
        """

        :param service_name
        :param service_guid
        :param host:
        :return: next port available for a service in host
        """

        host_ports_key = RedisServiceRegistryKeys.host_ports_key(host)

        def _next_available_port(pipe):
            count = pipe.zcard(host_ports_key)
            if count == 0:
                ports = []
                for p in xrange(self.STARTING_PORT, self.STARTING_PORT +
                                self.PORT_BATCH_SIZE):
                    ports.extend([p, p])
                pipe.zadd(host_ports_key, *ports)
            elif count == 1:
                ports = []
                starting_port = int(pipe.zrange(host_ports_key, 0, 0)[0])
                for p in xrange(starting_port + 1, starting_port + 1 +
                                self.PORT_BATCH_SIZE):
                    ports.extend([p, p])
                pipe.zadd(host_ports_key, *ports)

            starting_port = pipe.zrange(host_ports_key, 0, 0)[0]
            self._next_avaialble_port = int(starting_port)
            pipe.multi()
            pipe.zrem(host_ports_key, starting_port)

        self._redis.transaction(_next_available_port, host_ports_key)
        return self._next_avaialble_port

    def deregister_service(self, service_name, service_guid, host):
        """

        :param service_name:
        :param service_guid:
        :param host:
        :return: None
        """

        services_key = RedisServiceRegistryKeys.services_key()
        service_guids_key = \
            RedisServiceRegistryKeys.service_guids_key(service_name)
        service_instance_key = RedisServiceRegistryKeys.service_instance_key(
            service_name, service_guid)
        host_ports_key = RedisServiceRegistryKeys.host_ports_key(host)

        def _deregister_service(pipe):
            services_card = pipe.scard(services_key)
            service_guids_card = pipe.scard(service_guids_key)
            service_instance = pipe.hgetall(service_instance_key)
            try:
                port = service_instance['port']
            except KeyError:
                port = None
            pipe.multi()
            pipe.delete(service_instance_key)
            pipe.srem(service_guids_key, service_guid)
            if service_guids_card == 1:
                pipe.delete(service_guids_key)
                pipe.srem(services_key, service_name)
                if services_card == 1:
                    pipe.delete(services_key)
            if port is not None:
                pipe.zadd(host_ports_key, port, port)

        self._redis.transaction(_deregister_service,
                                services_key,
                                service_guids_key,
                                service_instance_key,
                                host_ports_key)

    def discover_service(self, service_name, num=1):

        services_key = RedisServiceRegistryKeys.services_key()
        if not self._redis.sismember(services_key, service_name):
            raise ServiceNotAvailableError("service: %s not available" %
                                           service_name)

        self.sample_guids = []
        self.configs = []

        service_guids_key = RedisServiceRegistryKeys.service_guids_key(
            service_name)
        available_guids = [x for x in self._redis.smembers(service_guids_key)]
        guid = available_guids[random.randint(0, len(available_guids) - 1)]
        service_instance_key = RedisServiceRegistryKeys.service_instance_key(
            service_name, guid)

        def _get_random_sample(pipe):
            available_guids = [x for x in pipe.smembers(service_guids_key)]
            if len(available_guids) > num:
                self.sample_guids = random.sample(available_guids, num)
            else:
                self.sample_guids = available_guids
            pipe.multi()

        self._redis.transaction(_get_random_sample, service_guids_key)

        watched_keys = []
        for guid in self.sample_guids:
            watched_keys.append(
                RedisServiceRegistryKeys.service_instance_key(service_name,
                                                              guid)
            )

        def _get_configs(pipe):
            for guid in self.sample_guids:
                service_instance_key = \
                    RedisServiceRegistryKeys.service_instance_key(
                        service_name, guid)
                self.configs.append(pipe.hgetall(service_instance_key))
            pipe.multi()

        self._redis.transaction(_get_configs, *watched_keys)

        for config in self.configs:
            config["socket_type"] = self.client_socket_type(config[
                "socket_type"])
            config["connect_method"] = "connect" \
                if config["connect_method"] == "bind" else "bind"
            config['functions'] = set(json.loads(config['functions']))
            for f in ['port', 'pid', 'start_time', 'alive']:
                if f in config:
                    config[f] = json.loads(config[f])

        return self.configs

    @classmethod
    def client_socket_type(cls, socket_type):
        if socket_type == "REP":
            return "REQ"
        elif socket_type == "REQ":
            return "REP"
        elif socket_type == "PUB":
            return "SUB"
        elif socket_type == "SUB":
            return "PUB"
        elif socket_type == "PUSH":
            return "PULL"
        elif socket_type == "PULL":
            return "PUSH"
        raise UnknownSocketTypeError("No corresponding socket pair for socket"
                                     "type: %s" % socket_type)


class RedisServiceRegistryKeys(object):
    """
    provides static methods to provide registry keys
    """

    STRING_KEY_PREFIX = 'st'
    LIST_KEY_PREFIX = 'li'
    HMAP_KEY_PREFIX = 'hm'
    SET_KEY_PREFIX = 'se'
    ZSET_KEY_PREFIX = 'zs'

    @classmethod
    def services_key(cls):
        """

        :return: key for Redis SET of available services
        """

        return "%s:s" % cls.SET_KEY_PREFIX

    @classmethod
    def service_guids_key(cls, service_name):
        """

        :param service_name:
        :return: key for SET of available providers of a service
        """

        return "%s:s:%s:g" % (cls.SET_KEY_PREFIX, service_name)

    @classmethod
    def service_instance_key(cls, service_name, service_guid):
        """

        :param service_name:
        :param service_guid:
        :return: key for HMAP describing a particular provider of a service
        """
        return "%s:s:%s:g:%s" % (cls.HMAP_KEY_PREFIX, service_name,
                                 service_guid)

    @classmethod
    def host_ports_key(cls, host):
        """

        :param host:
        :return: key for ZSET that keeps track of available ports for a host
        """

        return "%s:h:%s:p" % (cls.ZSET_KEY_PREFIX, host)

