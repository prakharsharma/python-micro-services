"""
Module provides utility class to call a function on a service
"""

import Queue
import uuid

from core.resourcepool import ResourcePool

from core.client import ServiceClient, DEFAULT_TIME_OUT, \
    DEFAULT_MAX_TRIES, DEFAULT_SLEEP_BEFORE_RETRY
from core.redis_service_registry import RedisServiceRegistry


RESOURCE_ACQUIRING_TIMEOUT = 2


class UnknownServiceError(RuntimeError):
    pass


class ClientResourceNotAvailableError(RuntimeError):
    pass


class ServiceClientResource(object):

    def __init__(self, service_name, service_config, logger=None):
        self.service = service_name
        self.client = ServiceClient(service_name,
                                    service_config=service_config,
                                    logger=logger,
                                    start_heartbeat_thread=False,
                                    timeout=-1)

    def __repr__(self):
        return "ServiceClientResource(service=%s, client=%r, alive=%s)" % (
            self.service, self.client, self.good_to_use())

    def good_to_use(self):
        return self.client.alive


class ServiceMethodCaller(object):

    """
    This class maintains a pool of service clients.
    """

    DEFAULT_POOL_SIZE = 5
    CLIENTS_PER_SERVICE_CONFIG = 5
    MOCK = False  # this is for tests

    def __init__(self, service_registry_redis_config, services,
                 logger=None):

        self._registry_redis_config = service_registry_redis_config
        self._registry = RedisServiceRegistry(**(self._registry_redis_config
                                                 or {}))
        self.logger = logger

        if self.MOCK:
            return

        self._managed_services = {}
        for service in services:
            if isinstance(service, tuple) or isinstance(service, list):
                service_name = service[0]
                try:
                    pool_size = int(service[1])
                except IndexError:
                    pool_size = self.DEFAULT_POOL_SIZE
            else:
                service_name = service
                pool_size = self.DEFAULT_POOL_SIZE
            self._managed_services[service_name] = [pool_size]

        for service_name, value in self._managed_services.items():
            self._managed_services[service_name].append(
                self._create_service_pool(service_name, value[0])
            )

        self.log('debug', 'created service method caller')

    def _create_service_pool(self, service_name, pool_size):
        resources = []
        service_configs = self._registry.discover_service(service_name,
                                                          num=pool_size)
        for config in service_configs:
            for i in range(self.CLIENTS_PER_SERVICE_CONFIG):
                self.log('debug', 'creating %d client resource for service '
                                  'config: %s' % (i + 1, config))
                resource = ServiceClientResource(service_name, config,
                                                 self.logger)
                resources.append(resource)

        self.log('debug', 'created a pool of clients: %r' % resources)
        return ResourcePool(resources)

    def log(self, level, message):
        try:
            if not hasattr(self, 'logger'):
                return
            logger = self.logger
            if logger is None:
                return
            if not hasattr(logger, level):
                return
            logger_method = getattr(logger, level)
            if not logger_method:
                return
            logger_method(message)
        except:
            pass

    def __call__(self, method, service, request, response_class=None,
                 timeout=DEFAULT_TIME_OUT, max_tries=DEFAULT_MAX_TRIES,
                 sleep_before_retry=DEFAULT_SLEEP_BEFORE_RETRY):
        """
        calls function 'method' on service 'service'

        :param method:
        :param service:
        :param request:
        :param response_class:
        :return:
        """

        if self.MOCK:
            return

        if service not in self._managed_services:
            raise UnknownServiceError('service: %s unknown' % service)

        try:
            pool = self._managed_services[service][1]
            with pool.acquire(timeout=RESOURCE_ACQUIRING_TIMEOUT) as resource:
                client = resource.client
                self.log('debug', 'using client: %r' % client)
                if hasattr(request, 'SerializeToString'):
                    request.header.request_guid = str(uuid.uuid4())
                    self.log('info', 'calling %s method on %s service '
                                     'with request guid: %s' %
                             (method, service,
                              request.header.request_guid))
                    request_message = request.SerializeToString()
                else:
                    request_message = str(request)
                response = client.request(method, request_message,
                                          response_class=response_class,
                                          timeout=timeout, max_tries=max_tries,
                                          sleep_before_retry=sleep_before_retry)
                if hasattr(request, 'SerializeToString'):
                    response_type = 'good' if response.header.success else 'bad'
                    self.log('info', 'received %s response for %s method from %s '
                                     'service for request guid: %s in %s '
                                     'microseconds' %
                             (response_type, method, service,
                              response.header.request_guid,
                              response.header.response_time))
                else:
                    self.log('info', 'received response: %s' % response)
                return response
        except Queue.Empty:
            self.log('error', 'no client to call method: %s on service: %s '
                     % (method, service))
            raise ClientResourceNotAvailableError()
        except Exception as exception:
            import traceback
            self.log('error', 'Error while calling method: %s on '
                              'service: %s. traceback: %s' %
                     (method, service, traceback.format_exc()))
            raise exception

