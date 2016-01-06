"""
Module provides base class for a service client
"""

import time
import threading
import uuid
import zmq

from utils import zmq_socket_from_socket_type, \
    current_timestamp

from redis_service_registry import \
    RedisServiceRegistry
from error import ServiceFunctionNotAvailableError


DEFAULT_MAX_TRIES = 3
DEFAULT_TIME_OUT = 5 * 1000  # 5 seconds in milliseconds
DEFAULT_SLEEP_BEFORE_RETRY = 3 * 1000  # 3 seconds in milliseconds
DEFAULT_HEARTBEAT_FREQUENCY = 30 * 1000  # in milliseconds


def socket_from_service_config(context, service_config,
                               timeout=DEFAULT_TIME_OUT):
    socket = zmq_socket_from_socket_type(context, service_config["socket_type"])
    connect_string = "tcp://%s:%d" % \
                     ("*"
                     if service_config["connect_method"] == "bind"
                     else service_config["host"],
                      service_config["port"])
    getattr(socket, service_config["connect_method"])(connect_string)
    socket.setsockopt(zmq.RCVTIMEO, timeout)
    socket.setsockopt(zmq.LINGER, 0)
    return socket


class ServiceClientError(RuntimeError):
    pass


class ServiceClientTimeoutError(ServiceClientError):
    pass


class ServiceClient(object):
    """
    Base class to represent a client for a service
    """

    def __init__(self, service_name,
                 registry_redis_config=None,
                 service_config=None,
                 timeout=DEFAULT_TIME_OUT,
                 sleep_before_retry=DEFAULT_SLEEP_BEFORE_RETRY,
                 max_tries=DEFAULT_MAX_TRIES,
                 heartbeat_frequency=DEFAULT_HEARTBEAT_FREQUENCY,
                 start_heartbeat_thread=True,
                 logger=None):

        self.logger = logger
        self._timeout = timeout
        self._sleep_before_retry = sleep_before_retry
        self._max_tries = max_tries
        self._heartbeat_frequency = heartbeat_frequency
        self._service_name = service_name

        if service_config:
            self._service_config = service_config
        else:
            self._registry = RedisServiceRegistry(**(registry_redis_config or
                                                     {}))
            self._service_config = self._registry.discover_service(
                self._service_name)[0]

        self.start_time = current_timestamp()
        self.shutdown_time = None
        self.guid = str(uuid.uuid4())
        self._context = zmq.Context()
        self._socket = socket_from_service_config(self._context,
                                                  self._service_config,
                                                  self._timeout)
        self.alive = True
        self.killed_by_error = None
        if start_heartbeat_thread:
            self._heartbeat_stop_event = threading.Event()
            heartbeat = ClientHeartbeat(self, self._service_config,
                                        self._timeout, self._heartbeat_frequency,
                                        self._max_tries)
            self._heartbeat_thread = threading.Thread(
                target=heartbeat,
                name='%s-client-heartbeat-%s' % (self._service_name,
                                                 current_timestamp()),
                args=(self._heartbeat_stop_event, )
            )
            self._heartbeat_thread.start()
        else:
            self._heartbeat_thread = None

    def __repr__(self):
        return 'ServiceClient(guid=%s, service_name=%s, service_guid=%s)' % \
               (self.guid, self._service_name, self._service_config['guid'])

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

    def shutdown(self):
        if self._heartbeat_thread is None:
            return
        if self.shutdown_time is not None:
            return
        self.alive = False
        self._heartbeat_stop_event.set()
        self._heartbeat_thread.join()
        self.log('error', 'stopped heartbeat thread')
        self._socket.close()
        self._socket = None
        self.shutdown_time = current_timestamp()

    def _setup_socket(self, reuse=True, timeout=DEFAULT_TIME_OUT):
        if reuse and self._socket is not None:
            return
        self._socket = socket_from_service_config(self._context,
                                                  self._service_config, timeout)

    def ping(self):
        return self.request('heartbeat', 'ping')

    def healthcheck(self):
        return self.request('healthcheck', 'health')

    def description(self):
        return self.request('description', 'description')

    def stop(self):
        return self.request('stop', 'stop')

    def request(self, function_name, request_message, response_class=None,
                timeout=DEFAULT_TIME_OUT, max_tries=DEFAULT_MAX_TRIES,
                sleep_before_retry=DEFAULT_SLEEP_BEFORE_RETRY):

        try_num = 0
        sleep_duration = None
        error = None

        self._setup_socket(timeout=timeout)

        while self.alive and try_num < max_tries:

            if function_name not in self._service_config['functions']:
                raise ServiceFunctionNotAvailableError(
                    '%r: function: %s not available for service: %s' % (
                        self, function_name, self._service_name
                    ))

            if sleep_duration:
                self.log('debug', 'Will try again in %s milliseconds' %
                         sleep_duration)
                time.sleep(sleep_duration/1000.0)
                self._setup_socket(reuse=False, timeout=timeout)

            try:
                self._socket.send_multipart(
                    [str(function_name), request_message]
                )
                response_string = self._socket.recv()
                if response_class is None:
                    return response_string
                else:
                    response = response_class()
                    response.ParseFromString(response_string)
                    return response

            except zmq.error.Again:
                error = ServiceClientTimeoutError(self._service_name,
                                                  function_name, timeout,
                                                  max_tries,
                                                  sleep_before_retry)
                self.log('debug', '%r can not complete function: %s of '
                                  'service: %s in %s milliseconds' %
                         (self, function_name, self._service_name, timeout))
                self._socket.close()
                self._socket = None
                sleep_duration = pow(2, try_num) * sleep_before_retry
                try_num += 1

            except zmq.error.ZMQError as exception:
                error = ServiceClientError(exception)
                self.log('error', 'ZMQError in %r while requesting '
                                  'function: %s of '
                                  'service: %s. Error: %r' %
                         (self, function_name, self._service_name, exception))
                break

            except Exception as exception:
                error = ServiceClientError(exception)
                self.log('error', 'Error in %r while requesting '
                                  'function: %s of service: %s. Error: %r' %
                         (self, function_name, self._service_name, exception))
                break

        if not self.alive:
            self.log('error', '%r is no longer alive, shutting it '
                              'down.' % self)

        self.shutdown()
        self.log('debug', 'heartbeat of %r shutdown' % self)

        if error:
            self.killed_by_error = error
            if isinstance(error, ServiceClientTimeoutError):
                self.log('error', '%r can not complete function: %s of '
                                  'service: %s in %s tries. Something must be '
                                  'wrong.' % (self, function_name,
                                              self._service_name, max_tries))
            raise error

        raise ServiceClientError('%r Should never get here' % self)


class ClientHeartbeat(object):

    def __init__(self, client, service_config,
                 timeout=DEFAULT_TIME_OUT,
                 heartbeat_frequency=DEFAULT_HEARTBEAT_FREQUENCY,
                 max_tries=DEFAULT_MAX_TRIES,
                 logger=None):
        self.logger = logger
        self._client = client
        # self._timeout = timeout
        self._timeout = 2 * 1000
        # self._max_tries = max_tries
        self._max_tries = 1
        self._heartbeat_frequency = heartbeat_frequency
        self._service_config = service_config
        self._context = zmq.Context()
        self._socket = socket_from_service_config(self._context,
                                                  self._service_config,
                                                  self._timeout)

    def __repr__(self):
        pass

    def __call__(self, *args, **kwargs):

        stop_event = args[0]
        error = None
        try_num = 0
        sleep_duration = None
        while not stop_event.is_set() and try_num < self._max_tries:
            error = None
            if sleep_duration:
                time.sleep(sleep_duration/1000.0)
            try:
                # print '%r sending heartbeat' % self._client
                self._socket.send_multipart(['heartbeat', 'heartbeat'])
                self._socket.recv()
                sleep_duration = self._heartbeat_frequency
            except zmq.error.Again:
                error = ServiceClientTimeoutError()
                self._socket.close()
                self._socket = socket_from_service_config(self._context,
                                                          self._service_config,
                                                          self._timeout)
                sleep_duration = pow(2, try_num) * self._heartbeat_frequency
                try_num += 1
            except zmq.error.ZMQError as exception:
                error = ServiceClientError(exception)
                break
            except Exception as exception:
                error = ServiceClientError(exception)
                break

        self._client.alive = False
        self._client.shutdown_time = current_timestamp()
        if error:
            self._client.killed_by_error = error
            self.log('error', 'heartbeat thread of client %r dying due '
                              'to error: %r' % (self._client, error))
            print '[%s] heartbeat of %r thread dying due to error: %r' % (
                time.strftime('%Y-%m-%d %H-%M-%S', time.localtime()),
                self._client, error)
        else:
            self.log('debug', 'Stopping heartbeat thread of %r' % self._client)

        print '[%s] Stopping heartbeat thread of %r' % (
            time.strftime('%Y-%m-%d %H-%M-%S', time.localtime()), self._client)

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
