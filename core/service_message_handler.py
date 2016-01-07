"""
Module provides following: -
    - holds definition of abstract base class to represent message handler
    - provides default implementation of heartbeat handler
    - provides default implementation of description handler
"""

import datetime
import json
import psutil
import os

from common.utils import current_timestamp

from core.error import StopServiceError, \
    BadServiceRequestError, BadServiceMessageHandlerError


class ServiceMessageHandler(object):

    """
    abstract base class to provide message handler interface
    """

    def __init__(self, service, socket_name, socket, logger=None,
                 is_proto=True):
        self._service = service
        self._socket_name = socket_name
        self._socket = socket
        self.config = self._service.config
        self.logger = logger
        self.request_class = None
        self.response_class = None
        self.is_proto = is_proto
        self.initialize()
        if is_proto and \
                (self.response_class is None or self.response_class is None):
            raise BadServiceMessageHandlerError("Both request_class and "
                                                "response_class have to be "
                                                "provided")

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

    def initialize(self):
        # place holder for custom initialization code for derived message
        # handlers
        pass

    def handle(self, message):
        request_start_time = current_timestamp()
        response = self.response_class()
        request_guid = None
        request_client = None
        try:
            try:
                request = self.request_class()
                request.ParseFromString(message)
                request_guid = request.header.request_guid
                request_client = request.header.client
                response.header.request_guid = request_guid
            except Exception as exception:
                raise BadServiceRequestError(exception)
            else:
                self._validate_request(request)
                self.log('info', '%s of %s service got request guid %s, '
                                 'from client: %s' %
                         (self.__class__.__name__, self._service.name,
                         request.header.request_guid,
                         request.header.client))
                self._handle(request, response)
                response.header.success = True
                self.log('debug', 'successfully processed request guid: %s, '
                                  'from client: %s' %
                         (request.header.request_guid,
                         request.header.client))
        except Exception as exception:
            import traceback
            self.log('error', 'Error while handling request. Type: %s, '
                              'Error: %r. Traceback: %s' %
                     (exception.__class__.__name__, exception,
                      traceback.format_exc()))
            self._response_from_exception(exception, response)
        finally:
            response.header.response_time = current_timestamp() - \
                request_start_time
            self.log('info', '%s of %s service took %s microseconds to '
                             'respond to request guid: %s, from client: %s' %
                     (self.__class__.__name__, self._service.name,
                     response.header.response_time, request_guid,
                     request_client))
            return response.SerializeToString()

    def _response_from_exception(self, exception, response):
        response.header.success = False
        response.header.error.type = exception.__class__.__name__
        response.header.error.message = str(exception.message)
        response.header.error.args = "[%s]" % ", ".join(
            [str(x) for x in exception.args])
        return response

    def _validate_request(self, request):
        # place holder for custom request validation code for derived message
        # handlers
        pass

    def _handle(self, request, response):
        raise NotImplementedError()


class HeartbeatHandler(ServiceMessageHandler):

    def __init__(self, service, socket_name, socket, logger=None):
        super(HeartbeatHandler, self).__init__(service, socket_name,
                                               socket, logger, is_proto=False)

    def handle(self, request):
        return "PONG"


class DescriptionHandler(ServiceMessageHandler):

    def __init__(self, service, socket_name, socket, logger=None):
        super(DescriptionHandler, self).__init__(service, socket_name,
                                                 socket, logger, is_proto=False)

    def handle(self, request):
        d = {
            'name': self._service.name,
            'env': self._service.env,
            'version': self._service.version,
            'pid': self._service.pid,
            'guid': self._service.guid,
            'host': self._service.host,
            'port': self._service.port,
            'socket_type': self._service.socket_type,
            'connect_method': self._service.connect_method,
            'functions': self._service.functions,
            'start_time': self._service.start_time,
            'function_deck': [x for x in self._service.function_deque],
            'stats': self._service.stats
        }
        return json.dumps(d)


class StopServiceHandler(ServiceMessageHandler):

    def __init__(self, service, socket_name, socket, logger=None):
        super(StopServiceHandler, self).__init__(service, socket_name,
                                                 socket, logger, is_proto=False)

    def handle(self, request):
        return 'STOPPED'


class HealthCheckHandler(ServiceMessageHandler):

    def __init__(self, service, socket_name, socket, logger=None):
        super(HealthCheckHandler, self).__init__(service, socket_name,
                                               socket, logger, is_proto=False)

    def handle(self, request):
        d = {
            'name': self._service.name,
            'env': self._service.env,
            'version': self._service.version,
            'pid': self._service.pid,
            'guid': self._service.guid,
            'host': self._service.host,
            'port': self._service.port,
            'socket_type': self._service.socket_type,
            'connect_method': self._service.connect_method,
            'functions': self._service.functions,
            'start_time': self._service.start_time,
            'function_deck': [x for x in self._service.function_deque],
            'stats': self._service.stats,
            'start_datetime': datetime.datetime.fromtimestamp(
                self._service.start_time/1000000
            ).strftime('%Y-%m-%d %H:%M:%S'),
            'cmdline': self._service.proc.cmdline()
        }

        proc = psutil.Process(self._service.pid)
        d['stats']['cpu_percent'] = proc.cpu_percent()
        try:
            d['stats']['vms'] = int(proc.memory_info().vms / 1024)
        except:
            d['stats']['vms'] = '?'
        try:
            d['stats']['rss'] = int(proc.memory_info().rss / 1024)
        except:
            d['stats']['rss'] = '?'
        d['stats']['memory_percent'] = round(proc.memory_percent(), 1)

        return json.dumps(d)


class DefaultMessageHandler(ServiceMessageHandler):

    def __init__(self, service, socket_name, socket, logger=None):
        super(DefaultMessageHandler, self).__init__(service, socket_name,
                                                    socket, logger,
                                                    is_proto=False)

    def handle(self, request):
        return "Function not available for service: %s" % self._service.name
