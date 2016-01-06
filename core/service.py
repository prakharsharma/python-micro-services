"""
This module backbone for a service, where request and response are
protobuf messages and communication happens over ZeroMQ sockets.

In addition to its core functions, each service implements methods to expose
- heartbeat
- healthcheck
- description
"""

import argparse
import collections
import ConfigParser
import hashlib
import json
import logging
import logging.config
import os
import os.path
import psutil
import random
import requests

import zmq
from core.service_message_handler import \
    HeartbeatHandler, DescriptionHandler, StopServiceHandler, \
    DefaultMessageHandler, HealthCheckHandler
from common.utils import redis_config_from_config_file, \
    zmq_socket_from_socket_type, set_time_zone, current_timestamp
from core.redis_service_registry import \
    RedisServiceRegistry
from core.error import StopServiceError


class Service(object):
    """
    """

    config = None
    VALID_CONN_METHOD = {"bind", "connect"}
    VALID_SCK_TYPES = {"REQ", "REP", "PUB", "SUB", "PUSH", "PULL"}
    MESSAGE_HANDLERS = {}
    CONFIG_REDIS_SECTION = "config_redis"
    BASE_TCP_ADDR = 'tcp://%s:%d'
    DEFAULT_FUNCTIONS = ["heartbeat", "healthcheck", "description", "stop"]
    DEFAULT_FUNCTION_MESSAGE_HANDLERS = {
        "heartbeat": HeartbeatHandler,
        "healthcheck": HealthCheckHandler,
        "description": DescriptionHandler,
        "stop": StopServiceHandler,
        "default": DefaultMessageHandler
    }
    EC2_INSTANCE_HOSTNAME_URL = \
        "http://169.254.169.254/latest/meta-data/public-hostname"
    EC2_METADATA_REQUEST_TIMEOUT = 1
    PID_DIR = "/home/ec2-user/publishing_services"
    FUNCTIONS_DECK_LENGTH = 10

    def __repr__(self):
        return "%s(name=%s, host=%s, guid=%s, pid=%s, description=%s, " \
               "functions=[%s])" % \
               (self.__class__.__name__, self.name, self.host, self.guid,
                self.pid, self.description, ", ".join(self.functions))

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

    def determine_host(self):
        try:
            r = requests.get(self.EC2_INSTANCE_HOSTNAME_URL,
                             timeout=self.EC2_METADATA_REQUEST_TIMEOUT)
            if r.status_code == requests.codes.OKAY:
                return r.content.strip()
        except Exception as exception:
            self.log('debug', 'looks like not running on an EC2 instance. '
                              'Trying to determine host from config file. '
                              'Error: %r' % exception)
            try:
                return self.config.get("global", "host")
            except Exception as exception:
                self.log('error', 'can not determine hostname, returning '
                                  'localhost. error: %r' % exception)
                return "localhost"

    def determine_guid(self):
        return hashlib.md5(
            ", ".join([
                "%s service" % self.name,
                self.host,
                "%s" % self.pid,
                "%s" % self.start_time,
                "%s" % random.randint(1, 1000000000)
            ])
        ).hexdigest()

    def _set_config(self, config_file=None):
        if not config_file:
            raise RuntimeError('A config file must be specified')

        logging.config.fileConfig(config_file)
        self.logger = logging.getLogger(self.__class__.__name__)

        self.config = ConfigParser.SafeConfigParser()
        self.config.read(config_file)

        self.description = self.config.get("global", "description")
        self.name = self.config.get("global", "name")
        self.env = self.config.get("global", "env")
        self.version = self.config.get("global", "version")
        self.start_time = current_timestamp()
        self.pid = os.getpid()
        self.proc = psutil.Process(self.pid)
        self.host = self.determine_host()
        self.guid = self.determine_guid()
        self.function_deque = collections.deque(
            maxlen=self.FUNCTIONS_DECK_LENGTH)
        self.stats = {
            'num_messages': 0,
            'num_success': 0,
            'num_error': 0,
            'avg_response_time': 0,
            'min_response_time': 0,
            'max_response_time': 0,
            'last_response_time': 0
        }
        self.pid_dir_path = self.PID_DIR
        try:
            self.pid_dir_path = self.config.get("global", "pid_dir")
        except ConfigParser.NoOptionError:
            pass
        self.pid_dir_path = "%s/%s" % (self.pid_dir_path, self.name)
        self.pid_file = "%s/%s" % (self.pid_dir_path, self.pid)

        self._registry = RedisServiceRegistry(
            **redis_config_from_config_file(
                self.config, "redis_service_registry",
                RedisServiceRegistry.DEFAULT_REDIS_CONFIG
            )
        )

        for k, v in self.DEFAULT_FUNCTION_MESSAGE_HANDLERS.items():
            if k not in self.MESSAGE_HANDLERS:
                self.MESSAGE_HANDLERS[k] = v

        self.functions = self.MESSAGE_HANDLERS.keys()

        self._message_handlers = {}

        try:
            self._setup_sockets()
            self._setup_message_handlers()
            self._registry.register_service({
                'name': self.name,
                'env': self.env,
                'guid': self.guid,
                'pid': self.pid,
                'host': self.host,
                'port': self.port,
                'socket_type': self.socket_type,
                'connect_method': self.connect_method,
                'functions': json.dumps(self.functions),
                'start_time': json.dumps(self.start_time),
                'alive': json.dumps(True)
            })
        except Exception as exception:
            import traceback
            self.log('error', 'Error while registering service: %s' %
                     traceback.format_exc())
            self._registry.deregister_service(self.name, self.guid, self.host)
            self.log('error', "%s while initializing %s service. Error: "
                              "%r" % (exception.__class__.__name__, self.name,
                                      exception))
            raise exception

    def _setup_sockets(self):
        self._poller = zmq.Poller()
        self._ports = {}
        self._context = zmq.Context()
        self.port, self.socket = self._get_socket_for_service()
        self._poller.register(self.socket, zmq.POLLIN)

    def _get_socket_for_service(self):
        port = self._registry.next_available_port(self.name, self.guid,
                                                  self.host)
        self.socket_type = "REP"
        self.connect_method = "bind"

        try:
            self.socket_type = self.config.get("global", "socket_type").upper()
            self.connect_method = self.config.get("global",
                                                  "connect_method").lower()
        except ConfigParser.NoSectionError:
            pass
        except ConfigParser.NoOptionError:
            pass

        if self.socket_type not in self.VALID_SCK_TYPES:
            raise RuntimeError(
                "Socket type %s not in set [%s] of valid socket types" %
                (self.connect_method, ", ".join(self.VALID_SCK_TYPES)))

        if self.connect_method not in self.VALID_CONN_METHOD:
            raise RuntimeError(
                "Connect method %s not in set [%s] of valid methods" %
                (self.connect_method, ", ".join(self.VALID_CONN_METHOD)))

        connect_string = self.BASE_TCP_ADDR % (
            "*" if self.connect_method == "bind" else self.host, port
        )

        socket = zmq_socket_from_socket_type(self._context, self.socket_type)

        getattr(socket, self.connect_method)(connect_string)
        return port, socket

    def _setup_message_handlers(self):
        for function, handler_class in self.MESSAGE_HANDLERS.items():
            self._message_handlers[function] = handler_class(
                self, function, self.socket, logger=self.logger
            )
            self.logger.debug("Registered handler: service: %s, function: %s, "
                              "handler: %s", self.name, function,
                              handler_class.__name__)

    def _run(self):
        while True:
            try:
                # self.logger.debug("poller: %s", self._poller)
                socks = dict(self._poller.poll())
            except KeyboardInterrupt as e:
                raise e
            except Exception as e:
                self.logger.error(e)
                raise e

            if self.socket in socks:
                function, request = self.socket.recv_multipart()
                function = function if function in self._message_handlers \
                    else 'default'
                self.function_deque.appendleft(function)
                if function != 'heartbeat':
                    self.logger.debug("Received RPC for function: %s", function)
                self.stats['num_messages'] += 1
                response_start_time = current_timestamp()
                try:
                    response = self._message_handlers[function].handle(request)
                    self.stats['num_success'] += 1
                except Exception:
                    response = 'empty response'
                    self.stats['num_error'] += 1
                    import traceback
                    self.log('error', 'Error while processing request for '
                                      'function: %s. Traceback: %s' %
                             (function, traceback.format_exc()))
                response_processing_time = current_timestamp() - \
                    response_start_time
                self.stats['last_response_time'] = response_processing_time
                self.stats['max_response_time'] = max(
                    self.stats['max_response_time'],
                    response_processing_time
                )
                if self.stats['min_response_time'] == 0:
                    self.stats['min_response_time'] = response_processing_time
                else:
                    self.stats['min_response_time'] = min(
                        self.stats['min_response_time'],
                        response_processing_time
                    )
                self.stats['avg_response_time'] = (
                    response_processing_time +
                    ((self.stats['num_messages'] - 1) * self.stats[
                        'avg_response_time'])
                )/float(self.stats['num_messages'])

                self.socket.send(response)

                if function == 'stop':
                    raise StopServiceError()

    def run(self):
        if not self.config:
            raise RuntimeError('A config file must be specified')
        try:
            self._run()
        except StopServiceError:
            self.logger.debug("Stopping :%s service in response to STOP "
                              "message.")
        except KeyboardInterrupt as e:
            self.log('debug', 'KeyboardInterrupt while running %s service' %
                     self.name)
        except Exception as exception:
            self.logger.error("Service crashed due to %s. args: [%s], message: "
                              "%s", exception.__class__.__name__, ", ".join(
                              exception.args), str(exception))
        finally:
            self._registry.deregister_service(self.name, self.guid, self.host)
            try:
                os.remove(self.pid_file)
            except:
                pass
            self.logger.debug("Deregistered %s service", self.name)

        self.logger.debug("%s service stopped", self.name)

    @classmethod
    def get_cmd_line_parser(cls):
        argparser = argparse.ArgumentParser(
            description="User management service")
        argparser.add_argument("-c", "--config_file", required=True,
                               help="config file")
        return argparser

    def run_service(self):
        set_time_zone()
        parser = self.get_cmd_line_parser()
        args = parser.parse_args()
        self._set_config(args.config_file)
        if not os.path.exists(self.pid_dir_path):
            os.makedirs(self.pid_dir_path)
        f = open(self.pid_file, 'w')
        f.write(json.dumps({
            'name': self.name,
            'env': self.env,
            'pid': self.pid,
            'guid': self.guid,
            'host': self.host,
            'port': self.port,
            'socket_type': self.socket_type,
            'connect_method': self.connect_method,
            'functions': self.functions,
            'start_time': self.start_time,
            'cmdline': self.proc.cmdline()
        }, indent=4))
        f.close()
        print 'Running %r' % self
        self.run()
