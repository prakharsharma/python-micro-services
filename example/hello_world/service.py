"""
This is an example implementation of a RPC service which uses
the base Service class
"""

import json

import hello_world_pb2
from core.service import Service
from core.service_message_handler import \
    ServiceMessageHandler


class HealthcheckHandler(ServiceMessageHandler):
    def handle(self, request):
        d = {"processed": self._service.processed}
        self._socket.send(json.dumps(d))


class GreetHandler(ServiceMessageHandler):
    def handle(self, message):
        try:
            response = hello_world_pb2.HelloResponse()
            request = hello_world_pb2.HelloRequest()
            request.ParseFromString(message)
            if request.HasField('name'):
                response.message = "Hello, %s" % request.name
            else:
                response.message = "Hello world"
            response.header.success = True
            self._service.processed += 1
            self._socket.send(response.SerializeToString())
            self._service.logger.debug("processed message with name: %s",
                                request.name)
        except:
            pass


class HelloWorldService(Service):
    """
    An example service, which exposes a greet function
    """

    MESSAGE_HANDLERS = {
        "healthcheck": HealthcheckHandler,
        "greet": GreetHandler
    }


if __name__ == "__main__":
    HelloWorldService().run_service()

