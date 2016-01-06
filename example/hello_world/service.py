"""
This is an example implementation of a RPC service which uses
the base Service class
"""

from example.hello_world.hello_world_pb2 import \
    HelloRequest, HelloResponse
from core.service import Service
from core.service_message_handler import \
    ServiceMessageHandler


class GreetHandler(ServiceMessageHandler):

    def initialize(self):
        super(GreetHandler, self).initialize()
        self.request_class = HelloRequest
        self.response_class = HelloResponse

    def _handle(self, request, response):
        if request.HasField('name'):
            response.message = "Hello, %s" % request.name
        else:
            response.message = "Hello world"

    def _validate_request(self, request):
        super(GreetHandler, self)._validate_request(request)

class HelloWorldService(Service):
    """
    An example service, which exposes a greet function
    """

    MESSAGE_HANDLERS = {
        "greet": GreetHandler
    }


if __name__ == "__main__":
    HelloWorldService().run_service()

