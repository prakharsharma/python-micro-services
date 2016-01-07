"""
client for hello world service
"""

from core.service_method_caller import \
    ServiceMethodCaller
from example.hello_world.hello_world_pb2 import \
    HelloRequest, HelloResponse
import argparse


SERVICE_NAME = 'HelloWorld'
DEFAULT_REDIS_REGISTRY_CONFIG = {
    'host': '127.0.0.1',
    'port': 6379,
    'db': 9,
}


def main(config_file):
    cont = True
    service_method_caller = ServiceMethodCaller(
        service_registry_redis_config=DEFAULT_REDIS_REGISTRY_CONFIG,
        services=[SERVICE_NAME]
    )
    while cont:
        cmd = raw_input("[ping/health/description/hello/stop] ").strip().lower()
        if cmd == "ping":
            response = service_method_caller(
                "heartbeat",
                SERVICE_NAME,
                "ping"
            )
            print "[Response] " + response
        elif cmd == "health":
            response = service_method_caller(
                "healthcheck",
                SERVICE_NAME,
                "healthcheck"
            )
            print "[Response] " + response
        elif cmd == "description":
            response = service_method_caller(
                "description",
                SERVICE_NAME,
                "description"
            )
            print "[Response] " + response
        elif cmd == "hello":
            name = raw_input("Name ? ").strip().lower()
            request = HelloRequest()
            request.name = name
            response = service_method_caller(
                "greet",
                SERVICE_NAME,
                request,
                response_class=HelloResponse
            )
            print "[Response] %s" % response
        elif cmd == "stop":
            response = service_method_caller(
                "stop",
                SERVICE_NAME,
                "stop"
            )
            print 'service stopped'
            break
        cont = raw_input("Wanna continue (y/n)? ").strip().lower() == "y"

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description = "Hello world client")
    parser.add_argument("-c", "--config_file", required = False,
                        help = "config file")
    args = parser.parse_args()
    main(args.config_file)

