"""
client for hello world service
"""

import hello_world_pb2
import argparse

from core.client import \
    ServiceClient


class HelloWorldClient(ServiceClient):
    """
    """

    def __init__(self, redis_config_file):
        super(HelloWorldClient, self).__init__("HelloWorld", redis_config_file)

    def greet(self, name):
        socket = self._get_endpoint_socket("greet")
        request = hello_world_pb2.HelloRequest()
        request.name = name
        socket.send(request.SerializeToString())
        response = hello_world_pb2.HelloResponse()
        response.ParseFromString(socket.recv())
        return response.message


def main(config_file):
    cont = True
    client = HelloWorldClient(config_file)
    while cont:
        cmd = raw_input("[ping/health/description/hello/stop] ").strip().lower()
        if cmd == "ping":
            print "[Response] " + client.ping()
        elif cmd == "health":
            print "[Response] " + client.healthcheck()
        elif cmd == "description":
            print "[Response] " + client.description()
        elif cmd == "hello":
            name = raw_input("Name ? ").strip().lower()
            print "[Response] " + str(client.greet(name))
        elif cmd == "stop":
            recv = client.stop()
            if recv == "STOPPED":
                print "Service stopped"
                break
        cont = raw_input("Wanna continue (y/n)? ").strip().lower() == "y"

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description = "Hello world client")
    parser.add_argument("-c", "--config_file", required = False,
                        help = "config file")
    args = parser.parse_args()
    main(args.config_file)

