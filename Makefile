PROTOC=protoc
SERVICES_BASE_PATH=.
SERVICES=service hello_world 
CURRENT_DIR=$(notdir $(shell pwd))

all: services

services: $(SERVICES)

service: $(SERVICES_BASE_PATH)/core/service.proto
	$(PROTOC) --python_out=./ $(SERVICES_BASE_PATH)/core/service.proto

hello_world: $(SERVICES_BASE_PATH)/example/hello_world/hello_world.proto
	$(PROTOC) --python_out=./ $(SERVICES_BASE_PATH)/example/hello_world/hello_world.proto

clean:
	find . -name "*_pb2.py" | xargs rm
	find . -name "*.pb.h" | xargs rm
	find . -name "*.pb.cc" | xargs rm
	find . -name "*.pyc" | xargs rm
