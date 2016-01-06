PROTOC=protoc
SERVICES_BASE_PATH=.
SERVICES=service
CURRENT_DIR=$(notdir $(shell pwd))

all: services

services: $(SERVICES)

service: $(SERVICES_BASE_PATH)/base/service.proto
	$(PROTOC) --python_out=./ $(SERVICES_BASE_PATH)/service.proto

clean:
	find . -name "*_pb2.py" | xargs rm
	find . -name "*.pb.h" | xargs rm
	find . -name "*.pb.cc" | xargs rm
	find . -name "*.pyc" | xargs rm
