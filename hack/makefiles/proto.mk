# Path to the protoc compiler
PROTOC ?= protoc

.PHONY: proto
proto: protoc-gen-go protoc-gen-go-grpc check-protoc ## Compile protobuf definitions to Go.
	@echo "Generating protobuf code..."
	PATH=$(LOCALBIN):$(PATH) $(PROTOC) --proto_path=proto --go_out=proto --go_opt=paths=source_relative \
    --go-grpc_out=proto --go-grpc_opt=paths=source_relative \
    proto/*.proto

.PHONY: check-protoc
check-protoc:
	@if ! command -v $(PROTOC) &> /dev/null; then \
		echo "Error: protoc is not installed. Please install it."; \
		exit 1; \
	fi
