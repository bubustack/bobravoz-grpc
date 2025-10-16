# Path to the protoc compiler
PROTOC ?= protoc

.PHONY: proto
proto: protoc-gen-go protoc-gen-go-grpc check-protoc ## Compile protobuf definitions to Go.
	@echo "Generating protobuf code..."
	PATH=$(LOCALBIN):$(PATH) $(PROTOC) --proto_path=proto --go_out=proto --go_opt=paths=source_relative \
    --go-grpc_out=proto --go-grpc_opt=paths=source_relative \
    proto/v1/*.proto

.PHONY: check-protoc
check-protoc:
	@if ! command -v $(PROTOC) &> /dev/null; then \
		echo "Error: protoc is not installed. Please install it."; \
		exit 1; \
	fi

## Buf tooling
.PHONY: buf-lint
buf-lint: ## Run buf lint on proto definitions
	@command -v buf >/dev/null 2>&1 || { echo "Error: buf CLI is not installed"; exit 1; }
	buf lint

.PHONY: buf-breaking
buf-breaking: ## Run buf breaking changes check (requires git history)
	@command -v buf >/dev/null 2>&1 || { echo "Error: buf CLI is not installed"; exit 1; }
	buf breaking --against '.git#branch=main'
