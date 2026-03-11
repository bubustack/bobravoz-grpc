# bobravoz-grpc — gRPC Transport Hub (AI Agent Guide)

bobravoz-grpc is the streaming transport operator for BubuStack. It bridges
Kubernetes resources to gRPC clients and manages real-time streaming data
planes for bobrapet's streaming Stories.

## What it does

- Analyzes Story definitions to configure optimal transport topologies:
  - **Peer-to-Peer (P2P):** Direct engram-to-engram connections for max throughput.
  - **Hub-and-Spoke:** Data routed through the operator's gRPC hub for in-flight processing.
- Runs an active gRPC data plane (hub) to broker streaming traffic.
- Manages transport bindings, connectors, and TLS.

## Project layout

```
cmd/main.go                    Operator manager entry point
cmd/connector/                 Standalone connector binary
internal/controller/           Kubernetes controllers (transport binding, connector lifecycle)
internal/connector/            Connector runtime implementation
internal/hub/                  gRPC hub server (streaming broker)
internal/transport/            Transport abstraction and implementations
internal/config/               Operator configuration
internal/webhook/              Admission webhooks
internal/telemetry/            Observability and metrics
pkg/metrics/                   Prometheus metrics (controller, hub, connector, RPC)
config/                        Kustomize manifests, CRDs, RBAC
hack/                          Build and deployment scripts
```

Uses Kubebuilder multi-group layout. Check `PROJECT` for resource details.

## Dependencies

- Imports `core` (contracts, transport runtime).
- Imports `bobrapet` API types (CRDs) but NOT controller internals.
- Imports `tractatus` for protobuf definitions.

## Build and test

```bash
go build ./...
make test
make manifests generate    # after editing *_types.go
make lint-fix              # after editing .go files
```

## Auto-generated files (DO NOT EDIT)

- `config/crd/bases/*.yaml`, `config/rbac/role.yaml` — `make manifests`
- `**/zz_generated.*.go` — `make generate`
- `PROJECT` — kubebuilder CLI
