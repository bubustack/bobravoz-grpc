# CLAUDE.md

This file provides guidance to Claude Code when working with code in this repository.

## Commands

```bash
# Build
make build                        # Build manager binary (runs manifests -> generate -> fmt -> vet)

# Everyday local dev (most common)
make kind-load-controller         # docker-build + docker-build-connector -> kind load both -> rollout restart
make deploy                       # Deploy controller to cluster (kustomize)
make install                      # Apply repo-owned generated manifests; bobrapet still owns the CRDs

# Kind cluster management (hack/makefiles/kind.mk)
make kind-create                  # Create kind cluster (KIND_CLUSTER_NAME=bobrapet, hack/kind-config.yaml)
make kind-delete                  # Delete kind cluster
make kind-status                  # Check cluster status (nodes + pods)
make kind-get-kubeconfig          # Export kubeconfig to ~/.kube/config
make kind-load-image IMAGE=x:tag  # Load arbitrary image into kind cluster

# Testing
make test                         # Unit tests with envtest
make test-e2e                     # E2e tests with dedicated Kind cluster (bobravoz-grpc-test-e2e)
make setup-test-e2e               # Set up e2e Kind cluster
make cleanup-test-e2e             # Tear down e2e Kind cluster

# Code quality
make lint                         # Run golangci-lint (v2.11.4)
make lint-fix                     # Run golangci-lint with auto-fix
make fmt                          # go fmt ./...
make vet                          # go vet ./...

# Code generation
make manifests                    # Generate webhook and RBAC manifests (controller-gen)
make generate                     # Generate DeepCopy methods (controller-gen)

# Docker
make docker-build                 # Build manager image (IMG=bobravoz-grpc:latest)
make docker-build-connector       # Build connector image (CONNECTOR_IMG=bobravoz-grpc-connector:latest)
make docker-push                  # Push manager image
make docker-push-connector        # Push connector image
make docker-buildx                # Multi-platform build (manager)
make docker-buildx-connector      # Multi-platform build (connector)

# Deployment
make install                      # Apply repo-owned generated manifests; bobrapet still owns the CRDs
make uninstall                    # Remove repo-owned generated manifests from the cluster
make deploy                       # Deploy controller to cluster
make undeploy                     # Remove controller from cluster

# Helm
make helm-chart                   # Generate Helm chart via helmify
```

**Fresh setup:** `make kind-create` -> install `bobrapet` and its CRDs -> `make deploy` -> `make kind-load-controller`
**Iteration:** edit code -> `make kind-load-controller` (rebuilds manager+connector, loads, restarts)

## Architecture

**bobravoz-grpc** is the streaming transport operator for BubuStack. It bridges Kubernetes resources to gRPC clients and manages real-time streaming data planes.

### What it does

- Analyzes Story definitions to configure optimal transport topologies:
  - **Peer-to-Peer (P2P):** Direct engram-to-engram connections for max throughput
  - **Hub-and-Spoke:** Data routed through the operator's gRPC hub for in-flight processing
- Runs an active gRPC data plane (hub) to broker streaming traffic
- Manages transport bindings, connectors, and TLS

### Project layout

```
cmd/
  main.go                  Operator manager entry point
  connector/               Standalone connector binary
internal/
  controller/              Kubernetes controllers (transport binding, connector lifecycle)
  connector/               Connector runtime implementation
  hub/                     gRPC hub server (streaming broker)
  transport/               Transport abstraction and implementations
  config/                  Operator configuration
  webhook/                 Admission webhooks
  telemetry/               Observability and metrics
pkg/
  metrics/                 Prometheus metrics (controller, hub, connector, RPC)
config/                    Kustomize manifests, RBAC, webhook, and network policy overlays
hack/                      Build and deployment scripts
```

Uses Kubebuilder multi-group layout. Check `PROJECT` for resource details.

## BubuStack Ecosystem Context

BubuStack is a Kubernetes-native workflow orchestration platform. This workspace is part of a multi-module Go ecosystem at `/Users/kashotyan/personal/bubustack/`.

### Module map

| Module | Role |
|--------|------|
| `core` | Shared contracts, templating engine, transport runtime, identity helpers |
| `tractatus` | Protobuf service and message definitions for gRPC transport |
| `bobrapet` | Kubernetes operator: CRDs, controllers, webhooks |
| **`bobravoz-grpc`** | **This project** — gRPC transport hub: streaming data plane |
| `bubu-sdk-go` | Go SDK for building Engrams and Impulses |
| `engrams/*` | Individual Engram/Impulse implementations |
| `bubuilder` | Web console and API server |
| `bubu-registry` | Git-backed component registry and CLI |

### Dependency graph (strict DAG — no cycles)

```
tractatus (protobuf contracts)
    |
  core (contracts, templating, transport runtime)
    |
  bobrapet (CRDs, controllers, webhooks, storage, enums)
   / \
  /   \
bubu-sdk-go    bobravoz-grpc <-- YOU ARE HERE
  |
engrams/*
```

### Dependencies

- Imports `core` (contracts, transport runtime)
- Imports `bobrapet` API types (CRDs) but NOT controller internals
- Imports `tractatus` for protobuf definitions

### Documentation

Comprehensive ecosystem docs live in the bobrapet project: `/Users/kashotyan/personal/bubustack/bobrapet/docs/`

Key docs for this project:
- `docs/realtime/streaming-contract.md` — Streaming message contract, ordering, control semantics
- `docs/realtime/transport-settings.md` — Backpressure, routing, replay, partitioning, recording
- `docs/overview/architecture.md` — Module map, runtime topology
- `docs/overview/durable-semantics.md` — Delivery guarantees and replay expectations
- `docs/observability/overview.md` — Streaming metrics, traces, debugging tips

## Key Conventions

- Hub-and-spoke vs P2P topology is determined by Story analysis — the operator picks the optimal topology automatically
- Transport CRDs (Transport, TransportBinding) are defined in bobrapet but managed by this operator
- Transport is cluster-scoped; TransportBinding is namespaced
- Connector images are built separately from the manager (see docker-build-connector)
- gRPC streaming uses bidirectional streams with tractatus protobuf definitions

## Debugging Across the Ecosystem

### Ownership boundaries

| Area | Owner |
|------|-------|
| Transport/TransportBinding CRD definitions | bobrapet |
| **Transport lifecycle, hub, connectors, data plane** | **bobravoz-grpc (this project)** |
| SDK transport connector (client side) | bubu-sdk-go |
| Protobuf message definitions | tractatus |
| Transport runtime abstractions | core |

### Common cross-project debugging scenarios

- **Transport not connecting**: Check TransportBinding status (kubectl describe) -> check bobravoz-grpc controller logs -> check connector pod logs
- **Streaming data loss/ordering**: Check streaming-contract.md for expected semantics -> check hub metrics (pkg/metrics/) -> check SDK transport_connector.go
- **Connector crash loops**: Check connector binary logs -> verify TLS config -> check transport binding spec matches expected topology
- **Topology mismatch**: Check Story definition transport settings -> verify bobravoz-grpc picked correct topology (P2P vs hub-and-spoke) -> check controller decision logic
- **gRPC errors**: Check tractatus protobuf compatibility -> verify connector and hub use same proto versions

### How to trace cross-boundary issues

1. Start with transport resource status: `kubectl get transports,transportbindings -A`
2. Check bobravoz-grpc manager logs for controller reconciliation errors
3. Check individual connector pod logs for data plane issues
4. If SDK-side, check transport_connector.go in bubu-sdk-go
5. Use streaming docs (streaming-contract.md, transport-settings.md) to understand expected behavior

## Skills

Skills live in `.claude/skills/` and are invoked via `/skill-name` in Claude Code.

### `/deep-implement`
Structured implementation workflow (Research → Plan → Annotate → Todo → Tests → Implement) with phase gates. Adapted for Go/kubebuilder: envtest, table-driven tests, `make manifests generate` after types changes.

### `/controller-audit`
Report-only audit of controller reconciliation and webhook code. Checks idempotency, status conditions, error handling, requeue logic, finalizers, RBAC markers, webhook patterns. Outputs PASS/WARN/FAIL with file:line citations.

### `/crd-review`
Post-edit review after `*_types.go` changes. Runs `make manifests generate`, checks markers, backwards compatibility, JSON tags, webhook coverage, status subresource.

### `/cross-debug`
Guided debugging across bobrapet/bobravoz-grpc/bubu-sdk-go boundaries. Identifies ownership, traces logs/resources, consults docs, proposes fixes with cross-project impact analysis.

## Workflow Rules

- **Always use Makefile targets** for build, test, lint, docker, kind, and deployment operations. Never run raw `docker`, `kind`, `kubectl apply/patch/delete`, or other infrastructure commands directly — use the corresponding `make` target instead (e.g. `make docker-build`, `make docker-build-connector`, `make test-e2e`, `make install`, `make deploy`).
- **Read-only kubectl is allowed** for debugging: `kubectl get`, `kubectl describe`, `kubectl logs`, `kubectl api-resources`, `kubectl config current-context`.
- **Ask before running** any infrastructure command that doesn't have a Makefile target (e.g. raw `docker`, `kind`, `kubectl apply`). The user will either point you to the right `make` target or approve the one-off command.

## Safety Rules

- **Never edit auto-generated files**: `config/rbac/role.yaml`, `dist/install.yaml`, `dist/charts/*`, `**/zz_generated.*.go`
- **Never remove `// +kubebuilder:scaffold:*` markers**
- **This repo normally does not own `*_types.go` CRD sources**: if you are changing shared API types, do that in `bobrapet` and run generation there
- **After Go edits**: `make fmt`, then targeted `go test`
- **Prefer small, reviewable diffs**
