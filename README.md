# 🔌 bobravoz-grpc — The pluggable gRPC transport hub for bobrapet
[![Go Reference](https://pkg.go.dev/badge/github.com/bubustack/bobravoz-grpc.svg)](https://pkg.go.dev/github.com/bubustack/bobravoz-grpc)
[![Go Report Card](https://goreportcard.com/badge/github.com/bubustack/bobravoz-grpc)](https://goreportcard.com/report/github.com/bubustack/bobravoz-grpc)

`bobravoz-grpc` is a specialized, high-performance transport operator for [bobrapet](https://github.com/bubustack/bobrapet), designed to enable real-time, streaming AI and data workflows on Kubernetes. It acts as an intelligent transport hub, dynamically configuring gRPC connections and performing in-flight data processing for `bobrapet`'s `streaming` stories.

Quick links:
- Transport docs: https://bubustack.io/docs/transport

## 🌟 Key features

- **Intelligent Transport Topologies**: Automatically analyzes `Story` definitions to configure the optimal connection pattern:
  - **Peer-to-Peer (P2P)**: For maximum throughput, engrams are connected directly when no intermediate processing is required.
  - **Hub-and-Spoke**: For complex workflows, data is routed through the operator's hub to execute `Story` primitives in-flight.
- **Active Data Processing Hub**: `bobravoz-grpc` is more than a configurator; it's an active data plane component. It runs its own gRPC server to handle live data streams, executing CEL-based transformations and other logic without adding latency.
- **Pluggable by Design**: Built on a flexible `Transport` interface, the operator is architected to support other real-time protocols like NATS or Kafka in the future.
- **Seamless `bobrapet` Integration**: Natively understands `bobrapet` concepts like `streaming` patterns and `PerStory` vs. `PerStoryRun` strategies to ensure correct and efficient transport configuration.
- **Declarative Configuration**: Simply add an annotation to your `bobrapet` `Story` to have `bobravoz-grpc` manage its transport.

## 🏗️ Architecture

`bobravoz-grpc` operates on both the Kubernetes control plane and the data plane to provide its functionality.

- **Control Plane**: The `TransportReconciler` watches for `StoryRun` resources. When it finds one belonging to a `streaming` `Story` configured for `grpc` transport, it analyzes the step graph and injects the correct `UPSTREAM_HOST` and `DOWNSTREAM_HOST` environment variables into the engram containers.

- **Data Plane**: The operator runs an embedded gRPC `Hub Server`. When a `Story` requires in-flight processing (e.g., a `transform` step), the reconciler configures the engrams to route their data through this hub, which then executes the primitive's logic.

### 🧭 Connection topologies

Depending on your `Story` definition, `bobravoz-grpc` will create one of two connection types:

1.  **Peer-to-Peer (P2P)**: Simple, direct connection for maximum performance.
    *Story Definition:*
    ```yaml
    steps:
      - name: step-a
        ref: { name: engram-a }
      - name: step-b
        ref: { name: engram-b }
    ```
    *Resulting Topology:*
    `Engram A <--- gRPC ---> Engram B`

2.  **Hub-and-Spoke**: Data is routed through the operator for processing.
    *Story Definition:*
    ```yaml
    steps:
      - name: step-a
        ref: { name: engram-a }
      - name: transform-data
        type: transform
        with:
          expr: '{ "new_payload": payload.old_field + "!" }'
      - name: step-b
        ref: { name: engram-b }
    ```
    *Resulting Topology:*
    `Engram A --- gRPC --> bobravoz-hub --- gRPC --> Engram B`

## 🚀 Quick start

Using `bobravoz-grpc` requires an existing `bobrapet` installation.

### 1. Install the Operator

First, install the Custom Resource Definitions (CRDs):
```bash
make install
```

Next, deploy the operator controller to your cluster:
```bash
make deploy IMG=<your-repo>/bobravoz-grpc:latest
```
*(Replace `<your-repo>` with your container registry)*

## 🛠️ Local Development

1.  **Clone the repository:**
    ```bash
    git clone https://github.com/bubustack/bobravoz-grpc.git
    cd bobravoz-grpc
    ```

2.  **Run the controller locally:**
    This command runs the operator on your machine, using your local `kubeconfig` to communicate with the cluster. This is great for rapid development and debugging.
    ```bash
    make run
    ```

3.  **Run tests:**
    ```bash
    make test
    ```

4.  **End-to-end tests (Kind optional):**
    ```bash
    make test-e2e
    ```

## 📢 Support, Security, and Changelog

- See `SUPPORT.md` for how to get help and report issues.
- See `SECURITY.md` for vulnerability reporting and security posture.
- See `CHANGELOG.md` for version history.

## 🤝 Community

- Code of Conduct: see [CODE_OF_CONDUCT.md](./CODE_OF_CONDUCT.md) (Contributor Covenant v3.0)

## 📄 License

Copyright 2025 BubuStack.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
