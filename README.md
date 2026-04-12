# 🔌 bobravoz-grpc — The pluggable gRPC transport hub for bobrapet
[![Go Reference](https://pkg.go.dev/badge/github.com/bubustack/bobravoz-grpc.svg)](https://pkg.go.dev/github.com/bubustack/bobravoz-grpc)
[![Go Report Card](https://goreportcard.com/badge/github.com/bubustack/bobravoz-grpc)](https://goreportcard.com/report/github.com/bubustack/bobravoz-grpc)
[![Artifact Hub](https://img.shields.io/endpoint?url=https://artifacthub.io/badge/repository/bubustack)](https://artifacthub.io/packages/search?repo=bubustack)

`bobravoz-grpc` is a specialized, high-performance transport operator for [bobrapet](https://github.com/bubustack/bobrapet), designed to enable real-time, streaming AI and data workflows on Kubernetes. It acts as an intelligent transport hub, dynamically configuring gRPC connections and performing in-flight data processing for `bobrapet`'s `realtime` stories.

### Prerequisites
- Go 1.26+ (matching `go.mod`)
- Docker or another OCI-compatible image builder
- `kubectl`
- Access to a Kubernetes cluster supported by the current `bobrapet` / `bobravoz-grpc` release set

## 🔗 Quick Links

- Streaming contract: https://bubustack.io/docs/streaming/streaming-contract
- Transport settings: https://bubustack.io/docs/streaming/transport-settings
- Quickstart: https://bubustack.io/docs/getting-started/quickstart
- Artifact Hub chart: https://artifacthub.io/packages/search?repo=bubustack&ts_query_web=bobravoz-grpc

## 🌟 Key Features

- **Intelligent Transport Topologies**: Automatically analyzes `Story` definitions to configure the optimal connection pattern:
  - **Peer-to-Peer (P2P)**: For maximum throughput, engrams are connected directly when no intermediate processing is required.
  - **Hub-and-Spoke**: For complex workflows, data is routed through the operator's hub to execute `Story` primitives in-flight.
- **Active Data Plane**: `bobravoz-grpc` is more than a configurator; it's an active data plane component. It runs its own gRPC hub to broker streaming traffic and enforce transport bindings without bloating the controller.
- **Pluggable by Design**: Built on a flexible `Transport` interface, the operator is architected to support other real-time protocols like NATS or Kafka in the future.
- **Seamless `bobrapet` Integration**: Natively understands `bobrapet` concepts like the `realtime` story pattern and `PerStory` vs. `PerStoryRun` strategies to ensure correct and efficient transport configuration.
- **Declarative Configuration**: Select the gRPC transport with the `bobravoz.bubustack.io/transport=grpc` `Story` annotation, then tune delivery and routing through `Story.spec.transports[]`, `TransportBinding`, and transport-level streaming settings.

## 🏗️ Architecture

`bobravoz-grpc` operates on both the Kubernetes control plane and the data plane to provide its functionality.

- **Control Plane**: The `TransportReconciler` watches `StoryRun` resources. When the parent `Story` selects `bobravoz.bubustack.io/transport=grpc`, it analyzes the step graph, resolves the relevant `TransportBinding`, and injects a deterministic `BUBU_TRANSPORT_BINDING` environment variable so SDK sidecars can resolve upstream/downstream peers.

- **Data Plane**: The operator runs an embedded gRPC `Hub Server`. When a `Story` is configured for hub mediation, the reconciler routes Engram traffic through the hub and injects lightweight connectors (from `ghcr.io/bubustack/bobravoz-grpc-connector`) alongside workloads, keeping the controller image lean.

## 🧱 Backpressure Settings

You can tune hub buffering per transport using `Story.spec.transports[].streaming`.
`Transport.spec.streaming` acts as the default and is overridden by
story-specific settings.

```yaml
spec:
  transports:
  - name: rt
    transportRef: livekit-default
    streaming:
      backpressure:
        buffer:
          maxMessages: 500
          maxBytes: 10485760
          dropPolicy: drop_oldest
```

### 🧭 Connection Topologies

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

2.  **Hub-and-Spoke**: Data is routed through the operator for mediation.
    *Story Definition:*
    ```yaml
    steps:
      - name: step-a
        ref: { name: engram-a }
      - name: step-b
        ref: { name: engram-b }
    ```
    *Resulting Topology:*
    `Engram A --- gRPC --> bobravoz-hub --- gRPC --> Engram B`

## 🚀 Quick Start

Using `bobravoz-grpc` requires an existing `bobrapet` installation.

For fresh cluster bootstrap, cert-manager, storage, and the first example,
start with:

- https://bubustack.io/docs/getting-started/quickstart
- https://bubustack.io/docs/getting-started/prerequisites

`bobravoz-grpc` does not own the `Transport` / `TransportBinding` CRDs. Install
`bobrapet` first, then deploy the published Helm chart:

```bash
helm repo add bubustack https://bubustack.github.io/helm-charts
helm repo update

helm install bobravoz-grpc bubustack/bobravoz-grpc \
  --namespace bobrapet-system \
  --create-namespace
```

Use a distinct Helm release name such as `bobravoz-grpc`. Reusing `bobrapet`
will fail because Helm release names must be unique per namespace.

By default the chart expects the shared `ClusterIssuer` `bobrapet-shared-ca`.
That is what the `bobrapet` chart creates when installed with the default Helm
release name.

If `bobrapet` was installed with a non-default Helm release name, pass the
actual issuer name when installing `bobravoz-grpc`:

```bash
helm install bobravoz-grpc bubustack/bobravoz-grpc \
  --namespace bobrapet-system \
  --create-namespace \
  --set sharedCAIssuerName=<bobrapet-release>-bobrapet-shared-ca
```

If you maintain your own images, build and push both the controller and connector artifacts:
```bash
# Controller / manager
make docker-build IMG=<your-repo>/bobravoz-grpc:<tag>
make docker-push IMG=<your-repo>/bobravoz-grpc:<tag>

# Connector sidecar
make docker-build-connector CONNECTOR_IMG=<your-repo>/bobravoz-grpc-connector:<tag>
make docker-push-connector CONNECTOR_IMG=<your-repo>/bobravoz-grpc-connector:<tag>
```
Helm chart consumers can then override `controllerManager.manager.image.repository` /
`controllerManager.manager.image.tag` and `controllerManager.manager.env.connectorImage`
to point at the published tags. Set `sharedCAIssuerName` as well if your
`bobrapet` release did not create the default `bobrapet-shared-ca`. Connector image settings can also come from the
operator ConfigMap (`connector.image` and `connector.image-pull-policy`), but
explicit manager flags or `CONNECTOR_IMAGE` / `CONNECTOR_IMAGE_PULL_POLICY`
environment variables take precedence over the ConfigMap.

## 🛠️ Local Development

1.  **Clone and build:**
    ```bash
    git clone https://github.com/bubustack/bobravoz-grpc.git
    cd bobravoz-grpc
    make build
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

5.  **Generate the Helm chart (writes to `dist/charts/`):**
    ```bash
    make helm-chart
    # override chart name if needed
    make helm-chart CHART=my-custom-name
    ```
    The chart overlays such as `Chart.yaml` and `README.md` live beneath
    `hack/charts/<chart>/` and are copied into the generated chart. Values are
    auto-generated by helmify from kustomize manifests.

## 📢 Support, Security, and Changelog

- See [`SUPPORT.md`](./SUPPORT.md) for how to get help and report issues.
- See [`SECURITY.md`](./SECURITY.md) for vulnerability reporting and security posture.
- See [`CHANGELOG.md`](./CHANGELOG.md) for the release-please managed version history.

## 🤝 Community

- Code of Conduct: see [CODE_OF_CONDUCT.md](./CODE_OF_CONDUCT.md) (Contributor Covenant v3.0)
- Discord: https://discord.gg/dysrB7D8H6

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
