# 🔌 bobravoz-grpc Helm Chart

BubuStack's gRPC transport operator and hub for realtime Stories. It watches
`bobrapet` resources, configures transport bindings, and serves the hub path
for mediated streaming flows.

## Prerequisites

- Kubernetes cluster supported by the current `bobrapet` / `bobravoz-grpc` release set
- Helm **3.x**
- [cert-manager](https://cert-manager.io/) installed
- `bobrapet` installed first so the `Transport` / `TransportBinding` CRDs and controllers exist
- Shared `ClusterIssuer` available. By default the chart uses `bobrapet-shared-ca`, which is what the `bobrapet` chart creates when installed with the default Helm release name.

## Installation

For fresh cluster bootstrap, cert-manager, storage, and the first workflow,
start with the website guides:

- [Quickstart](https://bubustack.io/docs/getting-started/quickstart)
- [Prerequisites](https://bubustack.io/docs/getting-started/prerequisites)

```bash
# Add the BubuStack Helm repo
helm repo add bubustack https://bubustack.github.io/helm-charts
helm repo update

# Install the transport operator
helm install bobravoz-grpc bubustack/bobravoz-grpc \
  --namespace bobrapet-system \
  --create-namespace
```

Use a distinct Helm release name such as `bobravoz-grpc`. Do not reuse the
`bobrapet` release name in the same namespace.

If `bobrapet` was installed with a non-default release name, set the shared CA
issuer explicitly when installing `bobravoz-grpc`:

```bash
helm install bobravoz-grpc bubustack/bobravoz-grpc \
  --namespace bobrapet-system \
  --create-namespace \
  --set sharedCAIssuerName=<bobrapet-release>-bobrapet-shared-ca
```

## Local Chart (Development)

```bash
make helm-chart

helm install bobravoz-grpc ./dist/charts/bobravoz-grpc \
  --namespace bobrapet-system \
  --create-namespace
```

`hack/charts/bobravoz-grpc/` only contains chart overlay files. The installable
chart is generated into `dist/charts/bobravoz-grpc/`.

## Migrating From Manifests / Kustomize

If you previously deployed `bobravoz-grpc` with `make deploy` or
`kubectl apply -f install.yaml`, remove that install before switching to Helm.
Helm will not adopt pre-existing unmanaged resources.

From a source checkout:

```bash
make undeploy
```

From a released installer manifest:

```bash
kubectl delete -f https://github.com/bubustack/bobravoz-grpc/releases/download/<tag>/install.yaml
```

If you already deleted the namespace and only cluster-scoped leftovers remain,
delete the stale `bobravoz-grpc` resources before retrying:

```bash
kubectl delete clusterrole \
  bobravoz-grpc-manager-role \
  bobravoz-grpc-metrics-auth-role \
  bobravoz-grpc-metrics-reader

kubectl delete clusterrolebinding \
  bobravoz-grpc-manager-rolebinding \
  bobravoz-grpc-metrics-auth-rolebinding

kubectl delete mutatingwebhookconfiguration \
  bobravoz-grpc-transport-connector-mutating-webhook-configuration
```

## Common Values

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `controllerManager.manager.image.repository` | string | `ghcr.io/bubustack/bobravoz-grpc` | Manager image repository |
| `controllerManager.manager.image.tag` | string | release version | Manager image tag |
| `controllerManager.manager.env.connectorImage` | string | chart `appVersion` | Connector sidecar image when left at the scaffold default |
| `operatorConfig.*` | object | generated defaults | Operator ConfigMap settings copied into the chart |
| `sharedCAIssuerName` | string | `bobrapet-shared-ca` | ClusterIssuer used for webhook, metrics, and hub certificates |
| `serviceAccount.*` | object | generated defaults | ServiceAccount creation and annotations |
| `kubernetesClusterDomain` | string | `cluster.local` | Cluster DNS suffix used in generated TLS SANs |

## Upgrading

```bash
helm repo update
helm upgrade bobravoz-grpc bubustack/bobravoz-grpc --namespace bobrapet-system
```

## Uninstalling

```bash
helm uninstall bobravoz-grpc --namespace bobrapet-system
```

`bobravoz-grpc` does not own the `bobrapet` CRDs. Keep managing those through
the `bobrapet` release or the upstream CRD install flow.

## Links

- [Documentation](https://bubustack.io/docs/transport)
- [Streaming Contract](https://bubustack.io/docs/streaming/streaming-contract)
- [Transport Settings](https://bubustack.io/docs/streaming/transport-settings)
- [GitHub](https://github.com/bubustack/bobravoz-grpc)
