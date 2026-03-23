# Bobravoz-gRPC Operator Configuration

The bobravoz-grpc operator reads its configuration from a Kubernetes ConfigMap. The ConfigMap name and namespace are passed via CLI flags:

```
--config-name=bobravoz-grpc-operator-config
--config-namespace=bobrapet-system
```

When deployed via Helm, these are set automatically from `manager.configName` and `manager.configNamespace` values.

---

## Hub

| Key | Default | Purpose |
| --- | --- | --- |
| `hub.transport-security-mode` | `plaintext` | Transport security mode: `tls` or `plaintext`. |
| `hub.buffer-max-messages` | `1000` | Maximum buffered messages per stream. |
| `hub.buffer-max-bytes` | `10485760` | Maximum buffer size in bytes (10 MiB). |
| `hub.buffer-eviction-ttl` | `10m` | TTL before buffered messages are evicted. |
| `hub.buffer-eviction-interval` | `1m` | How often the eviction sweep runs. |
| `hub.channel-buffer-size` | `100` | Go channel buffer size for stream processing. |
| `hub.per-message-timeout` | `10m` | Timeout for delivering a single message. |
| `hub.max-active-streams` | `2000` | Maximum concurrent active streams. |
| `hub.max-buffers` | `1000` | Maximum number of stream buffers. |
| `hub.max-downstreams-hard-cap` | `64` | Hard cap on downstream targets per stream. |

---

## Connector

| Key | Default | Purpose |
| --- | --- | --- |
| `connector.image` | `ghcr.io/bubustack/bobravoz-connector:latest` | Connector sidecar image. |
| `connector.image-pull-policy` | `IfNotPresent` | Image pull policy for connector pods. |

---

## Templating

| Key | Default | Purpose |
| --- | --- | --- |
| `templating.evaluation-timeout` | `5s` | Timeout for hub-side template evaluation. |
| `templating.max-output-bytes` | `65536` | Maximum evaluated output size. |
| `templating.deterministic` | `false` | Restricts non-deterministic helpers. |
| `templating.offloaded-data-policy` | `error` | How to handle templates referencing offloaded data. |
| `templating.materialize-engram` | `bubu-materialize` | Engram used for pod-based materialization. |

---

## Telemetry

| Key | Default | Purpose |
| --- | --- | --- |
| `telemetry.trace-propagation` | `true` | Propagate OTEL trace context through hub/connector pipeline. |
