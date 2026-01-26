# Contributing to bobravoz-grpc

Thank you for helping make the transport operator fast, stable, and Kubernetes-native. This guide explains how to report issues, propose features, and ship high-quality pull requests.

## Reporting bugs

- Search [existing issues](https://github.com/bubustack/bobravoz-grpc/issues?q=is%3Aissue) before filing a new one.
- When opening a bug, include:
  - The `Story` / `StoryRun` snippet that reproduces the issue, plus any annotations that select the `grpc` transport.
  - Logs from the transport controller (`kubectl logs deployment/bobravoz-grpc-controller-manager`) and affected Engram pods.
  - Kubernetes version, cluster type (Kind/Minikube/managed), and whether transport bindings were pre-existing or newly created.
  - If the problem involves streaming payloads, attach the relevant `TransportBinding` status or `BUBU_TRANSPORT_BINDING` contents (with secrets redacted).
- Tag the issue with `kind/bug`, an `area/*` label (operator, transport, sdk, engram, impulse), and `priority/*` if known.

## Requesting enhancements

- Use the [feature template](https://github.com/bubustack/bobravoz-grpc/issues/new?template=feature_request.md) to describe the scenario, scale requirements, and proposed behaviour.
- For CRD or API changes, include the desired spec/field layout and how it interacts with existing bobrapet semantics (`PerStory`, `PerStoryRun`, annotations, etc.).
- If the request spans multiple repos (e.g., SDK + operator), start a thread in [org-wide Discussions](https://github.com/orgs/bubustack/discussions) so we can coordinate.

## Pull requests

1. **Fork & branch** from `main`, keeping each PR focused on a single change-set.
2. **Discuss breaking changes early.** Open an issue before altering CRDs, metrics, or environment contracts so downstream components can prepare.
3. **Run the quality gates** locally:
   ```bash
   make lint            # golangci-lint v2.4.0 (downloaded into ./bin)
   make test            # envtest-backed unit tests
   make generate        # when APIs or deep-copy types change
   make manifests       # updates config/crd/bases/*
   make test-e2e        # optional, spins up Kind to validate transport bindings
   make docker-build IMG=<registry>/bobravoz-grpc:dev
   ```
4. **Document user-facing changes.** Update `README.md`, `SUPPORT.md`, CRD comments, and sample manifests when behaviour or defaults change.
5. **Fill in the PR template.** Include the commands you ran, link to relevant issues (`Fixes #123`), and call out any follow-up work.

## Development workflow

### Prerequisites

- Go 1.25.1 or later (matching the module’s `go` directive).
- Docker or another OCI-compatible builder.
- `make`, `kubectl`, and a Kubernetes cluster (Kind/Minikube is enough for local testing).

### Local setup

1. Fork the repo and clone your fork.
2. `cd bobravoz-grpc`
3. `make lint-config` if you need to verify golangci-lint settings.
4. `make help` lists every available target grouped by category.

### Running tests

```bash
# Fast unit tests (envtest)
make test

# End-to-end Kind tests (optional but recommended for transport changes)
make test-e2e
```

### Commit style & Code of Conduct

- Follow [Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/) (`feat:`, `fix:`, `docs:`, `chore:`) so release tooling can generate changelog entries automatically.
- Participation in this project is governed by the [Contributor Covenant Code of Conduct](./CODE_OF_CONDUCT.md). Report unacceptable behaviour to [conduct@bubustack.com](mailto:conduct@bubustack.com) or via the org Discussions moderation queue.
