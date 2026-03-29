---
name: "Bug report"
about: "Report a reproducible issue in bobravoz-grpc or its transport integration points"
labels: ["kind/bug", "status/triage"]
---

## Area
- [ ] Controller / manager
- [ ] Hub routing / buffering
- [ ] Connector / P2P runtime
- [ ] Webhook / manifest / chart
- [ ] CI / release automation
- [ ] Docs / website

## Related component(s) involved (optional)
- [ ] bobrapet input / CRD behavior
- [ ] bubu-sdk-go client / runtime behavior
- [ ] Engram consumer behavior
- [ ] Impulse / trigger behavior

## What happened?
Tell us what broke. Include the Story/StoryRun status, the expected behaviour, and what you observed instead.

## Minimal reproduction
1. Inputs/Story snippet (YAML or JSON)
2. Commands you ran (`kubectl`, `make`, etc.)
3. Cluster details (Kubernetes version, Kind/Minikube/managed cluster)

```
apiVersion: stories.bubustack.io/v1alpha1
kind: Story
metadata:
  name: example
spec:
  ...
```

## Logs & traces
- `kubectl logs` for controllers or Engrams (set `BUBU_DEBUG=true` if possible)
- Relevant excerpts from `storyrun` / `steprun` status
- `TransportBinding`, hub, or connector logs if streaming is impacted

## Additional context
Anything else we should know? For example, custom overrides, secrets/providers, or recent upgrades.
