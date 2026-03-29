---
name: "Feature request"
about: "Propose a new capability for bobravoz-grpc, its charts, or transport integration points"
labels: ["kind/feature", "status/triage"]
---

## Problem statement
What workflow or operational gap are you trying to solve? Include scale, latency, tenancy, or compliance constraints if relevant.

## Proposed change
Describe the behaviour you’d like to see. If this affects transport metadata,
manager configuration, chart values, or adjacent SDK / bobrapet contracts, list
the new fields and defaults.

```
apiVersion: stories.bubustack.io/v1alpha1
kind: Story
spec:
  transports:
    - name: realtime
      transportRef: livekit-default
```

## Affected component(s)
- [ ] bobravoz-grpc controller / hub / connector
- [ ] Helm chart / manifests / install flow
- [ ] CI / release automation
- [ ] bobrapet integration point
- [ ] bubu-sdk-go integration point
- [ ] Engram / Impulse consumer integration point
- [ ] Docs / website

## Alternatives considered
What did you try already? Examples: different transport settings, chart
overrides, external controller logic, or custom connector behavior.

## Additional context
Links, design docs, screenshots, or related issues/discussions.
