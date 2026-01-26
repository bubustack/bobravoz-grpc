# Audit Mode (bobravoz-grpc)

SCOPE:
- Operating in: /Users/kashotyan/personal/bubustack/bobravoz-grpc
- Primary output location: /Users/kashotyan/personal/bubustack/_controller_audit
- Do NOT write audit artifacts outside _controller_audit/.
- Do NOT edit Go code in this repo unless explicitly requested by the operator.
  (Focus is evidence-backed audit artifacts + misconfiguration discovery.)

PRIMARY GOALS:
- Inventory all controller/worker functions in this repo with evidence.
- Detect:
  - SetupWithManager defined but not referenced (misconfiguration candidates)
  - dead-ish paths (POTENTIAL only unless proven)
  - overlap candidates (POTENTIAL only; evidence-driven)

ABSOLUTE RULES:
- NO ASSUMPTIONS.
- Every factual statement added to markdown must have evidence (file:line or exact command output).
- No contradictions across reports; use the correction protocol and log in progress.md.

WORKFLOW:
- Always start with Phase 0 resume in _controller_audit:
  read progress.md and existing artifacts; do not rerun work unnecessarily.
- Run Phase 1 structure + extraction ONLY if this repo is NEW/STALE in the audit.
- Maintain wiring map + watch map + references map as required (evidence-driven).
- Use batch triage + deep-dive mechanics when analysis is requested.

OUTPUTS:
- _controller_audit/bobravoz-grpc/structure.md
- _controller_audit/controllers/<repo_name>/... package markdown
- _controller_audit/controllers/_inventory.md updates (if new packages discovered)
- _controller_audit/controllers/_wiring_map.md updates
- _controller_audit/controllers/_watch_map.md updates
- _controller_audit/_generated/indexes/bobravoz-grpc/... and references/... (if new/stale)

STOP CONDITION:
- Before stopping, run report lint + consistency sweep for modified markdown and log in progress.md.
