# Task spec-c003-kalo-conformance B01-1 Plan

## Goal
Reject malformed receipt value links

## Description
Create a bounded implementation slice for `Add Kalo test adapter hooks for the SPEC v0.1-alpha conformance suite`. This plan is grounded by the task index preflight, but it is not authoritative; confirm predicted files and tests before making edits.

## Resources
- `B00-index.md`
- `B01-1-reject-malformed-receipt-value-links-result.md`
- `task.json`
- `internal/specv0/catalog.go`
- `internal/specv0/model.go`
- `internal/specv0/conformance_test.go`
- Canonical `SPEC-CONFORMANCE-V0.1-ALPHA.1` Receipt-chain case

## Starting Context
### Files to Inspect First
- `internal/specv0/catalog.go`
- `internal/specv0/model.go`

### Tests to Inspect First
- `internal/specv0/catalog_test.go`
- `internal/specv0/conformance_test.go`

## Expected Change Surface
- `internal/specv0/catalog.go`
- `internal/specv0/conformance_test.go`
- `devspecs/tasks/spec-c003-kalo-conformance/**`

## Out-of-Scope Areas
- Any new SPEC resource, field, or compatibility rule.
- Receipt signing, trust, or generalized provenance profiles.
- Changes to the frozen PDPP common execution path.

## Risks
- Task-related on-disk paths may be missing from the indexed candidate set.
- On-disk paths matched the task but were not indexed: Inspect the warned files or refresh the index before trusting missing context. Evidence: `internal/specv0/catalog_test.go` - on-disk path matched task terms but was not in the indexed candidate set: test, spec; `internal/specv0/route_test.go` - on-disk path matched task terms but was not in the indexed candidate set: test, spec.

## Success Criteria
- [x] Primary implementation surface is verified before edits.
- [x] Route Receipt nodes resolve to matching Processor Receipts.
- [x] Adjacent route Receipts require matching semantic and byte identities.
- [x] A digest mismatch is rejected by a focused test.
- [x] Existing Kalo tests and vet remain clean.
- [ ] A checkpoint records actual files, tests, misses, noise, and decision.

## Tasks
- [x] Inspect receipt models and catalog link validation.
- [x] Refine the slice to the missing canonical Receipt-link invariant.
- [x] Implement exact adjacent boundary matching.
- [x] Run focused tests, the full Go suite, and vet.
- [ ] Update `B01-1-reject-malformed-receipt-value-links-result.md` or run `ds task checkpoint`.

## Decision Gates
- Promote: the workspace was useful enough and misses are actionable.
- Improve: useful start, but incomplete/noisy enough to require template or retrieval changes.
- Rework: task workspace feels like planning overhead or fails to capture useful evidence.
- Rollback: workspace creates false confidence or worsens agent performance.
- Block: external input or a missing prerequisite prevents useful progress.
