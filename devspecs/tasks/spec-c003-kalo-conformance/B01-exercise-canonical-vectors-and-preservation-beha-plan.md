# Task spec-c003-kalo-conformance B01 Plan

## Goal
Exercise canonical vectors and preservation behavior

## Description
Create a bounded implementation slice for `Add Kalo test adapter hooks for the SPEC v0.1-alpha conformance suite`. This plan is grounded by the task index preflight, but it is not authoritative; confirm predicted files and tests before making edits.

## Resources
- `B00-index.md`
- `B01-exercise-canonical-vectors-and-preservation-beha-result.md`
- `task.json`
- `cmd/kalo/spec.go`
- `cmd/kalo/main.go`
- `pkg/hostfuncs/host.go`
- `pkg/registry/kalo_registry_client.go`
- `pkg/registry/kalo_registry.go`
- `pkg/hostfuncs/system.go`
- `cmd/kalo/main_test.go`
- `pkg/config/config_test.go`

## Starting Context
### Files to Inspect First
- `cmd/kalo/spec.go`
- `cmd/kalo/main.go`
- `pkg/hostfuncs/host.go`
- `pkg/registry/kalo_registry_client.go`
- `pkg/registry/kalo_registry.go`
- `pkg/hostfuncs/system.go`

### Tests to Inspect First
- `cmd/kalo/main_test.go`
- `pkg/config/config_test.go`
- `pkg/hostfuncs/system_test.go`
- `cmd/kalo/main_test.go#L52`
- `cmd/kalo/main_test.go#L14`
- `pkg/registry/registry_test.go#L14`

## Expected Change Surface
- `internal/specv0/conformance_test.go`
- `devspecs/tasks/spec-c003-kalo-conformance/**`

## Out-of-Scope Areas
- Replanning the whole thread unless evidence says this slice should split or be superseded.
- Broad pack-ranking changes unless they are necessary for this task.
- Treating the generated context as complete without verification.
- Production CLI/runtime behavior; the adapter hook is test-only.
- Vendoring canonical SPEC fixtures into Kalo.

## Risks
- Task-related on-disk paths may be missing from the indexed candidate set.
- On-disk paths matched the task but were not indexed: Inspect the warned files or refresh the index before trusting missing context. Evidence: `internal/specv0/catalog_test.go` - on-disk path matched task terms but was not in the indexed candidate set: test, spec; `internal/specv0/route_test.go` - on-disk path matched task terms but was not in the indexed candidate set: test, spec.

## Success Criteria
- [x] Primary implementation surface is verified before edits.
- [x] Relevant `internal/specv0` tests and models were inspected.
- [x] Canonical address vectors execute against Kalo's parser.
- [x] Optional extensions survive a Kalo model round trip.
- [x] Exact-byte digest mismatch rejection is directly exercised.
- [x] Changes stay inside the bounded test-only slice.
- [ ] A checkpoint records actual files, tests, misses, noise, and decision.

## Tasks
- [x] Inspect `internal/specv0` models and existing route/catalog tests.
- [x] Refine the slice to a test-only external-vector adapter hook.
- [x] Add canonical address, extension-preservation, and digest tests.
- [x] Run focused validation with `SPEC_CONFORMANCE_ROOT`.
- [ ] Update `B01-exercise-canonical-vectors-and-preservation-beha-result.md` or run `ds task checkpoint`.

## Decision Gates
- Promote: the workspace was useful enough and misses are actionable.
- Improve: useful start, but incomplete/noisy enough to require template or retrieval changes.
- Rework: task workspace feels like planning overhead or fails to capture useful evidence.
- Rollback: workspace creates false confidence or worsens agent performance.
- Block: external input or a missing prerequisite prevents useful progress.
