# Task spec-c001-kalo-cli A01 Plan

## Goal
Implement native SPEC validate route run and receipts

## Description
Create a bounded implementation slice for `Implement native SPEC validate route run and receipts for workspace change SPEC-C001: SPEC v0.1-alpha external review polish`. This plan is grounded by the task index preflight, but it is not authoritative; confirm predicted files and tests before making edits.

## Workspace Link
```yaml
workspace_id: spec-pdpp-kalo-evidence
workspace_root: "C:\\Users\\brenn\\go\\src\\github.com\\kalo-build\\spec-pdpp-kalo-evidence"
parent_change: SPEC-C001
repo_alias: kalo-cli
```

## Resources
- `A00-index.md`
- `A01-implement-native-spec-validate-route-run-and-rec-result.md`
- `task.json`
- `cmd/kalo/spec.go`
- `internal/specv0/route.go`
- `internal/specv0/catalog.go`
- `cmd/kalo/main.go`
- `cmd/kalo/main_test.go#L23`
- `cmd/kalo/main_test.go#L46`
- `internal/specv0/model.go`

## Starting Context
### Files to Inspect First
- `cmd/kalo/spec.go`
- `internal/specv0/route.go`
- `internal/specv0/catalog.go`
- `cmd/kalo/main.go`

### Tests to Inspect First
- `cmd/kalo/main_test.go#L23`
- `cmd/kalo/main_test.go#L46`

## Expected Change Surface
- `README.md`
- `go.mod`
- `go.sum`
- `cmd/kalo/spec.go`
- `internal/specv0/route.go`
- `internal/specv0/catalog.go`
- `internal/specv0/model.go`
- `internal/specv0/route_test.go`
- `internal/specv0/catalog_test.go`
- `cmd/kalo/main.go`

## Out-of-Scope Areas
- Replanning the whole thread unless evidence says this slice should split or be superseded.
- Broad pack-ranking changes unless they are necessary for this task.
- Treating the generated context as complete without verification.

## Risks

## Success Criteria
- [ ] `kalo spec validate`, `route`, and `run` consume SPEC v0.1-alpha directly.
- [ ] Route discovery uses Processor edges, exact Contract/binding identity, and deterministic tie-breaking—not Composition fixtures.
- [ ] ProcessorImplementation selection verifies exact Wasm bytes and declared capabilities.
- [ ] Unsupported required extensions, required state, effects, representations, and implementations fail closed.
- [ ] Every input, intermediate value, and output validates at its selected binding.
- [ ] Kalo emits linked, schema-valid Processor and Composition Receipts.
- [ ] Adapter/runtime code contains no PDP-Connect, Amazon, DoorDash, or Shop behavior.
- [ ] Focused and full Go tests pass on Windows and Linux CI.

## Tasks
- [ ] Implement and test address parsing, catalog loading, link/digest validation, route discovery, and capability rejection.
- [ ] Bind selected filesystem/WASI implementations through the existing restricted Kalo runtime.
- [ ] Generate and validate exact-byte value digests and linked Receipts.
- [ ] Exercise the public commands against the evidence catalog.
- [ ] Checkpoint the task with actual changed files and test output.

## Decision Gates
- Promote: the workspace was useful enough and misses are actionable.
- Improve: useful start, but incomplete/noisy enough to require template or retrieval changes.
- Rework: task workspace feels like planning overhead or fails to capture useful evidence.
- Rollback: workspace creates false confidence or worsens agent performance.
- Block: external input or a missing prerequisite prevents useful progress.
