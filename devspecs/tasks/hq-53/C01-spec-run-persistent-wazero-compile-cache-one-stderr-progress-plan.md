# Task hq-53 C01 Plan

## Goal
spec run: persistent wazero compile cache + one stderr progress line (task 51 friction #1)

## Description
Create a bounded implementation slice for `spec run: persistent wazero compile cache + one stderr progress line (task 51 friction #1)`. This plan is grounded by the task index preflight, but it is not authoritative; confirm predicted files and tests before making edits.

## Resources
- `C00-index.md`
- `C01-spec-run-persistent-wazero-compile-cache-one-stderr-progress-result.md`
- `task.json`
- `cmd/kalo/main.go`
- `cmd/kalo/spec.go`
- `cmd/kalo/spec_recipe.go`
- `internal/specv0/value.go`
- `cmd/kalo/spec_memory.go`
- `cmd/kalo/spec_warnings.go`
- `cmd/kalo/wasm_memory_unix.go`
- `internal/specv0/catalog.go`

## Starting Context
### Files to Inspect First
- `cmd/kalo/main.go`
- `cmd/kalo/spec.go`
- `cmd/kalo/spec_recipe.go`
- `internal/specv0/value.go`
- `cmd/kalo/spec_memory.go`
- `cmd/kalo/spec_warnings.go`
- `cmd/kalo/wasm_memory_unix.go`
- `internal/specv0/catalog.go`

### Tests to Inspect First
- `cmd/kalo/spec_memory_test.go`
- `cmd/kalo/spec_recipe_test.go`
- `internal/specv0/value_test.go#L13`
- `cmd/kalo/spec_stage_test.go`
- `cmd/kalo/main_test.go#L14`
- `cmd/kalo/spec_warnings_test.go`
- `cmd/kalo/wasm_memory_unix_test.go#L10`

## Expected Change Surface
- `cmd/kalo/main.go`
- `cmd/kalo/spec.go`
- `cmd/kalo/spec_recipe.go`
- `internal/specv0/value.go`
- `cmd/kalo/spec_memory.go`
- `cmd/kalo/spec_warnings.go`
- `cmd/kalo/wasm_memory_unix.go`
- `internal/specv0/catalog.go`

## Out-of-Scope Areas
- Replanning the whole thread unless evidence says this slice should split or be superseded.
- Broad pack-ranking changes unless they are necessary for this task.
- Treating the generated context as complete without verification.

## Risks
- Pack completeness is not high; verify the working set before editing.
- Initial pack includes downgraded noise candidates; avoid editing them unless verification supports it.

## Success Criteria
- [ ] Primary implementation surface is verified before edits.
- [ ] Relevant tests are found or the test-surface miss is recorded.
- [ ] Changes stay inside the bounded slice.
- [ ] A checkpoint records actual files, tests, misses, noise, and decision.

## Tasks
- [ ] Inspect the predicted primary files.
- [ ] Inspect same-package, same-stem, or receipt-related tests.
- [ ] Refine the slice if context is incomplete.
- [ ] Implement the smallest useful change.
- [ ] Run focused validation.
- [ ] Update `C01-spec-run-persistent-wazero-compile-cache-one-stderr-progress-result.md` or run `ds task checkpoint`.

## Decision Gates
- Promote: the workspace was useful enough and misses are actionable.
- Improve: useful start, but incomplete/noisy enough to require template or retrieval changes.
- Rework: task workspace feels like planning overhead or fails to capture useful evidence.
- Rollback: workspace creates false confidence or worsens agent performance.
- Block: external input or a missing prerequisite prevents useful progress.
