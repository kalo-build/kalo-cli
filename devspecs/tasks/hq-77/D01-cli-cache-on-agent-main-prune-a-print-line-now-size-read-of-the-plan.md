# Task hq-77 D01 Plan

## Goal
CLI cache on agent-main: prune (A) + print line now; size read of the 21 MB wasm; then compress at rest (B) against bars

## Description
Create a bounded implementation slice for `CLI cache on agent-main: prune (A) + print line now; size read of the 21 MB wasm; then compress at rest (B) against bars`. This plan is grounded by the task index preflight, but it is not authoritative; confirm predicted files and tests before making edits.

## Resources
- `D00-index.md`
- `D01-cli-cache-on-agent-main-prune-a-print-line-now-size-read-of-the-result.md`
- `task.json`
- `cmd/kalo/main.go`
- `pkg/registry/kalo_registry_client.go`
- `cmd/kalo/spec_compile_cache.go`
- `cmd/kalo/spec_recipe.go`
- `cmd/kalo/wasm_memory_unix.go`
- `cmd/kalo/spec.go`
- `cmd/kalo/main_test.go`
- `cmd/kalo/spec_compile_cache_test.go`

## Starting Context
### Files to Inspect First
- `cmd/kalo/main.go`
- `pkg/registry/kalo_registry_client.go`
- `cmd/kalo/spec_compile_cache.go`
- `cmd/kalo/spec_recipe.go`
- `cmd/kalo/wasm_memory_unix.go`
- `cmd/kalo/spec.go`

### Tests to Inspect First
- `cmd/kalo/main_test.go`
- `cmd/kalo/spec_compile_cache_test.go`
- `cmd/kalo/spec_recipe_test.go`
- `cmd/kalo/main_test.go#L14`
- `scripts/test-install.sh`
- `cmd/kalo/wasm_memory_unix_test.go#L10`
- `cmd/kalo/spec_memory_test.go`
- `pkg/hostfuncs/system_test.go#L11`
- `pkg/registry/registry_test.go#L14`

## Expected Change Surface
- `cmd/kalo/main.go`
- `pkg/registry/kalo_registry_client.go`
- `cmd/kalo/spec_compile_cache.go`
- `cmd/kalo/spec_recipe.go`
- `cmd/kalo/wasm_memory_unix.go`
- `cmd/kalo/spec.go`

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
- [ ] Update `D01-cli-cache-on-agent-main-prune-a-print-line-now-size-read-of-the-result.md` or run `ds task checkpoint`.

## Decision Gates
- Promote: the workspace was useful enough and misses are actionable.
- Improve: useful start, but incomplete/noisy enough to require template or retrieval changes.
- Rework: task workspace feels like planning overhead or fails to capture useful evidence.
- Rollback: workspace creates false confidence or worsens agent performance.
- Block: external input or a missing prerequisite prevents useful progress.
