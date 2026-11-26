# Task hq-84 E01 Plan

## Goal
kalo-cli release PR week 48 (agent-main → main): open Thu 26 Nov, note for request on Fri 27 Nov

## Description
Create a bounded implementation slice for `kalo-cli release PR week 48 (agent-main → main): open Thu 26 Nov, note for request on Fri 27 Nov`. This plan is grounded by the task index preflight, but it is not authoritative; confirm predicted files and tests before making edits.

## Resources
- `E00-index.md`
- `E01-kalo-cli-release-pr-week-48-agent-main-main-open-thu-26-nov-note-result.md`
- `task.json`
- `cmd/kalo/main.go`
- `cmd/kalo/spec_recipe.go`
- `cmd/kalo/spec.go`
- `cmd/kalo/spec_stage.go`
- `pkg/registry/kalo_registry_client.go`
- `cmd/kalo/main_test.go`
- `cmd/kalo/spec_recipe_test.go`
- `cmd/kalo/spec_compile_cache_test.go#L17`

## Starting Context
### Files to Inspect First
- `cmd/kalo/main.go`
- `cmd/kalo/spec_recipe.go`
- `cmd/kalo/spec.go`
- `cmd/kalo/spec_stage.go`
- `pkg/registry/kalo_registry_client.go`

### Tests to Inspect First
- `cmd/kalo/main_test.go`
- `cmd/kalo/spec_recipe_test.go`
- `cmd/kalo/spec_compile_cache_test.go#L17`
- `cmd/kalo/main_test.go#L14`
- `cmd/kalo/spec_stage_test.go`
- `scripts/test-install.sh`
- `cmd/kalo/spec_memory_test.go`

## Expected Change Surface
- `cmd/kalo/main.go`
- `cmd/kalo/spec_recipe.go`
- `cmd/kalo/spec.go`
- `cmd/kalo/spec_stage.go`
- `pkg/registry/kalo_registry_client.go`

## Out-of-Scope Areas
- Replanning the whole thread unless evidence says this slice should split or be superseded.
- Broad pack-ranking changes unless they are necessary for this task.
- Treating the generated context as complete without verification.

## Risks
- Task-related on-disk paths may be missing from the indexed candidate set.
- Pack completeness is not high; verify the working set before editing.
- On-disk paths matched the task but were not indexed: Inspect the warned files or refresh the index before trusting missing context. Evidence: `.github/workflows/release.yml` - on-disk path matched task terms but was not in the indexed candidate set: release, thu.
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
- [ ] Update `E01-kalo-cli-release-pr-week-48-agent-main-main-open-thu-26-nov-note-result.md` or run `ds task checkpoint`.

## Decision Gates
- Promote: the workspace was useful enough and misses are actionable.
- Improve: useful start, but incomplete/noisy enough to require template or retrieval changes.
- Rework: task workspace feels like planning overhead or fails to capture useful evidence.
- Rollback: workspace creates false confidence or worsens agent performance.
- Block: external input or a missing prerequisite prevents useful progress.
