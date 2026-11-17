# Task hq-59 D01 Plan

## Goal
Prepare, don't merge: kalo-cli README (reach §2c) and page links in the kalo-plugins READMEs, as held PRs for "A: merged"

## Description
Create a bounded implementation slice for `Prepare, don't merge: kalo-cli README (reach §2c) and page links in the kalo-plugins READMEs, as held PRs for "A: merged"`. This plan is grounded by the task index preflight, but it is not authoritative; confirm predicted files and tests before making edits.

## Resources
- `D00-index.md`
- `D01-prepare-don-t-merge-kalo-cli-readme-reach-2c-and-page-links-in-result.md`
- `task.json`
- `cmd/kalo/spec_recipe.go`
- `cmd/kalo/main.go`
- `cmd/kalo/spec.go`
- `pkg/registry/kalo_registry_client.go`
- `cmd/kalo/spec_stage.go`
- `cmd/kalo/spec_compile_cache.go`
- `scripts/build-all-plugins.sh`
- `scripts/upload-plugins-to-gcs.sh`

## Starting Context
### Files to Inspect First
- `cmd/kalo/spec_recipe.go`
- `cmd/kalo/main.go`
- `cmd/kalo/spec.go`
- `pkg/registry/kalo_registry_client.go`
- `cmd/kalo/spec_stage.go`
- `cmd/kalo/spec_compile_cache.go`
- `scripts/build-all-plugins.sh`
- `scripts/upload-plugins-to-gcs.sh`

### Tests to Inspect First
- `cmd/kalo/spec_recipe_test.go`
- `cmd/kalo/main_test.go#L14`
- `cmd/kalo/spec_stage_test.go`
- `cmd/kalo/spec_compile_cache_test.go`
- `pkg/config/config_test.go#L11`
- `cmd/kalo/spec_memory_test.go`
- `scripts/test-install.sh`
- `internal/specv0/conformance_test.go#L47`
- `pkg/registry/registry_test.go#L14`

## Expected Change Surface
- `cmd/kalo/spec_recipe.go`
- `cmd/kalo/main.go`
- `cmd/kalo/spec.go`
- `pkg/registry/kalo_registry_client.go`
- `cmd/kalo/spec_stage.go`
- `cmd/kalo/spec_compile_cache.go`
- `scripts/build-all-plugins.sh`
- `scripts/upload-plugins-to-gcs.sh`

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
- [ ] Update `D01-prepare-don-t-merge-kalo-cli-readme-reach-2c-and-page-links-in-result.md` or run `ds task checkpoint`.

## Decision Gates
- Promote: the workspace was useful enough and misses are actionable.
- Improve: useful start, but incomplete/noisy enough to require template or retrieval changes.
- Rework: task workspace feels like planning overhead or fails to capture useful evidence.
- Rollback: workspace creates false confidence or worsens agent performance.
- Block: external input or a missing prerequisite prevents useful progress.
