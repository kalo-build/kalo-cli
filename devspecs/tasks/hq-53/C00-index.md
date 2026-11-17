# Task hq-53

## Task
spec run: persistent wazero compile cache + one stderr progress line (task 51 friction #1)

## Status
packed

## Series
C

## Profile
code-change

## Created At
2026-11-17T14:16:50Z

## Original Query
spec run: persistent wazero compile cache + one stderr progress line (task 51 friction #1)

## Repo / Workspace
- Repo: `/data/workspaces/eng1/src/kalo-cli`
- Workspace: `/data/workspaces/eng1/src/kalo-cli/devspecs/tasks/hq-53`

## Resources
- `task.json`
- `C01-spec-run-persistent-wazero-compile-cache-one-stderr-progress-plan.md`
- `C01-spec-run-persistent-wazero-compile-cache-one-stderr-progress-result.md`

## Task Slices
- C01: spec run: persistent wazero compile cache + one stderr progress line (task 51 friction #1). Plan: `C01-spec-run-persistent-wazero-compile-cache-one-stderr-progress-plan.md`. Result: `C01-spec-run-persistent-wazero-compile-cache-one-stderr-progress-result.md`.

## Relevant Map Areas
- `cmd/kalo`
- `internal/specv0`

## Likely Primary Files
- `cmd/kalo/main.go` - cmd/kalo/main.go (go)
  Evidence: query term match in body: cache; query term match in body: compile; query term match in body: line
- `cmd/kalo/spec.go` - cmd/kalo/spec.go (go)
  Evidence: query term match in path: spec; query term match in body: cache; query term match in body: compile
- `cmd/kalo/spec_recipe.go` - cmd/kalo/spec_recipe.go (go)
  Evidence: query term match in path: spec; query term match in body: cache; query term match in body: compile
- `internal/specv0/value.go` - internal/specv0/value.go (go)
  Evidence: query term match in path: spec; query term match in body: line; query term match in body: one
- `cmd/kalo/spec_memory.go` - cmd/kalo/spec_memory.go (go)
  Evidence: query term match in path: spec; query term match in body: compile; query term match in body: line
- `cmd/kalo/spec_warnings.go` - cmd/kalo/spec_warnings.go (go)
  Evidence: relationship expansion: source_manifest_loss_safe_preserved; query term match in path: spec; query term match in body: line
- `cmd/kalo/wasm_memory_unix.go` - cmd/kalo/wasm_memory_unix.go (go)
  Evidence: query term match in body: line; query term match in body: one; query term match in body: task
- `internal/specv0/catalog.go` - internal/specv0/catalog.go (go)
  Evidence: relationship expansion: source_manifest_family_recovery; query term match in path: spec; query term match in body: compile

## Likely Tests
- `cmd/kalo/spec_memory_test.go` - cmd/kalo/spec_memory_test.go (go)
  Evidence: relationship expansion: source_manifest_family_recovery; query term match in path: spec; query term match in body: line
- `cmd/kalo/spec_recipe_test.go` - cmd/kalo/spec_recipe_test.go (go)
  Evidence: relationship expansion: source_manifest_family_recovery; query term match in path: spec; query term match in body: cache
- `internal/specv0/value_test.go#L13` - TestDecodeValueYAMLMatchesJSON
  Evidence: relationship expansion: source_manifest_loss_safe_preserved; query term match in path: spec; query term match in body: line
- `cmd/kalo/spec_stage_test.go` - cmd/kalo/spec_stage_test.go (go)
  Evidence: relationship expansion: source_manifest_relationship_recovery; query term match in path: spec; query term match in body: line
- `cmd/kalo/main_test.go#L14` - TestCLI_RootHelp
  Evidence: relationship expansion: source_manifest_relationship_recovery; query term match in body: line; query term match in body: run
- `cmd/kalo/spec_warnings_test.go` - cmd/kalo/spec_warnings_test.go (go)
  Evidence: relationship expansion: source_manifest_loss_safe_preserved; query term match in path: spec; query term match in body: line
- `cmd/kalo/wasm_memory_unix_test.go#L10` - TestReservedLinearMemoryGrowsInPlace
  Evidence: relationship expansion: test_companion; pack tier: related (test_companion); query term match in title: line

## Likely Docs / Plans / Config
None found in the initial preflight.

## Supporting Context
None found in the initial preflight.

## Related Git Receipts
- `aaf859c` 2026-11-14 - spec run: reserve plugin memory, default cap from free memory, one-line OOM
  Matched paths: `cmd/kalo/main.go`, `cmd/kalo/spec.go`, `cmd/kalo/spec_memory.go`
- `f874e8f` 2026-11-14 - feat(spec): YAML values, file-set outputs and KALO:PLUGIN-IO for kalo spec run
  Matched paths: `cmd/kalo/main.go`, `cmd/kalo/spec.go`, `internal/specv0/catalog.go`
- `cfffb4a` 2026-11-14 - spec run: forward plugin warning: lines in quiet recipe runs (#10)
  Matched paths: `cmd/kalo/spec.go`, `cmd/kalo/spec_warnings.go`

## Noise Risks
- `cmd/kalo/testdata/specrun/plugin/main.go` - cmd/kalo/testdata/specrun/plugin/main.go (go)
  Evidence: query term match in path: run; query term match in path: spec; query term match in body: line

## Known Knowns
- The preflight found likely primary implementation files.
- The preflight found likely behavior/test artifacts.
- Git receipts provide historical trust evidence for packed paths.

## Known Unknowns
- Pack completeness is not high; verify the working set before editing.

## Confidence Summary
- Primary file confidence: high
- Test coverage confidence: high
- Docs/config coverage confidence: low
- Git receipt confidence: high
- Noise risk: medium
- Pack completeness: medium

Why:
- found 8 likely primary file(s)
- found 7 likely test file(s)
- found 3 related Git receipt(s)
- 1 file(s) were downgraded as likely noise

Agent instruction:
Validate the test and integration surface before editing. Record critical misses and distracting inclusions in the slice result or a task checkpoint.

## Suggested Starting Slice
Use `C01-spec-run-persistent-wazero-compile-cache-one-stderr-progress-plan.md` as the first bounded plan in this task thread. Refine it before editing if primary files, tests, or integration points look incomplete.

## Agent Preflight Checklist
- [ ] Verify the likely primary files against the repo before editing.
- [ ] Search for same-package or same-command tests if test confidence is not high.
- [ ] Check receipt-touched related files before assuming the pack is complete.
- [ ] Record files actually read, edited, tests run, misses, and noise in `C01-spec-run-persistent-wazero-compile-cache-one-stderr-progress-result.md` or `ds task checkpoint`.
