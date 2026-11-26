# Task hq-77

## Task
CLI cache on agent-main: prune (A) + print line now; size read of the 21 MB wasm; then compress at rest (B) against bars

## Status
packed

## Series
D

## Profile
code-change

## Created At
2026-11-19T08:13:01Z

## Original Query
CLI cache on agent-main: prune (A) + print line now; size read of the 21 MB wasm; then compress at rest (B) against bars

## Repo / Workspace
- Repo: `/data/workspaces/eng_head/kalo-cli`
- Workspace: `/data/workspaces/eng_head/kalo-cli/devspecs/tasks/hq-77`

## Resources
- `task.json`
- `D01-cli-cache-on-agent-main-prune-a-print-line-now-size-read-of-the-plan.md`
- `D01-cli-cache-on-agent-main-prune-a-print-line-now-size-read-of-the-result.md`

## Task Slices
- D01: CLI cache on agent-main: prune (A) + print line now; size read of the 21 MB wasm; then compress at rest (B) against bars. Plan: `D01-cli-cache-on-agent-main-prune-a-print-line-now-size-read-of-the-plan.md`. Result: `D01-cli-cache-on-agent-main-prune-a-print-line-now-size-read-of-the-result.md`.

## Relevant Map Areas
- `cmd/kalo`
- `pkg/registry`
- `pkg/hostfuncs`
- `scripts`

## Likely Primary Files
- `cmd/kalo/main.go` - cmd/kalo/main.go (go)
  Evidence: query term match in path: main; query term match in body: cache; query term match in body: cli
- `pkg/registry/kalo_registry_client.go` - pkg/registry/kalo_registry_client.go (go)
  Evidence: query term match in path: cli; query term match in body: cache; query term match in body: line
- `cmd/kalo/spec_compile_cache.go` - cmd/kalo/spec_compile_cache.go (go)
  Evidence: anchor-first ranking: score 24.000; matches cache, prune, line, read; fields path, title, symbol, body; query term match in path: cache; query term match in body: line
- `cmd/kalo/spec_recipe.go` - cmd/kalo/spec_recipe.go (go)
  Evidence: query term match in body: cache; query term match in body: cli; query term match in body: compress
- `cmd/kalo/wasm_memory_unix.go` - cmd/kalo/wasm_memory_unix.go (go)
  Evidence: relationship expansion: source_manifest_family_recovery; query term match in path: wasm; query term match in body: line
- `cmd/kalo/spec.go` - cmd/kalo/spec.go (go)
  Evidence: query term match in body: cache; query term match in body: cli; query term match in body: line

## Likely Tests
- `cmd/kalo/main_test.go`
  Evidence: relationship expansion: source_manifest_test_reservation; pack tier: related (reserved manifest test with direct query evidence); query term match in path: main
- `cmd/kalo/spec_compile_cache_test.go`
  Evidence: relationship expansion: source_manifest_test_reservation; pack tier: related (reserved manifest test with direct query evidence); query term match in path: cache
- `cmd/kalo/spec_recipe_test.go` - cmd/kalo/spec_recipe_test.go (go)
  Evidence: relationship expansion: source_manifest_family_recovery; query term match in body: cache; query term match in body: cli
- `cmd/kalo/main_test.go#L14` - TestCLI_RootHelp
  Evidence: relationship expansion: source_manifest_loss_safe_preserved; query term match in path: main; query term match in title: cli
- `scripts/test-install.sh`
  Evidence: relationship expansion: source_manifest_family_recovery; query term match in body: line; query term match in body: print
- `cmd/kalo/wasm_memory_unix_test.go#L10` - TestReservedLinearMemoryGrowsInPlace
  Evidence: relationship expansion: source_manifest_relationship_recovery; query term match in path: wasm; query term match in title: line
- `cmd/kalo/spec_memory_test.go` - cmd/kalo/spec_memory_test.go (go)
  Evidence: relationship expansion: source_manifest_relationship_recovery; query term match in body: line; query term match in body: main
- `pkg/hostfuncs/system_test.go#L11` - TestSystemNowDeterministic
  Evidence: relationship expansion: source_manifest_relationship_recovery; query term match in title: now; query term match in body: line
- `pkg/registry/registry_test.go#L14` - TestPluginLockInfo_PluginIdentity
  Evidence: relationship expansion: same_directory_test_companion; pack tier: related (same_directory_test_companion); query term match in body: line

## Likely Docs / Plans / Config
None found in the initial preflight.

## Supporting Context
None found in the initial preflight.

## Related Git Receipts
- `b14ff12` 2026-11-17 - spec run: prune other wazero versions' compile cache; README note (task 53 follow-up) (#19)
  Matched paths: `cmd/kalo/spec.go`, `cmd/kalo/spec_compile_cache.go`, `cmd/kalo/spec_compile_cache_test.go`
- `bfde0a2` 2026-11-17 - spec run: keep compiled plugins on disk, say what runs (hq-53) (#17)
  Matched paths: `cmd/kalo/spec.go`, `cmd/kalo/spec_compile_cache.go`, `cmd/kalo/spec_compile_cache_test.go`
- `aaf859c` 2026-11-14 - spec run: reserve plugin memory, default cap from free memory, one-line OOM
  Matched paths: `cmd/kalo/main.go`, `cmd/kalo/spec.go`, `cmd/kalo/wasm_memory_unix.go`

## Noise Risks
- `cmd/kalo/testdata/specrun/plugin/main.go` - cmd/kalo/testdata/specrun/plugin/main.go (go)
  Evidence: relationship expansion: source_manifest_family_recovery; query term match in path: main; query term match in body: cli

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
- found 6 likely primary file(s)
- found 9 likely test file(s)
- found 3 related Git receipt(s)
- 1 file(s) were downgraded as likely noise

Agent instruction:
Validate the test and integration surface before editing. Record critical misses and distracting inclusions in the slice result or a task checkpoint.

## Suggested Starting Slice
Use `D01-cli-cache-on-agent-main-prune-a-print-line-now-size-read-of-the-plan.md` as the first bounded plan in this task thread. Refine it before editing if primary files, tests, or integration points look incomplete.

## Agent Preflight Checklist
- [ ] Verify the likely primary files against the repo before editing.
- [ ] Search for same-package or same-command tests if test confidence is not high.
- [ ] Check receipt-touched related files before assuming the pack is complete.
- [ ] Record files actually read, edited, tests run, misses, and noise in `D01-cli-cache-on-agent-main-prune-a-print-line-now-size-read-of-the-result.md` or `ds task checkpoint`.
