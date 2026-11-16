# Task spec-c003-kalo-conformance

## Task
Add Kalo test adapter hooks for the SPEC v0.1-alpha conformance suite

## Status
packed

## Series
B

## Profile
code-change

## Created At
2026-09-04T16:33:34Z

## Original Query
Add Kalo test adapter hooks for the SPEC v0.1-alpha conformance suite

## Repo / Workspace
- Repo: `C:\Users\brenn\go\src\github.com\kalo-build\kalo-cli`
- Workspace: `C:/Users/brenn/go/src/github.com/kalo-build/kalo-cli/devspecs/tasks/spec-c003-kalo-conformance`

## Resources
- `task.json`
- `B01-exercise-canonical-vectors-and-preservation-beha-plan.md`
- `B01-exercise-canonical-vectors-and-preservation-beha-result.md`

## Task Slices
- B01: Exercise canonical vectors and preservation behavior. Plan: `B01-exercise-canonical-vectors-and-preservation-beha-plan.md`. Result: `B01-exercise-canonical-vectors-and-preservation-beha-result.md`.

## Relevant Map Areas
- `cmd/kalo`
- `pkg/hostfuncs`
- `pkg/registry`
- `pkg/config`

## Likely Primary Files
- `cmd/kalo/spec.go` - cmd/kalo/spec.go (go)
  Evidence: pack tier: related (explicit exact plan/track ID reference); exact intent ID: explicit body reference; anchor-first ranking: score 10.904; matches kalo, spec, v01alpha; fields path, title, body, symbol
- `cmd/kalo/main.go` - cmd/kalo/main.go (go)
  Evidence: query term match in path: kalo; query term match in body: add; query term match in body: alpha
- `pkg/hostfuncs/host.go` - pkg/hostfuncs/host.go (go)
  Evidence: relationship expansion: source_manifest_family_recovery; query term match in body: add; query term match in body: kalo
- `pkg/registry/kalo_registry_client.go` - pkg/registry/kalo_registry_client.go (go)
  Evidence: relationship expansion: source_manifest_family_recovery; query term match in path: kalo; query term match in body: add
- `pkg/registry/kalo_registry.go` - pkg/registry/kalo_registry.go (go)
  Evidence: relationship expansion: source_manifest_family_recovery; query term match in path: kalo; query term match in body: spec
- `pkg/hostfuncs/system.go` - pkg/hostfuncs/system.go (go)
  Evidence: relationship expansion: filesystem_source_family_recovery; query term match in body: kalo

## Likely Tests
- `cmd/kalo/main_test.go`
  Evidence: query term match in path: kalo; query term match in path: test; test-case behavior signal
- `pkg/config/config_test.go`
  Evidence: relationship expansion: source_manifest_test_reservation; pack tier: related (reserved manifest test with direct query evidence); test-name token anchor
- `pkg/hostfuncs/system_test.go`
  Evidence: relationship expansion: source_manifest_test_reservation; pack tier: related (reserved manifest test with direct query evidence); query term match in path: test
- `cmd/kalo/main_test.go#L52` - TestPluginExecutionDiagnosticIncludesCompatibilityContext
  Evidence: relationship expansion: source_manifest_loss_safe_preserved; query term match in path: kalo; query term match in path: test
- `cmd/kalo/main_test.go#L14` - TestCLI_RootHelp
  Evidence: relationship expansion: source_manifest_loss_safe_preserved; query term match in path: kalo; query term match in path: test
- `pkg/registry/registry_test.go#L14` - TestPluginLockInfo_PluginIdentity
  Evidence: relationship expansion: source_manifest_loss_safe_preserved; query term match in path: test; query term match in body: kalo

## Likely Docs / Plans / Config
- `pkg/config/config.go` - pkg/config/config.go (go)
  Evidence: relationship expansion: filesystem_source_family_recovery; query term match in body: kalo; query term match in body: spec

## Supporting Context
None found in the initial preflight.

## Related Git Receipts
- `cbb63ba` 2026-09-03 - feat: add native SPEC v0.1-alpha adapter
  Matched paths: `cmd/kalo/main.go`, `cmd/kalo/spec.go`
- `3b914d3` 2026-03-15 - feat: plugin reuse (aliasing) with pipeline config / store overrides, refactor + basic test coverage
  Matched paths: `cmd/kalo/main.go`, `cmd/kalo/main_test.go`, `pkg/config/config_test.go`
- `f725426` 2026-07-23 - fix(runtime): enforce restricted plugin execution
  Matched paths: `cmd/kalo/main.go`, `cmd/kalo/main_test.go`, `pkg/hostfuncs/host.go`

## Noise Risks
None found in the initial preflight.

## Freshness Warnings
These on-disk paths match the task wording but were not present in the indexed candidate set. Treat them as stale-index risk, not proof that the initial pack is wrong.

- `internal/specv0/catalog_test.go` - on-disk path matched task terms but was not in the indexed candidate set: test, spec
- `internal/specv0/route_test.go` - on-disk path matched task terms but was not in the indexed candidate set: test, spec

## Risk Cards
Evidence-backed checks to run before trusting the initial task context. These are not required edit targets.

- On-disk paths matched the task but were not indexed [medium, freshness]
  Agent check: Inspect the warned files or refresh the index before trusting missing context.
  Evidence: `internal/specv0/catalog_test.go` - on-disk path matched task terms but was not in the indexed candidate set: test, spec; `internal/specv0/route_test.go` - on-disk path matched task terms but was not in the indexed candidate set: test, spec

## Known Knowns
- The preflight found likely primary implementation files.
- The preflight found likely behavior/test artifacts.
- Git receipts provide historical trust evidence for packed paths.

## Known Unknowns
- Task-related on-disk paths may be missing from the indexed candidate set.

## Confidence Summary
- Primary file confidence: high
- Test coverage confidence: high
- Docs/config coverage confidence: medium
- Git receipt confidence: high
- Noise risk: low
- Pack completeness: high

Why:
- found 6 likely primary file(s)
- found 6 likely test file(s)
- found 3 related Git receipt(s)

Agent instruction:
Validate the test and integration surface before editing. Record critical misses and distracting inclusions in the slice result or a task checkpoint.

## Suggested Starting Slice
Use `B01-exercise-canonical-vectors-and-preservation-beha-plan.md` as the first bounded plan in this task thread. Refine it before editing if primary files, tests, or integration points look incomplete.

## Agent Preflight Checklist
- [ ] Verify the likely primary files against the repo before editing.
- [ ] Search for same-package or same-command tests if test confidence is not high.
- [ ] Check receipt-touched related files before assuming the pack is complete.
- [ ] Record files actually read, edited, tests run, misses, and noise in `B01-exercise-canonical-vectors-and-preservation-beha-result.md` or `ds task checkpoint`.
- [ ] After all slices are terminal, complete the one-time durable record review at `B00`; record none, recorded artifacts, or a deferred target.
