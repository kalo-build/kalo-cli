# Task spec-c001-kalo-cli

## Task
Implement native SPEC validate route run and receipts for workspace change SPEC-C001: SPEC v0.1-alpha external review polish

## Status
packed

## Series
A

## Profile
code-change

## Created At
2026-09-03T14:18:29Z

## Original Query
Implement native SPEC validate route run and receipts for workspace change SPEC-C001: SPEC v0.1-alpha external review polish

## Repo / Workspace
- Repo: `C:\Users\brenn\go\src\github.com\kalo-build\kalo-cli`
- Workspace: `C:/Users/brenn/go/src/github.com/kalo-build/kalo-cli/devspecs/tasks/spec-c001-kalo-cli`

## Workspace Link
```yaml
workspace_id: spec-pdpp-kalo-evidence
workspace_root: "C:\\Users\\brenn\\go\\src\\github.com\\kalo-build\\spec-pdpp-kalo-evidence"
parent_change: SPEC-C001
repo_alias: kalo-cli
```

## Resources
- `task.json`
- `A01-implement-native-spec-validate-route-run-and-rec-plan.md`
- `A01-implement-native-spec-validate-route-run-and-rec-result.md`

## Task Slices
- A01: Implement native SPEC validate route run and receipts. Plan: `A01-implement-native-spec-validate-route-run-and-rec-plan.md`. Result: `A01-implement-native-spec-validate-route-run-and-rec-result.md`.

## Relevant Map Areas
- `cmd/kalo`
- `internal/specv0`

## Likely Primary Files
- `cmd/kalo/spec.go` - cmd/kalo/spec.go (go)
  Evidence: pack tier: related (explicit exact plan/track ID reference); exact intent ID: explicit body reference; anchor-first ranking: score 24.000; matches spec, validate, route, v01alpha; fields path, title, symbol, body
- `internal/specv0/route.go` - internal/specv0/route.go (go)
  Evidence: anchor-first ranking: score 24.000; matches route; fields path, title, symbol, body; query term match in path: route; query term match in path: spec
- `internal/specv0/catalog.go` - internal/specv0/catalog.go (go)
  Evidence: query term match in path: spec; query term match in body: alpha; query term match in body: implement
- `cmd/kalo/main.go` - cmd/kalo/main.go (go)
  Evidence: relationship expansion: source_manifest_family_recovery; query term match in body: alpha; query term match in body: change

## Likely Tests
- `cmd/kalo/main_test.go#L23` - TestCLI_RunHelp
  Evidence: relationship expansion: source_manifest_loss_safe_preserved; query term match in title: run
- `cmd/kalo/main_test.go#L46` - TestRunTargetRejectsImpossibleMemoryLimitBeforeReadingProject
  Evidence: relationship expansion: source_manifest_loss_safe_preserved; query term match in title: run

## Likely Docs / Plans / Config
- `internal/specv0/model.go` - internal/specv0/model.go (go)
  Evidence: query term match in path: spec; query term match in body: implement; query term match in body: receipts

## Supporting Context
None found in the initial preflight.

## Related Git Receipts
- `b6323f0` 2026-07-23 - feat(config): add plugin compatibility diagnostics
  Matched paths: `cmd/kalo/main.go`, `cmd/kalo/main_test.go`
- `f725426` 2026-07-23 - fix(runtime): enforce restricted plugin execution
  Matched paths: `cmd/kalo/main.go`, `cmd/kalo/main_test.go`
- `3b914d3` 2026-03-15 - feat: plugin reuse (aliasing) with pipeline config / store overrides, refactor + basic test coverage
  Matched paths: `cmd/kalo/main.go`, `cmd/kalo/main_test.go`

## Noise Risks
None found in the initial preflight.

## Known Knowns
- The preflight found likely primary implementation files.
- The preflight found likely behavior/test artifacts.
- Git receipts provide historical trust evidence for packed paths.

## Known Unknowns

## Confidence Summary
- Primary file confidence: high
- Test coverage confidence: high
- Docs/config coverage confidence: medium
- Git receipt confidence: high
- Noise risk: low
- Pack completeness: high

Why:
- found 4 likely primary file(s)
- found 2 likely test file(s)
- found 3 related Git receipt(s)

Agent instruction:
Validate the test and integration surface before editing. Record critical misses and distracting inclusions in the slice result or a task checkpoint.

## Suggested Starting Slice
Use `A01-implement-native-spec-validate-route-run-and-rec-plan.md` as the first bounded plan in this task thread. Refine it before editing if primary files, tests, or integration points look incomplete.

## Agent Preflight Checklist
- [ ] Verify the likely primary files against the repo before editing.
- [ ] Search for same-package or same-command tests if test confidence is not high.
- [ ] Check receipt-touched related files before assuming the pack is complete.
- [ ] Record files actually read, edited, tests run, misses, and noise in `A01-implement-native-spec-validate-route-run-and-rec-result.md` or `ds task checkpoint`.
- [ ] After all slices are terminal, complete the one-time durable record review at `A00`; record none, recorded artifacts, or a deferred target.
