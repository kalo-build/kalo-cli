# Task hq-84

## Task
kalo-cli release PR week 48 (agent-main → main): open Thu 26 Nov, note for request on Fri 27 Nov

## Status
packed

## Series
E

## Profile
code-change

## Created At
2026-11-26T08:00:40Z

## Original Query
kalo-cli release PR week 48 (agent-main → main): open Thu 26 Nov, note for request on Fri 27 Nov

## Repo / Workspace
- Repo: `/data/workspaces/eng_head/kalo-cli`
- Workspace: `/data/workspaces/eng_head/kalo-cli/devspecs/tasks/hq-84`

## Resources
- `task.json`
- `E01-kalo-cli-release-pr-week-48-agent-main-main-open-thu-26-nov-note-plan.md`
- `E01-kalo-cli-release-pr-week-48-agent-main-main-open-thu-26-nov-note-result.md`

## Task Slices
- E01: kalo-cli release PR week 48 (agent-main → main): open Thu 26 Nov, note for request on Fri 27 Nov. Plan: `E01-kalo-cli-release-pr-week-48-agent-main-main-open-thu-26-nov-note-plan.md`. Result: `E01-kalo-cli-release-pr-week-48-agent-main-main-open-thu-26-nov-note-result.md`.

## Relevant Map Areas
- `cmd/kalo`
- `pkg/registry`
- `scripts`

## Likely Primary Files
- `cmd/kalo/main.go` - cmd/kalo/main.go (go)
  Evidence: anchor-first ranking: score 24.000; matches main; fields path, title, symbol, body; query term match in path: kalo; query term match in path: main
- `cmd/kalo/spec_recipe.go` - cmd/kalo/spec_recipe.go (go)
  Evidence: query term match in path: kalo; query term match in body: cli; identifier/body match: kalo-cli
- `cmd/kalo/spec.go` - cmd/kalo/spec.go (go)
  Evidence: query term match in path: kalo; query term match in body: cli; identifier/body match: kalo-cli
- `cmd/kalo/spec_stage.go` - cmd/kalo/spec_stage.go (go)
  Evidence: relationship expansion: filesystem_source_family_recovery; query term match in path: kalo; query term match in body: cli
- `pkg/registry/kalo_registry_client.go` - pkg/registry/kalo_registry_client.go (go)
  Evidence: relationship expansion: source_manifest_family_recovery; query term match in path: cli; query term match in path: kalo

## Likely Tests
- `cmd/kalo/main_test.go`
  Evidence: relationship expansion: source_manifest_test_reservation; pack tier: related (reserved manifest test with direct query evidence); query term match in path: kalo
- `cmd/kalo/spec_recipe_test.go` - cmd/kalo/spec_recipe_test.go (go)
  Evidence: relationship expansion: source_manifest_family_recovery; query term match in path: kalo; query term match in body: cli
- `cmd/kalo/spec_compile_cache_test.go#L17` - TestMain
  Evidence: relationship expansion: source_manifest_loss_safe_preserved; anchor-first ranking: score 24.000; matches main; fields title, test_name, symbol, body; exact test-name anchor
- `cmd/kalo/main_test.go#L14` - TestCLI_RootHelp
  Evidence: relationship expansion: source_manifest_loss_safe_preserved; query term match in path: kalo; query term match in path: main
- `cmd/kalo/spec_stage_test.go`
  Evidence: relationship expansion: source_manifest_test_reservation; pack tier: related (reserved manifest test with direct query evidence); query term match in path: kalo
- `scripts/test-install.sh`
  Evidence: relationship expansion: source_manifest_relationship_recovery; query term match in body: kalo; query term match in body: release
- `cmd/kalo/spec_memory_test.go` - cmd/kalo/spec_memory_test.go (go)
  Evidence: relationship expansion: source_manifest_loss_safe_preserved; query term match in path: kalo; query term match in body: main

## Likely Docs / Plans / Config
None found in the initial preflight.

## Supporting Context
None found in the initial preflight.

## Related Git Receipts
- `b1ea891` 2026-11-14 - spec run: harden recipe runs before the first preview tag
  Matched paths: `cmd/kalo/main.go`, `cmd/kalo/spec.go`, `cmd/kalo/spec_recipe.go`
- `f874e8f` 2026-11-14 - feat(spec): YAML values, file-set outputs and KALO:PLUGIN-IO for kalo spec run
  Matched paths: `cmd/kalo/main.go`, `cmd/kalo/spec.go`, `cmd/kalo/spec_stage.go`
- `aaf859c` 2026-11-14 - spec run: reserve plugin memory, default cap from free memory, one-line OOM
  Matched paths: `cmd/kalo/main.go`, `cmd/kalo/spec.go`, `cmd/kalo/spec_stage.go`

## Noise Risks
- `cmd/kalo/testdata/specrun/plugin/main.go` - cmd/kalo/testdata/specrun/plugin/main.go (go)
  Evidence: relationship expansion: source_manifest_family_recovery; query term match in path: kalo; query term match in path: main
- `cmd/kalo/spec_compile_cache_test.go` - cmd/kalo/spec_compile_cache_test.go (go)
  Evidence: relationship expansion: source_manifest_family_recovery; query term match in path: kalo; query term match in body: main

## Freshness Warnings
These on-disk paths match the task wording but were not present in the indexed candidate set. Treat them as stale-index risk, not proof that the initial pack is wrong.

- `.github/workflows/release.yml` - on-disk path matched task terms but was not in the indexed candidate set: release, thu

## Risk Cards
Evidence-backed checks to run before trusting the initial task context. These are not required edit targets.

- On-disk paths matched the task but were not indexed [medium, freshness]
  Agent check: Inspect the warned files or refresh the index before trusting missing context.
  Evidence: `.github/workflows/release.yml` - on-disk path matched task terms but was not in the indexed candidate set: release, thu

## Known Knowns
- The preflight found likely primary implementation files.
- The preflight found likely behavior/test artifacts.
- Git receipts provide historical trust evidence for packed paths.

## Known Unknowns
- Task-related on-disk paths may be missing from the indexed candidate set.
- Pack completeness is not high; verify the working set before editing.

## Confidence Summary
- Primary file confidence: high
- Test coverage confidence: high
- Docs/config coverage confidence: low
- Git receipt confidence: high
- Noise risk: medium
- Pack completeness: medium

Why:
- found 5 likely primary file(s)
- found 7 likely test file(s)
- found 3 related Git receipt(s)
- 2 file(s) were downgraded as likely noise

Agent instruction:
Validate the test and integration surface before editing. Record critical misses and distracting inclusions in the slice result or a task checkpoint.

## Suggested Starting Slice
Use `E01-kalo-cli-release-pr-week-48-agent-main-main-open-thu-26-nov-note-plan.md` as the first bounded plan in this task thread. Refine it before editing if primary files, tests, or integration points look incomplete.

## Agent Preflight Checklist
- [ ] Verify the likely primary files against the repo before editing.
- [ ] Search for same-package or same-command tests if test confidence is not high.
- [ ] Check receipt-touched related files before assuming the pack is complete.
- [ ] Record files actually read, edited, tests run, misses, and noise in `E01-kalo-cli-release-pr-week-48-agent-main-main-open-thu-26-nov-note-result.md` or `ds task checkpoint`.
