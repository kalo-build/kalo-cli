# Task hq-59

## Task
Prepare, don't merge: kalo-cli README (reach §2c) and page links in the kalo-plugins READMEs, as held PRs for "A: merged"

## Status
packed

## Series
D

## Profile
code-change

## Created At
2026-11-17T15:42:02Z

## Original Query
Prepare, don't merge: kalo-cli README (reach §2c) and page links in the kalo-plugins READMEs, as held PRs for "A: merged"

## Repo / Workspace
- Repo: `/tmp/kcli59`
- Workspace: `/tmp/kcli59/devspecs/tasks/hq-59`

## Resources
- `task.json`
- `D01-prepare-don-t-merge-kalo-cli-readme-reach-2c-and-page-links-in-plan.md`
- `D01-prepare-don-t-merge-kalo-cli-readme-reach-2c-and-page-links-in-result.md`

## Task Slices
- D01: Prepare, don't merge: kalo-cli README (reach §2c) and page links in the kalo-plugins READMEs, as held PRs for "A: merged". Plan: `D01-prepare-don-t-merge-kalo-cli-readme-reach-2c-and-page-links-in-plan.md`. Result: `D01-prepare-don-t-merge-kalo-cli-readme-reach-2c-and-page-links-in-result.md`.

## Relevant Map Areas
- `cmd/kalo`
- `scripts`
- `pkg/registry`
- `internal/specv0`
- `pkg/config`

## Likely Primary Files
- `cmd/kalo/spec_recipe.go` - cmd/kalo/spec_recipe.go (go)
  Evidence: query term match in path: kalo; query term match in body: cli; identifier/body match: kalo-cli
- `cmd/kalo/main.go` - cmd/kalo/main.go (go)
  Evidence: query term match in path: kalo; query term match in body: cli; query term match in body: don
- `cmd/kalo/spec.go` - cmd/kalo/spec.go (go)
  Evidence: query term match in path: kalo; query term match in body: cli; query term match in body: don
- `pkg/registry/kalo_registry_client.go` - pkg/registry/kalo_registry_client.go (go)
  Evidence: query term match in path: cli; query term match in path: kalo; query term match in body: plugins
- `cmd/kalo/spec_stage.go` - cmd/kalo/spec_stage.go (go)
  Evidence: relationship expansion: filesystem_source_family_recovery; query term match in path: kalo; query term match in body: cli
- `cmd/kalo/spec_compile_cache.go` - cmd/kalo/spec_compile_cache.go (go)
  Evidence: relationship expansion: filesystem_source_family_recovery; query term match in path: kalo; scout source preservation: query roots kalo; primary test roots spec
- `scripts/build-all-plugins.sh`
  Evidence: relationship expansion: source_manifest_family_recovery; query term match in path: plugins; query term match in body: kalo
- `scripts/upload-plugins-to-gcs.sh`
  Evidence: relationship expansion: source_manifest_family_recovery; query term match in path: plugins; query term match in body: cli

## Likely Tests
- `cmd/kalo/spec_recipe_test.go` - cmd/kalo/spec_recipe_test.go (go)
  Evidence: query term match in path: kalo; query term match in body: cli; identifier/body match: kalo-plugins
- `cmd/kalo/main_test.go#L14` - TestCLI_RootHelp
  Evidence: relationship expansion: source_manifest_loss_safe_preserved; query term match in path: kalo; query term match in title: cli
- `cmd/kalo/spec_stage_test.go`
  Evidence: relationship expansion: source_manifest_test_reservation; pack tier: related (reserved manifest test with direct query evidence); query term match in path: kalo
- `cmd/kalo/spec_compile_cache_test.go`
  Evidence: relationship expansion: source_manifest_test_reservation; pack tier: related (reserved manifest test with direct query evidence); query term match in path: kalo
- `pkg/config/config_test.go#L11` - TestStepSpecUnmarshalYAML_StringForm
  Evidence: relationship expansion: source_manifest_relationship_recovery; authority prior: classifier confidence
- `cmd/kalo/spec_memory_test.go` - cmd/kalo/spec_memory_test.go (go)
  Evidence: relationship expansion: source_manifest_relationship_recovery; query term match in path: kalo; query term match in body: don
- `scripts/test-install.sh`
  Evidence: relationship expansion: source_manifest_relationship_recovery; query term match in body: don; query term match in body: kalo
- `internal/specv0/conformance_test.go#L47` - TestConformanceAddressVectors
  Evidence: relationship expansion: source_manifest_relationship_recovery
- `pkg/registry/registry_test.go#L14` - TestPluginLockInfo_PluginIdentity
  Evidence: relationship expansion: same_directory_test_companion; pack tier: related (same_directory_test_companion); query term match in body: kalo

## Likely Docs / Plans / Config
None found in the initial preflight.

## Supporting Context
None found in the initial preflight.

## Related Git Receipts
- `b1ea891` 2026-11-14 - spec run: harden recipe runs before the first preview tag
  Matched paths: `cmd/kalo/main.go`, `cmd/kalo/spec.go`, `cmd/kalo/spec_recipe.go`
- `b6323f0` 2026-07-23 - feat(config): add plugin compatibility diagnostics
  Matched paths: `cmd/kalo/main.go`, `pkg/registry/kalo_registry_client.go`, `scripts/build-all-plugins.sh`
- `aaf859c` 2026-11-14 - spec run: reserve plugin memory, default cap from free memory, one-line OOM
  Matched paths: `cmd/kalo/main.go`, `cmd/kalo/spec.go`, `cmd/kalo/spec_stage.go`

## Noise Risks
- `cmd/kalo/testdata/specrun/plugin/main.go` - cmd/kalo/testdata/specrun/plugin/main.go (go)
  Evidence: relationship expansion: source_manifest_family_recovery; query term match in path: kalo; query term match in body: cli

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
- found 9 likely test file(s)
- found 3 related Git receipt(s)
- 1 file(s) were downgraded as likely noise

Agent instruction:
Validate the test and integration surface before editing. Record critical misses and distracting inclusions in the slice result or a task checkpoint.

## Suggested Starting Slice
Use `D01-prepare-don-t-merge-kalo-cli-readme-reach-2c-and-page-links-in-plan.md` as the first bounded plan in this task thread. Refine it before editing if primary files, tests, or integration points look incomplete.

## Agent Preflight Checklist
- [ ] Verify the likely primary files against the repo before editing.
- [ ] Search for same-package or same-command tests if test confidence is not high.
- [ ] Check receipt-touched related files before assuming the pack is complete.
- [ ] Record files actually read, edited, tests run, misses, and noise in `D01-prepare-don-t-merge-kalo-cli-readme-reach-2c-and-page-links-in-result.md` or `ds task checkpoint`.
