# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Run full test + validation suite (same as CI)
yarn test

# Run only unit tests
node tests/run-all.js

# Run individual test files
node tests/lib/utils.test.js
node tests/lib/package-manager.test.js
node tests/hooks/hooks.test.js

# Run only a specific CI validator
node scripts/ci/validate-agents.js
node scripts/ci/validate-skills.js
node scripts/ci/validate-hooks.js
node scripts/ci/validate-commands.js
node scripts/ci/validate-rules.js
node scripts/ci/validate-install-manifests.js
node scripts/ci/validate-no-personal-paths.js

# Lint (ESLint + markdownlint)
yarn lint

# Coverage (80% threshold enforced)
yarn coverage

# ECC CLI — install, inspect, and manage content
node scripts/ecc.js install <profile|component>
node scripts/ecc.js plan <profile>
node scripts/ecc.js catalog --text
node scripts/ecc.js doctor
node scripts/ecc.js repair
node scripts/ecc.js status
node scripts/ecc.js uninstall
```

## Architecture

### Install System

The selective-install system is the core distribution mechanism. Three manifest files in `manifests/` define what gets installed:

- **`install-components.json`** — atomic installable units (individual agents, skill dirs, command files, hook entries, rule files)
- **`install-modules.json`** — named groups of components (e.g. `agents-core`, `workflow-quality`, `framework-language`)
- **`install-profiles.json`** — curated module bundles for different personas (`core`, `developer`, `security`, `research`, etc.)

All three have corresponding JSON schemas in `schemas/`. The CI validator (`validate-install-manifests.js`) checks schema compliance, referential integrity (modules reference real components, profiles reference real modules), and that components point to real files.

`scripts/install-apply.js` resolves a profile → modules → components and copies/symlinks files into the target harness directory. `scripts/install-plan.js` does a dry-run preview. `scripts/doctor.js` / `repair.js` detect and restore drifted files using an install-state SQLite store.

### Content Formats

**Agents** (`agents/*.md`) — Markdown with YAML frontmatter:
```
---
name: agent-name
description: When Claude should invoke this (be specific)
tools: ["Read", "Write", "Edit", "Bash", "Grep", "Glob"]
model: haiku | sonnet | opus
---
```

**Skills** (`skills/<name>/SKILL.md`) — Markdown with frontmatter:
```
---
name: skill-name
description: ...
origin: ECC
---
```
Skills live under `skills/` (curated) or `~/.claude/skills/` (generated/imported). See `docs/SKILL-PLACEMENT-POLICY.md`.

**Commands** (`commands/*.md`) — Markdown with `description:` frontmatter only. Invoked as `/command-name`.

**Hooks** (`hooks/hooks.json`) — JSON with `hooks` key containing event arrays (`PreToolUse`, `PostToolUse`, `SessionStart`, `Stop`). Each entry has `matcher` (expression string), `hooks` array, and `description`.

**Rules** (`rules/`) — Plain Markdown; always-follow guidelines. Split into `common/` and per-language subdirectories.

### Scripts

`scripts/lib/` contains shared utilities used by hooks and install scripts: package manager detection (`package-manager.js`), utilities (`utils.js`), session/state store access (`state-store.js`).

`scripts/hooks/` contains hook implementations called at runtime by the harness.

`scripts/ci/` contains CI-only validators — not part of the runtime.

### Cross-Harness Support

Content is also bundled for other AI harnesses:
- **Cursor**: `.cursor/skills/`, `.cursor/rules/`
- **Codex (OpenAI)**: `.agents/skills/`, `agents/openai.yaml`
- **OpenCode**: `.opencode/`

When adding a skill that should be available on Cursor or Codex, manually copy it to the respective directory and note it in your PR.

## Contributing

File naming: lowercase with hyphens (`python-reviewer.md`, `tdd-workflow.md`).

PR title format: `feat(skills): add rust-patterns skill` / `fix(agents): ...` / `docs: ...`

When adding a new component:
1. Add the file in the appropriate directory
2. Register it in `manifests/install-components.json`
3. Add it to the relevant module in `manifests/install-modules.json` if needed
4. Run `yarn test` — the CI validators will catch schema violations and missing file references

Do not include personal paths (e.g. `/home/yourname/`, `/Users/yourname/`) — `validate-no-personal-paths.js` will fail the build.
