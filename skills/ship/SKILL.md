---
name: ship
description: Ship Nanotrace repository changes end-to-end. Use when Codex is asked to ship, push, publish, commit and push, open a PR, prepare a release branch, or otherwise move local Nanotrace work from the working tree toward GitHub. Covers worktree review, focused validation, intentional commits, branch push, optional PR creation, and safe handoff. Do not use for live cloud deploys unless the user explicitly asks to deploy in the current turn.
---

# Ship

Use this skill to move completed Nanotrace changes from local files to a clean
GitHub-ready state. Default to shipping the current repo state, not inventing new
scope.

## Guardrails

- Read root `AGENTS.md` first. For Node/npm commands, load NVM and use Node
  `22.17.1`:

```sh
export NVM_DIR="$HOME/.nvm"; . "$NVM_DIR/nvm.sh"; nvm use 22.17.1
```

- Preserve user work. Never revert, restage, or overwrite unrelated changes
  unless the user explicitly asks.
- Do not amend existing commits unless explicitly requested.
- Do not run destructive git commands such as `git reset --hard` or
  `git checkout --` unless explicitly requested.
- Do not run cloud-mutating deploy, destroy, scale, DNS, or production commands
  unless the user explicitly asks for that action in the current turn.
- If live deployment guidance is requested, use the
  `nanotrace-deployment-lifecycle` skill as the deployment companion.

## Workflow

1. Inspect state.

```sh
git status --short
git branch --show-current
git diff --stat
git diff
```

Identify which files are part of the requested ship and which appear unrelated.
If unrelated changes are present, leave them unstaged and mention them.

2. Validate the shipped scope.

Choose the narrowest checks that prove the change. Prefer focused tests over a
slow blanket suite, but run broader checks when the change crosses boundaries.

For docs-only changes:

```sh
python3 /Users/johnsuh/.codex/skills/.system/skill-creator/scripts/quick_validate.py skills/ship
```

For Rust changes:

```sh
cargo fmt
cargo clippy --all-targets --all-features
cargo test --all-features
```

For Node/UI/TypeScript changes:

```sh
export NVM_DIR="$HOME/.nvm"; . "$NVM_DIR/nvm.sh"; nvm use 22.17.1
npm run typecheck
```

For local ingest/query path changes:

```sh
export NVM_DIR="$HOME/.nvm"; . "$NVM_DIR/nvm.sh"; nvm use 22.17.1
npm run integration:kafka
```

3. Review final diff.

```sh
git diff --stat
git diff
```

Summarize the actual change, any validation run, and any known gaps before
committing if there is ambiguity in scope.

4. Commit intentionally.

Stage only files that belong to the ship:

```sh
git add <paths>
git diff --cached --stat
git diff --cached
git commit -m "<concise imperative message>"
```

Use a message that describes the user-facing or repo-facing outcome, for
example `Simplify README` or `Add ship skill`.

5. Push the current branch.

```sh
git branch --show-current
git remote -v
git push -u origin HEAD
```

If the branch already tracks a remote, `git push` is sufficient.

6. Open or update a PR when requested.

Prefer the GitHub plugin workflow if available. Otherwise use `gh`:

```sh
gh pr status
gh pr create --draft --title "<title>" --body "<body>"
```

For an existing PR, update the body or comment with the shipped summary and
validation result instead of creating a duplicate PR.

## Final Handoff

Report only what matters:

- Commit SHA and branch.
- Push target or PR URL if created.
- Validation commands run and result.
- Any skipped checks or residual risks.
- Any unrelated local changes left untouched.
