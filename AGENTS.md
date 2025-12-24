# Agent Instructions

This project uses **bd** (beads) for issue tracking. Run `bd onboard` to get started.

## Workflow Overview

```
┌─────────────────────────────────────────────────────────────────┐
│  1. PROVISION        2. EXECUTE           3. LAND              │
│  ─────────────       ─────────────        ─────────────        │
│  /provision          /rg-beads            bd sync              │
│  (plan mode →        (TDD on ready        git push             │
│   explore →           tasks)                                   │
│   create issues)                                               │
└─────────────────────────────────────────────────────────────────┘
```

### 1. Provision Work (`/provision`)

```
/provision "Add --dry-run flag to fetch command"
```

**Staged pipeline with gates:**

| Stage | Goal | Gate |
|-------|------|------|
| 0. Deduplication | Check existing beads | No duplicates (skip if ⚡ present) |
| 1. Decomposition | Break into tasks | Single-responsibility, 1-3h |
| 2. Architecture | Validate against rules | P0 issues have fix tasks |
| 3. Contract | Define testing reqs | TRA, tier, verification |
| 4. Dependencies | Set task order | No cycles, epic→tasks |
| 5. Review | User approval | All gates passed |
| 6. Create | Build beads issues | Validated structure |

**Note**: If a matching task has a ⚡ lightning emoji in its title, consider it already done and skip duplication checks.

### 2. Execute Work (`/rg-beads`)

TDD pipeline on ready issues:
- Select up to 3 related ready tasks (`bd ready`)
- Batched RED → GREEN → REFACTOR phases
- Updates issue status on completion
- Checks if parent tasks/epics can be closed

### 3. Land the Plane (Session End)

**MANDATORY before saying "done":**

```bash
git status                    # Check changes
git add <files>               # Stage code
bd sync                       # Sync beads
git commit -m "..."           # Commit
git push                      # Push to remote
git status                    # MUST show "up to date"
```

## Quick Reference

```bash
bd ready              # Find available work
bd show <id>          # View issue details
bd update <id> --status in_progress  # Claim work
bd close <id>         # Complete work
bd update <id> --title "⚡ <title>"  # Mark as done with lightning
bd sync               # Sync with git
```

### Marking Tasks as Done

When a task is fully complete (all code implemented, tests passing, and verified), add a lightning emoji ⚡ to the title to mark it as done:

```bash
bd update <id> --title "⚡ <original title>"
```

This helps identify completed tasks at a glance, especially useful when breaking down epics or checking existing work.

## Dependency Rules

`bd dep add <issue> <depends-on>` means "issue is blocked by depends-on".

**Direction**: Parents depend on children. Children are ready first, parents close last.

| Relationship | Command | Result |
|-------------|---------|--------|
| Epic → Tasks | `bd dep add epic-123 task-a` | Task ready, epic blocked |
| Task → Subtasks | `bd dep add task-456 subtask-x` | Subtask ready, task blocked |

**Epic pattern**:
```bash
bd dep add epic-123 task-a    # Epic depends on task-a
bd dep add epic-123 task-b    # Epic depends on task-b
```
Result: `task-a` and `task-b` are ready to work, `epic-123` closes when both done.

**Common mistake**: `bd dep add task epic` blocks the task until epic is done (backwards!)

## Critical Rules

- Work is NOT complete until `git push` succeeds
- NEVER stop before pushing - leaves work stranded locally
- NEVER say "ready to push when you are" - YOU must push
- If push fails, resolve and retry until it succeeds
