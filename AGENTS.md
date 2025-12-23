# Agent Instructions

This project uses **bd** (beads) for issue tracking. Run `bd onboard` to get started.

## Quick Reference

```bash
bd ready              # Find available work
bd show <id>          # View issue details
bd update <id> --status in_progress  # Claim work
bd close <id>         # Complete work
bd sync               # Sync with git
```

## Landing the Plane (Session Completion)

**When ending a work session**, you MUST complete ALL steps below. Work is NOT complete until `git push` succeeds.

**MANDATORY WORKFLOW:**

1. **File issues for remaining work** - Create issues for anything that needs follow-up
2. **Run quality gates** (if code changed) - Tests, linters, builds
3. **Update issue status** - Close finished work, update in-progress items
4. **PUSH TO REMOTE** - This is MANDATORY:
   ```bash
   git pull --rebase
   bd sync
   git push
   git status  # MUST show "up to date with origin"
   ```
5. **Clean up** - Clear stashes, prune remote branches
6. **Verify** - All changes committed AND pushed
7. **Hand off** - Provide context for next session

**CRITICAL RULES:**
- Work is NOT complete until `git push` succeeds
- NEVER stop before pushing - that leaves work stranded locally
- NEVER say "ready to push when you are" - YOU must push
- If push fails, resolve and retry until it succeeds

## Dependency Rules

`bd dep add <issue> <depends-on>` means "issue is blocked by depends-on".

**CRITICAL**: Dependencies flow from parent to child. The parent (epic/task) depends on its children (tasks/subtasks), meaning the parent cannot be completed until all children are done.

| Relationship | Command | Meaning |
|-------------|---------|---------|
| Epic → Tasks | `bd dep add epic-123 task-a` | Epic blocked until task done |
| Epic → Tasks | `bd dep add epic-123 task-b` | Epic blocked until task done |
| Task → Subtasks | `bd dep add task-456 subtask-x` | Task blocked until subtask done |

**Epic pattern**: When an epic contains tasks, the epic MUST depend on ALL its tasks:
```bash
# Create epic with tasks as children
bd create --type epic --title "My Epic"
bd create --type task --title "Task A" --parent epic-123
bd create --type task --title "Task B" --parent epic-123

# IMPORTANT: --parent creates child→parent dependency (backwards!)
# Remove the auto-created dependencies, then add correct parent→child dependencies
bd dep remove task-a epic-123
bd dep remove task-b epic-123

# Epic depends on its tasks (epic blocked until tasks done)
bd dep add epic-123 task-a
bd dep add epic-123 task-b
```
Result: Tasks are ready to work independently, epic closes last (only after all tasks complete).

**Note**: When using `--parent`, beads automatically creates a dependency from child to parent. You MUST reverse these dependencies so the parent (epic) depends on children (tasks), not the other way around.

**Common mistakes**:
- ❌ `bd dep add task epic` - WRONG: This blocks the task until epic is done (backwards!)
- ❌ `bd dep add epic feature-request` - WRONG: Epic should depend on its tasks, not external issues
- ✅ `bd dep add epic task` - CORRECT: Epic blocked until task done
