---
name: commit-staged-files
description: Create a git commit from the currently staged files. Use when the user asks to commit staged changes, make a clean commit message, or ensure the subject and body accurately reflect the staged diff.
---

# Commit Staged Files

Create a single commit from the current index only.
Keep the message specific to the staged changes and do not include unstaged or untracked work.

## Workflow

1. Inspect the staged diff before committing.

Use:

```bash
git diff --cached --stat
git diff --cached --name-only
git diff --cached
```

2. Write the commit message from the diff.

Use a short, specific subject that names the main change.
Write a body only when it adds value, such as:
- multiple related changes
- behavior changes
- migration or compatibility notes

Keep the title and description aligned with the actual staged changes.
Avoid generic subjects like "update" or "fix stuff".

3. Commit only the staged files.

Use `git commit` without staging additional paths.
Do not amend unless the user explicitly asks.

## Message Rules

- Subject: imperative mood, concise, and specific. If there are staged files specifically from one product, have the product name in the beginning of the commit subject, like \[HSM\] or \[Proxy\].
- Body: summarize the staged changes accurately in plain language
- Do not mention work that is not in the index
- If the staged diff spans unrelated changes, describe the broadest honest scope rather than inventing a narrower one
