# Webhook prompts

Dispatch repos declare one block per repository; the relay expands it into the
four locked endpoints (`review`, `fix`, `merge`, `merged`). Custom generic
endpoints keep the manual shape:

```yaml
webhooks:
  defaults:
    comment_trigger:
      review: ['please re-review']
      fix: ['please fix review', 'please fix ci']
    limits:
      max_runs_per_pr: 3
      window: 60m
  repos:
    - repo: acme/widget
      provider: github
      platform: discord
      channel_id: '<channel-id>'
      secret: <your-webhook-secret> # one secret shared by the 4 hooks
      checkout: /home/operator/projects/widget
      # automation: {default: review-only, authors: {owner-login: full}}
  endpoints:
    - name: sentry-alerts
      path: /webhooks/sentry-alerts
      workflow: custom # locked review/fix/merge/merged come from repos: above
      secret: <your-webhook-secret>
      platform: telegram
      channel_id: '<channel-id>'
      prompt_file: webhooks/example-review.md
      thread: true
      workspace:
        type: none
      admit:
        - event: ping
```

## Things to know

- **Startup load.** Locked prompt files are read when the service starts:
  `webhooks/github-review.md`, `webhooks/github-pr-fix.md`,
  `webhooks/github-merge.md`, `webhooks/github-merged.md` (resolved vs the
  config directory, same rules as `prompt_file`). Edit a prompt, then restart
  the service for it to take effect. Missing files fail startup with the exact
  path to create.
- **Prompts are operator configuration.** Keep your real prompts outside this repository —
  they describe your own workflow policy. This directory ships the mechanism and one
  generic example (`example-review.md`) only.
- **Go template rendering.** Prompts are rendered as Go templates with
  `Option("missingkey=error")`: referencing a field that is absent for the delivered event
  fails the render, so reference only fields the endpoint's events are guaranteed to carry.
- **Template values.**
  - `{{.webhook.<field>}}` — the normalized envelope, whose names are stable across
    providers and events: `event_type`, `action`, `repository`, `pr_number`, `pr_url`,
    `title`, `head_branch`, `base_branch`, `default_branch`, `review_state`,
    `review_verdict`, `has_findings`, `pr_author`, `comment_trigger`, `repository`.
  - `{{.payload.<path>}}` — the raw webhook payload, for anything the envelope does not
    normalize.
- **Untrusted input.** Webhook fields, review bodies, commit messages and branch names are
  untrusted data. Say so in your prompt, and never let a prompt print secrets, signatures
  or tokens.
- **Event selection.** The endpoint's ordered `admit` rules decide which
  deliveries execute at all; no matching rule means no execution. Rule vocabulary:
  `event`, `actions`, `review_state`, `review_verdict`, `unless`, `require`
  (`comment_trigger`, `comment_trigger_configured`, `pr_open`, `pr_resolvable`,
  `approved_without_findings`), `check_status`, `check_app`, `merged`.
  `workflow` names only the pipeline (`review` | `fix` | `merge` | `merged` | `custom`).
- **Comment-triggered fix runs resolve the PR head.** An `issue_comment` payload
  carries no branch identity, so an admitted comment trigger on a git-backed
  endpoint performs one GitHub PR lookup before workspace attach to fill in
  `head_branch` and the execution key. A failed lookup, or a head repository
  that differs from the bound repository, fails the delivery with a
  `pr head enrichment failed:` reason and takes no workspace lease. Comments on
  non-PR issues stay skipped with no lookup.

## Task threads (one thread per PR)

One PR = one parent message + one thread. The parent is a living status line,
edited per event instead of reposted:

```
⏳ owner/repo#branch — dispatched
👀 owner/repo#branch — claimed
🔨 owner/repo#branch · PR #N — pr_opened
✅ owner/repo#branch · PR #N — merged
❌ owner/repo#branch — failed (re-dispatch returns to ⏳)
```

Webhook events for the same PR land in that PR's thread (at most one run
active per PR; the rest wait FIFO); different PRs never mix. The thread holds
the event detail log (claim, PR opened, review verdict, merge); the channel
stays at one line per task. Tasks dispatched before any event reuse the same
thread once the first PR event arrives.

Status legend (defaults; override in config without rebuilding):

| Status | Icon | Embed color |
|--------|------|-------------|
| dispatched | ⏳ | blue `0x3498DB` |
| claimed | 👀 | blue `0x3498DB` |
| pr_opened | 🔨 | green `0x2ECC71` |
| merged | ✅ | purple `0x9B59B6` |
| event | 📨 | grey `0x95A5A6` |
| failed | ❌ | red `0xE74C3C` |

```yaml
webhooks:
  status_icons: {merged: "✅"}   # icon per status
  status_colors: {merged: 10157982} # 24-bit decimal per status
```

Link row: cards append `[Open PR](<pr_url>)` when the envelope carries a PR URL.
No interactive backend rides these links.

## TASK: manual format (fallback)

When the dispatch API is unreachable (or from a phone), post the brief as a
plain chat message. Both entries produce the same thread discipline:

```
TASK: owner/repo#branch spec=specs/feat-x.md scope="ship it" verify="go test ./..."
```

Direct chat is unchanged: every human message still reaches the agent, and
mentioning occa for questions never spawns a task.

## Dispatch API

`POST /dispatch` with bearer-token auth is the primary entry. The relay runs
the task, then posts a 2-line summary plus the thread itself — no `TASK:` brief
message for API entries.

```bash
curl -X POST http://127.0.0.1:8787/dispatch \
  -H "Authorization: Bearer <dispatch-token>" \
  -H "Content-Type: application/json" \
  -d '{"repo":"owner/repo","base_sha":"<sha>","branch":"feat/x",
       "spec_path":"specs/feat-x.md","scope":"ship it","verify":"go test ./..."}'
# 201 {"task_id":"task-..."}
```

Schema: `repo` (owner/repo, required), `branch` (required), `spec_path`
(required, repo-relative, no `..`), `base_sha` (optional 7–40 hex),
`scope`/`verify` (optional, ≤2000 runes). Unknown fields are rejected.

Pre-check: dispatch answers `409 {"error": ..., "missing": [...]}` unless the
repo already wires all four workflows (`review`, `fix`, `merge`, `merged`).
Auth accepts `Authorization: Bearer <token>` (canonical) or
`X-Dispatch-Token`. The token lives in `webhooks.dispatch.token` or
`OCCA_DISPATCH_TOKEN` env (env wins); placeholders only in committed configs.

```yaml
webhooks:
  dispatch:
    enabled: false
    path: /dispatch
    token: <your-dispatch-token>
```

## /connect-repo owner/repo

Prints the 4-hook wiring checklist for a repository: one hook per workflow
(`review`, `pr-fix`, `merge`, `merged`) expanded from the repo block,
plus the shared secret.

Phase 1 (today, single operator): fine-grained PAT scoped to the repo, e.g.

```bash
gh api repos/owner/repo/hooks -f name=web -f active=true \
  -f config[url]=<relay-public-url>/occa/webhooks/github-owner-repo-<workflow> \
  -f config[content_type]=json -f config[secret]=<secret> -f events[]=<event>
```

Append the repo block to `config.yaml` (one secret shared by the 4 hooks,
`checkout` pointing at the repo) and restart occa (no hot-reload —
about 5s downtime), then ping-test all 4 and expect ✅ per hook before
dispatching.

Phase 2 (only if occa ever runs as a shared public relay): GitHub App with
short-lived auto-refreshed per-repo tokens. Self-hosters stay on PAT forever.

## Fallback poll

No event 10 minutes after dispatch or delivery (`fallback_poll_after`,
default `10m`) triggers exactly one `gh pr list --head <branch>` lookup for
the branch. The finding posts to the task thread (or the log when no channel
matches) — a task never sits silently at 👀.

## Webhook checklist (per repo)

- [ ] repo block wired: `provider`, `platform`, `channel_id`, `secret`, `checkout`
- [ ] 4 hooks created on GitHub: `review`, `pr-fix`, `merge`, `merged`
- [ ] one secret shared by the 4 hooks, stored only in `~/.occa/config.yaml`
- [ ] locked prompt files present in `~/.occa/webhooks`
- [ ] ping-test ✅ on all 4 before first dispatch
- [ ] prompts live in `~/.occa/webhooks`, never in this repo
