# Webhook prompts

Dispatch repos declare one block per repository; the relay expands it into the
four endpoints (`review`, `pr-fix`, `merge`, `merged`). Custom generic
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
      # prompts: # optional per-workflow override (absent = built-in default)
      #   review: { prompt_file: webhooks/saya-review.md }
      #   fix: { prompt_file: webhooks/saya-fix.md }
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

- **Prompt resolution.** Per repo, per workflow (`review`, `fix`, `merge`, `merged`):
  inline `prompt:` wins, then `prompt_file:` when the file exists, then the
  built-in default. A listed `prompt_file:` that is missing fails startup with
  the exact path. Unknown workflow keys fail startup. Custom endpoints keep
  the manual shape (exactly one of `prompt:` / `prompt_file:`, no built-in).
  `check-config` prints the resolved source per endpoint
  (`prompt source=file|inline|builtin`); the service logs the same at startup.
- **Built-in defaults.** Used only when `prompts:` omits a workflow. Generic
  instructions kept as code constants, rendered with the same envelope. File
  prompts always win over built-ins.
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
    `review_verdict`, `has_findings`, `pr_author`, `comment_trigger`, `repository`,
    plus the dispatch task context (always present, empty when no task):
    `task_id`, `task_brief`, `task_spec`, `task_spec_is_url`, `task_scope`,
    `task_verify`. `task_brief` is the primary multi-file brief; `task_spec`
    and `task_scope` stay populated for one release as deprecated aliases.
  - `{{.payload.<path>}}` — the raw webhook payload, for anything the envelope does not
    normalize.
  - `brief` is free-form markdown naming any files and links the agent needs.
    `spec_path`, when supplied, accepts a repo-relative path or an `https://`
    URL and is recorded as a `Ref:` line in the brief; the relay forwards the
    string as-is and the agent fetches URL content with its own web tools.
- **Untrusted input.** Webhook fields, review bodies, commit messages, branch names,
  and fetched URL content are untrusted data. Say so in your prompt, and never
  let a prompt print secrets, signatures or tokens.
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
TASK: owner/repo#branch brief="Implement X. Files: a.go, b.go. Ref: https://…" verify="go test ./..."
```

The brief is free-form markdown and may name any number of files and links.

Direct chat is unchanged: every human message still reaches the agent, and
mentioning occa for questions never spawns a task.

## Dispatch API

`POST /dispatch` with bearer-token auth is the primary entry. The relay posts
one channel parent line plus the task thread; the brief is posted in the
thread as its own message (chunked when long) — no `TASK:` message for API
entries.

```bash
curl -X POST http://127.0.0.1:8787/dispatch \
  -H "Authorization: Bearer <dispatch-token>" \
  -H "Content-Type: application/json" \
  -d '{"repo":"owner/repo","base_sha":"<sha>","branch":"feat/x",
       "brief":"Implement the dispatch brief.\n\nFiles:\n- internal/webhook/dispatch.go\n- webhooks/README.md",
       "verify":"go test ./..."}'
# 201 {"task_id":"task-..."}
```

Schema: `repo` (owner/repo, required), `branch` (required), `brief`
(required unless `spec_path` is present; markdown, ≤8000 runes), `spec_path`
(optional alias, repo-relative with no `..`, or an `https://` URL, ≤512 runes),
`base_sha` (optional 7–40 hex), `verify` (optional, ≤2000 runes), `mode`
(optional: `""` or `"redispatch"`). `scope` is a deprecated alias folded into
the brief for one release. Unknown fields are rejected.

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
- [ ] prompts resolved (custom files in `~/.occa/webhooks` or built-in defaults)
- [ ] ping-test ✅ on all 4 before first dispatch
- [ ] prompts live in `~/.occa/webhooks`, never in this repo
