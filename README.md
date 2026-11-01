# OCCA

> A chat adapter that lets you drive an [OpenCode](https://opencode.ai) agent from your messaging platform.

OCCA sits between a chat platform (Telegram, Discord) and OpenCode's server
mode. It answers its own short-form commands, forwards everything else to
OpenCode unchanged, and streams the agent's responses back into the chat as
progressive edits. It is a single static binary with zero runtime
dependencies — OpenCode owns the agent loop, model configuration, and
provider state.

## What it can do

- **Telegram & Discord adapters** — stream OpenCode responses as progressive
  chat edits, render markdown per platform, inline permission prompts with
  buttons, file and voice attachments, and lifecycle reactions on Discord.
- **Transparent passthrough** — anything that is not one of OCCA's commands
  goes to OpenCode verbatim, so new agent commands work without changes.
- **Sessions & commands** — per-conversation sessions that persist across
  restarts, session titles, a numbered session switcher, context usage in
  `/status`, and session control via `/stop`, `/steer`, and `/reset`.
- **Conversation queue** — while the agent is busy, up to five more messages
  queue up and run automatically in order when the current response finishes.
- **Access control** — deny-by-default at the ingress, per-platform sender
  allowlists: list your Telegram and Discord sender IDs under
  `telegram.allowed_sender_ids` / `discord.allowed_sender_ids` in
  `~/.occa/config.yaml` (a listed sender holds full rights in every channel
  and thread on that platform).
- **Scheduled tasks** — describe a recurring task in plain language and OCCA
  runs it on a cron schedule, pushing each result back to the chat.
- **Webhook ingestion** — HTTP endpoints with per-endpoint secrets that feed
  prompts into a channel for agent analysis.
- **Repo skill packs** — a repo carries its own working rules as
  `.occa/skills/<name>/SKILL.md` (plus cross-repo rules in `~/.occa/skills/`).
  Each run resolves them by trigger (`always`, `keyword`, or `paths` against
  the dispatched scope) and injects the selected set once, at the top of the
  session's first prompt. `/skills` lists what a workdir loads;
  `occa skills lint --workdir <dir>` checks the files in CI and
  `occa skills explain` shows why a skill would fire. The optional `skills:`
  block in the config tunes or disables it.
- **PR actions from chat** — each PR card carries Merge, Review and
  Re-dispatch; a blocked PR says in plain words what happened and offers
  Retry, Merge and Cancel. `/inbox` lists every PR that waits on you, with the
  same buttons (they work on Telegram too), and the optional `webhooks.digest`
  block posts a once-a-day summary (`/digest` shows it on demand).
- **Deterministic lane steps** — occa merges in code (approved on the head,
  checks green, GitHub mergeable; one `gh pr merge --match-head-commit`, no
  agent turn; there is no merge prompt), writes the post-merge summary itself
  with the built-in merged prompt, hands the agent earlier reviews and failing
  check logs in the prompt, and merges the base branch before a conflict fix
  so the agent only resolves the files that actually conflict.
  `repos[].merge` picks the method, branch deletion, and the paths an older
  approval may carry over (`stale_approval_paths`); with `auto_fix.conflict` a
  conflicting approved PR starts the conflict fix.
- **Provider limits fail fast** — when the agent reports it will retry the
  model only after the run's deadline (a quota or rate limit), the run stops at
  once and the PR blocks with the provider's own message and a Retry button.
- **OpenCode process management** — lazy-spawns one agent instance per working
  directory, idles them out, caps capacity, and shuts down gracefully
  (optional auto-install when the `opencode` binary is missing).
- **SQLite store** — sessions, channels, overrides, and schedules in a single
  pure-Go database file with versioned migrations.

## Install

Build from source:

```sh
go build -o occa ./cmd/occa
```

To make `occa` available on your PATH:

```sh
go install ./cmd/occa
```

Quick start:

```sh
export OCCA_TELEGRAM_TOKEN="<your bot token>"
occa
```

Add your sender IDs to `~/.occa/config.yaml` so OCCA answers you:

```yaml
telegram:
  allowed_sender_ids:
    - '1065778107'
discord:
  allowed_sender_ids:
    - '1519692433808556133'
```

The first run creates `~/.occa/config.yaml` with defaults; every option can be
overridden with an `OCCA_*` environment variable (env var > config file >
built-in default). Bot tokens are env-only and never written to the config
file.

### Webhook configuration

Dispatch repos declare one block per repository under `webhooks.repos`; the binary expands it into four locked endpoints (`review`, `fix`, `merge`, `merged`) with locked admission rules, paths, thread discipline and workspace policy — admission is locked in code, not configured per endpoint. The binary refuses to start when `policy`/`runtime` keys are missing, when a repo block is incomplete, or when a hand-written endpoint reuses a locked workflow (custom workflows keep the manual shape under `webhooks.endpoints`). Copy `config.example.yaml` as the starting template — every key carries an inline comment explaining what it tunes — and validate with `occa webhooks check-config --config <path>`.

`comment_trigger` defaults live under `webhooks.defaults` (per workflow) with optional per-repo overrides. Overrides merge with the inherited phrase list (union) instead of replacing it; matching is case-insensitive against comment bodies. A repo with no matching rule executes nothing; the loop guards stay (per-PR trigger cap via `limits`, rate limits, prompt-level "never post a trigger phrase"). Per-author automation (`review-only` default) decides whether fix/merge runs execute at all.

`comment_authors` (also under `webhooks.defaults` or per repo) restricts human comment-triggered workflows to an explicit GitHub login allowlist — `['anggasct']`. Matching is case-insensitive, entries are trimmed and deduped, and blank entries are refused at startup. An empty list keeps the historical behaviour for event-driven runs and full automation, but it does not unlock `fix` under `automation.default: review-only`: that exception requires a non-empty allowlist and admits only a listed author. The allowlist only gates runs started by a matched human comment trigger; event-driven reviews and the internal re-review queued after a fix round bypass it. Keep the relay's own GitHub identity out of the list: the relay comments under its own token, so allowlisting that login would let its status comments re-trigger work. `merge` stays gated by the automation policy.

Everything under `runtime` is required tuning: the network, durability, capacity and pagination values the binary sizes itself with. The same fail-closed contract applies — no built-in defaults, no fallbacks — so a missing or unknown key, or a cross-field violation (e.g. `min_interval > max_duration`), refuses startup naming the block and key. See the inline comments in `config.example.yaml` for what each key does.

Not every number is tuning. Pacing that shapes a single interaction (typing tick, working-edit backoff ladder, progress ticker), poll tickers that only sample a configured deadline, and hard platform API limits (Discord action-row and button caps, Telegram message limits) stay in code by design. The in-code residue is a fixed allowlist — 22 duration literals for shutdown drain waits, typing/edit cadence, Telegram init backoff, the agent-browser retry delay, the progress ticker, readiness/reap poll pacing, permission-prompt coalescing, the PR run retry backoff ladders, the dispatch retry base delay, the PR author lookup timeout, the post-merge settle timeout, the per-repo run-limit window and the PR-state lookup timeout — and CI fails any change whose duration literals over `internal/` and `cmd/` do not match that allowlist exactly, so tuning cannot quietly reappear in code.

## Database backup and restore

Use the operator commands to protect the SQLite store during upgrades:

```sh
occa db backup --output ~/.occa/backups/occa.db
occa db restore --input ~/.occa/backups/occa.db
```

The database path is resolved from `--config` (or `-c`), the default
`~/.occa/config.yaml`, and the `OCCA_DB_PATH` environment override. Backups use
a SQLite-consistent snapshot and can run while OCCA is live. Restore must be
run with OCCA stopped; it refuses an active database, validates integrity and
schema compatibility, creates a timestamped `<database>.pre-restore.*.db`
safety copy, and atomically replaces the database. Restore failures leave the
existing database unchanged.

`--force` allows an existing backup output to be overwritten and permits an
intentional restore from an older schema. It does not bypass integrity checks
or the stopped-service requirement.

Before a binary or schema upgrade, `scripts/backup-db.sh [output-dir]` resolves
the configured database and writes a timestamped backup. Set `OCCA_CONFIG` for
a non-default config file or `OCCA_BIN` for the OCCA binary path.

## Contributing

Bug reports, feature requests, and pull requests are welcome. Please open an
issue before starting large changes so the approach can be discussed first.
