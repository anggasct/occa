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

Admission is config, not code: every endpoint declares ordered `admit` rules, and the binary refuses to start when any endpoint misses `admit`, when `policy`/`runtime` keys are missing, or when a comment-trigger endpoint has no `limits`. `workflow` selects a pipeline only (`review` | `fix` | `merge` | `merged` | `custom`) and implies no admission policy. Copy `config.example.yaml` as the starting template and validate with `occa webhooks check-config --config <path>`.

```yaml
webhooks:
  bind: 127.0.0.1:8787
  policy:
    trust_review_logins: []
    verdicts:
      approved: ['approved']
      request_changes: ['request changes', 'request_changes']
  runtime:
    max_body_size: 10MB
    max_concurrent_events: 16
    max_queued_per_key: 8
    processing_timeout: 30m
    claim_grace: 32m
    retry_after: 30s
    workspace_retry_backoff: [30s, 60s, 120s]
    retention: 720h
    retention_keep: 500
    prune_interval: 10m
    dispatcher_idle_ttl: 1h
    http_read_header_timeout: 10s
    http_read_timeout: 30s
    http_write_timeout: 30s
    http_idle_timeout: 2m
    review_dedupe_window: 60m
    isolated_workspace_ttl: 24h
    usage_retention: 2160h
    usage_max_rows: 100000
    recovery_event_retention: 720h
  endpoints:
    - name: github-review
      path: /github-review
      secret: <webhook-secret>
      workflow: review
      platform: discord
      channel_id: "<your-channel-id>"
      prompt_file: webhooks/example-review.md
      comment_trigger: ["<your-trigger-phrase>"]
      admit:
        - event: pull_request
          actions: [opened, reopened, ready_for_review]
        - event: issue_comment
          actions: [created]
          require: [comment_trigger, pr_open]
      limits:
        max_runs_per_pr: 3
        window: 60m
```

`comment_trigger` is a per-endpoint list of case-insensitive substrings matched against comment bodies. An endpoint with no matching rule executes nothing; the loop guards stay (per-PR trigger cap via `limits`, rate limits, prompt-level "never post a trigger phrase").

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
