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
- **Access control** — deny-by-default at the ingress, per-user roles per
  channel, admin-only commands.
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
export OCCA_ADMIN_ID="<your user id>"
occa
```

The first run creates `~/.occa/config.yaml` with defaults; every option can be
overridden with an `OCCA_*` environment variable (env var > config file >
built-in default). Bot tokens are env-only and never written to the config
file.

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
