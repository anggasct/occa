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

## Stack

Go · SQLite · Telegram Bot API · Discord API · OpenCode (SSE)

## Install

```sh
curl -fsSL https://github.com/anggasct/occa/releases/latest/download/occa_$(uname -s)_$(uname -m) | bash
```

The installer detects your OS/arch, installs the matching release binary, and
runs OpenCode's official installer if `opencode` is not on `PATH`. To pin a
specific release:

```sh
OCCA_VERSION=v1.0.0 curl -fsSL https://github.com/anggasct/occa/releases/latest/download/occa_$(uname -s)_$(uname -m) | bash
```

Or build from source:

```sh
go build -o occa ./cmd/occa
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

## Scheduling

Describe a recurring task in plain language:

> every morning at 9am, summarize my GitHub issues

OCCA registers a cron job and pushes each run's result back to the chat.
`/schedules` lists active jobs; `/schedules delete <id>` removes one.

## How it works

```
chat platform ──► channel adapter ──► router ──► opencode
                      ▲                  │
                      └── streaming ─────┘
```

- **Adapters** own the platform SDKs (Telegram, Discord) and normalize every
  incoming message into one generic shape.
- **Router** authorizes everything at the ingress, classifies input as a
  callback, an OCCA command, or an ordinary message, and enforces the command
  namespace and listen-mode policy.
- **Relay** speaks to OpenCode — sessions, messages, commands, and its SSE
  event stream — over `opencode serve`'s HTTP interface.
- **Process manager** supervises one OpenCode instance per working directory:
  lazy spawn, health checks, idle reaping, and graceful process-group
  shutdown.
- **Store** persists sessions, channel and user configuration, and schedules
  in SQLite.
- **Render** converts markdown to the destination platform's format and
  guarantees every outbound string is escaped exactly once and split without
  cutting characters.

## Documentation

- [OpenCode docs](https://opencode.ai/docs) — the agent backend OCCA talks to
- [OpenCode installer](https://opencode.ai/install) — installs the `opencode`
  binary OCCA manages

## Contributing

Bug reports, feature requests, and pull requests are welcome. Please open an
issue before starting large changes so the approach can be discussed first.
