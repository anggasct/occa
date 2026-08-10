# OCCA

A chat adapter for [OpenCode](https://opencode.ai). OCCA bridges your chat
platform to OpenCode: it answers only its own `/occa:*` command namespace and
forwards everything else to OpenCode unchanged, so you can drive an OpenCode
agent from Telegram or Discord.

OCCA is a single static binary with zero runtime dependencies. It holds no
agent loop, no model configuration, and no provider state — OpenCode owns all
of that.

## Features

- **Telegram & Discord adapters** — receive messages, stream OpenCode
  responses as progressive edits, render markdown per platform (HTML for
  Telegram, native for Discord), inline permission prompts with buttons, file
  and voice attachments.
- **Transparent passthrough** — anything that is not an `/occa:*` command goes
  to OpenCode verbatim, so new OpenCode commands work without changes.
- **Streaming responses** — OpenCode's event stream is buffered into
  incremental chat edits, with multi-message overflow and continuation markers
  for long outputs.
- **Sessions & commands** — per-channel session persistence across restarts.
- **Access control** — deny-by-default at the ingress boundary, per-user roles
  per channel, admin-only commands.
- **Scheduled tasks** — describe a recurring task in chat and OCCA registers a
  cron job whose results are pushed back to the channel.
- **Webhook ingestion** — HTTP endpoints with per-endpoint secrets that feed
  prompts into a channel for OpenCode analysis.
- **OpenCode process management** — lazy spawn of one OpenCode instance per
  working directory, idle reaping, capacity caps, graceful shutdown, optional
  auto-install when the `opencode` binary is missing.
- **SQLite store** — sessions, channels, overrides, and schedules in a single
  pure-Go database file with versioned migrations.

## Requirements

- A chat bot token for at least one platform (Telegram, Discord, or both).
- [OpenCode](https://opencode.ai) installed (its server mode, `opencode
  serve`, is used under the hood).

## Install

```sh
curl -fsSL https://github.com/anggasct/occa/releases/latest/download/occa_$(uname -s)_$(uname -m) | bash
```

The installer detects your OS/arch and installs the matching release binary.
If `opencode` is not already on `PATH`, it runs OpenCode's official installer.

To install a specific release instead of the latest:

```sh
OCCA_VERSION=v1.0.0 curl -fsSL https://github.com/anggasct/occa/releases/latest/download/occa_$(uname -s)_$(uname -m) | bash
```

Or build from source:

```sh
go build -o occa ./cmd/occa
```

## Quick start

```sh
export OCCA_TELEGRAM_TOKEN="<your bot token>"
export OCCA_ADMIN_ID="<your user id>"
occa
```

The first run creates `~/.occa/config.yaml` with defaults; every option can be
overridden with an `OCCA_*` environment variable (env var > config file >
built-in default). Bot tokens are env-only and never written to the config
file.

## Usage

Send OCCA a message in your chat and it is forwarded to OpenCode:

> refactor this function to use generics

When OpenCode asks for permission to use a tool, an inline prompt with buttons
appears in the chat:

> 🔐 Permission requested: bash — Allow once / Always / Reject

### Commands

| Command | Purpose |
|---------|---------|
| `/help` | List commands |
| `/status` | Agent health and session info |
| `/session [list\|new\|switch <id>\|delete <id>]` | Manage sessions |
| `/dir [path]` | View or set this channel's working directory |
| `/channel [mention\|all\|thread]` | View or set listen mode |
| `/model [channel] [provider/model-id[@variant]]` | View or set the model |
| `/admin <user_id>` | Grant or revoke admin |
| `/allow <user_id>` / `/deny <user_id>` | Grant or revoke access |
| `/reset` | Clear the current session and start fresh |
| `/schedules [delete <id>]` | View or delete scheduled tasks |

Legacy `/occa:*` and `/occa_*` forms still work as aliases.

Everything else — including other `/`-prefixed commands — is passed through to
OpenCode untouched.

### Scheduling

Describe a recurring task in plain language:

> every morning at 9am, summarize my GitHub issues

OCCA registers a cron job and pushes each run's result back to the chat.
`/occa:schedules` lists active jobs; `/occa:schedules delete <id>` removes one.

## How it works

```
chat platform ──► channel adapter ──► router ──► opencode
                      ▲                  │
                      └── streaming ─────┘
```

- **Adapters** own the platform SDKs (Telegram, Discord) and normalize every
  incoming message into one generic shape.
- **Router** authorizes everything at the ingress, classifies input as a
  callback, an `/occa:*` command, or an ordinary message, and enforces the
  command namespace and listen-mode policy.
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

## License

[MIT](LICENSE)
