# Webhook prompts

An endpoint can render a markdown prompt file per delivery. The file path comes from
`prompt_file` and is resolved relative to the directory that holds `config.yaml`
(an absolute path also works):

```yaml
webhooks:
  endpoints:
    - name: github-acme-review
      path: /webhooks/github-acme-review
      auth: github_hmac_sha256
      secret: <your-webhook-secret>
      workflow: review
      platform: discord
      channel_id: '<channel-id>'
      prompt_file: webhooks/example-review.md
      thread: true
```

## Things to know

- **Startup load.** Prompt files are read when the service starts. Edit a prompt, then
  restart the service for it to take effect.
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
