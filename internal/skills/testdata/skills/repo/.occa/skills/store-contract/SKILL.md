---
name: store-contract
description: How the SQLite store is layered in this repo.
trigger: paths
paths: ["internal/store/**", "./cmd/db/*.go"]
priority: 120
---
Every query goes through `internal/store`; no `database/sql` outside it.
