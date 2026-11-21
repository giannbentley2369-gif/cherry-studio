# Agent Note: Self-hosted multi-device sync

Status: proposed

English | [中文](2026-11-21-self-hosted-sync.zh.md)

## Problem

A backup can restore a profile, but it cannot reconcile independent edits on two running desktops. Copying live SQLite files would also copy device-specific state and bypass the services that maintain message trees, search indexes and observable preferences.

## Proposal

Users opt in under Data Settings with a server URL and bearer token. The independent Node server owns a SQLite revision log and SHA-256-addressed blobs, never a desktop database. Choose HTTPS except on loopback. The operator and all clients holding the token are trusted with the selected data; provider API keys require a separate opt-in. Desktop credentials use OS-backed Electron safeStorage when available; with no secure backend the token can be entered for the current process but is never persisted as plaintext.

Offer nine independent gates: chats and messages; providers and API keys; assistants and prompts; MCP servers; portable preferences; notes and knowledge; agents and sessions; managed files and attachments; other data (groups, pins, tags, mini apps, paintings and translations). Map gates to explicit table and field allowlists. Skip external paths, boot settings, caches, logs, local workspace bindings, executable grants and vector indexes. Treat managed note metadata separately from Markdown contents. Imported session history must not resume work, and importing an MCP definition must not launch it.

Store configuration, cursors, retry state and canonical per-entity hashes in a separate profile database. A scan detects creates, edits and deletes without changing existing write paths. Server winners are ordered by source modification time then device ID, including tombstones and revivals. Authenticated SSE hints prompt a paginated pull; startup, focus, manual and periodic sync recover missed events. Disabling a category must not erase it or implicitly enable a dependency.

Preserve identities in one synchronous desktop write transaction. Append narrow sync helpers to owners only where public methods cannot preserve identity, timestamps or notifications. Defer unresolved dependencies and active streams, apply parents before children and reverse the order for deletes. Let local triggers maintain FTS. Refresh the owning caches and notify open views only after commit. Do not modify desktop schemas, migrations, generated data/route files, legacy stores or backup implementations.

## Alternatives considered

- File or database replication conflates user data with physical identities, external paths and live connections.
- A central raw SQL writer would duplicate each domain's invariants and leave cached state stale.
- Tracking every local mutation would improve scan performance but spread hooks across upstream services.

## Acceptance criteria

- AC1 — An Ubuntu operator can run the isolated server with Docker Compose or native Node/systemd and connect two desktops. Only health is public; tokens, event delivery, pagination and bounded hash-verified blob transfers are covered. (verification: server tests and deployment guide)
- AC2 — Two migrated desktop databases converge after offline creates, edits, deletes and revivals. Timestamp ties resolve deterministically; replay does not create changes; toggling a category does not discard its history. (verification: main integration tests)
- AC3 — Pulled data preserves message roots, foreign keys and local full-text indexes. Active streams are not overwritten, protected paths are never traversed, and importing a record cannot grant local execution authority. (verification: main integration tests)
- AC4 — Settings expose nine category toggles, token entry, test connection, interval, manual sync, confirmed local reset and live status. A committed remote change reaches open views without restarting; rollback does not publish it. (verification: renderer, IPC and Electron)
- AC5 — The isolated engine, standalone server and append-only owner helpers are reviewable without desktop schema, migration, generated or backup changes. The PR records all existing-file changes and actual test, typecheck and lint results. (verification: changed-file inspection and project checks)

## Risks

Clock skew affects last-write-wins; it is not a text merge. Independent category choices can leave references pending. An unavailable key store requires re-entry after restarting. Protect the server directory and token: transport encryption is not end-to-end encryption.
