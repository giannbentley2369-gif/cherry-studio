# Self-hosted sync (desktop)

This optional service compares portable v2 records with per-device baselines in a separate `self-hosted-sync.sqlite` under the active profile. It does not copy the application database or observe every write. A desktop that has not enabled sync makes no background sync requests.

## Trust and consent

Configure only a server you control. The server operator and other clients with the bearer token can read and change data in the selected categories. The server stores records and managed blobs without end-to-end encryption. Selecting **AI Providers & API Keys** includes manual provider credentials; it is off by default. A token entered for one server is not reused for a different URL. With an OS key store, the desktop keeps a sealed token. Without one, including Linux's insecure `basic_text` backend, re-enter it after restarting.

Use HTTPS for a network server. HTTP is for loopback (including a local SSH tunnel). Events are hints; the durable cursor and periodic pull recover when the event connection is interrupted. Last-write-wins compares source modification time and then device ID, not server arrival order. Keep the devices' clocks synchronized. This is not a backup or a text merge.

## Data ownership

Only explicitly mapped records are eligible. A missing parent in an unselected category is not permission to import that category. Machine-local state does not become eligible merely because a future migration adds a column.

| Setting | Data boundary |
| --- | --- |
| Chats & Messages | Topic/message identities, tree relationships, portable content and topic-scoped groups, pins and tag links (shared tag definitions belong to Other Data); FTS identities and search text are regenerated locally. |
| AI Providers & API Keys | Portable provider/model configuration. OS identities and machine-bound sign-ins stay local. |
| Assistants & Prompts | Assistant definitions, prompts, and their eligible bindings. |
| MCP Servers | Portable definitions, not installation paths, execution grants or local trust decisions. |
| App Settings / Preferences | An explicit list of portable preference keys; never BootConfig, network credentials from other features, caches or window geometry. |
| Notes & Knowledge | Default-root note metadata and portable knowledge sources. A note row records starred/expanded state, not a Markdown document. No traversal of a user's external notes directory, and no copying vector indexes. |
| Agents & Sessions | Portable profiles and conversation history, not live execution, jobs, channel credentials, native thread state or external workspaces. |
| Files & Attachments | Verified content of internal file entries and eligible owner-specific links. Neither an external entry nor a path embedded in a record authorizes a filesystem read. |
| Other Data | Portable groups, pins, tags, web mini apps, paintings and translations. Installed mini-app code and local grants stay on the device. |

New adapters belong here. Import through a narrow method on the table owner inside `DbService.withWriteTx()`; never issue a foreign write just because the transaction handle is available. Check dependencies and active work before applying, keep file/network waits outside SQLite transactions, and publish cache changes only after commit. A disabled category or a temporarily unavailable file must not be mistaken for a local deletion.

Resetting sync state forgets local replication bookkeeping, not desktop data, server data, credentials or the device ID. For deployment and a two-device walkthrough, see the [server guide](../../../../sync-server/README.md).
