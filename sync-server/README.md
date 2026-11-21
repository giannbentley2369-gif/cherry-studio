# Cherry Studio self-hosted sync server

Run one Node process and give each trusted Cherry Studio desktop its URL and bearer token. No cloud account or external database is needed. The server sees the selected data in plaintext, including API keys if that category is enabled. The token grants full access: protect it, the server data directory, and backups. Use different servers/tokens for people who should not share data.

The following recipes start in a checkout containing this feature on **Ubuntu 24.04**. Both listen on loopback by default.

## Docker Compose

```sh
cd sync-server
sudo apt-get update
sudo apt-get install -y docker.io docker-compose-v2
sudo systemctl enable --now docker
cp -n .env.example .env
chmod 600 .env
sudo docker compose up --build -d
curl -fsS http://127.0.0.1:8787/api/v1/health
sudo docker compose exec -T sync-server cat /data/token.txt
```

Leave `SYNC_TOKEN` unset to generate a strong token. It is printed once on first boot and saved with mode 0600. If you set it explicitly in the private environment file, use that value instead. The named volume is mounted at `/data` and survives container replacement; do not use `docker compose down -v` unless you intend to erase the server.

## Native Node.js and systemd

Use Node.js 22 LTS (Node 20 also works). The following installs the official 22.x Linux binary under a dedicated prefix on amd64 or arm64 without replacing the system Node. The SHA-256 check uses the release manifest downloaded via HTTPS from nodejs.org.

```sh
cd sync-server
sudo apt-get update
sudo apt-get install -y ca-certificates curl xz-utils build-essential python3
(
  set -eu
  case "$(dpkg --print-architecture)" in
    amd64) arch=x64 ;;
    arm64) arch=arm64 ;;
    *) echo 'Use an official Node.js build for this CPU.' >&2; exit 1 ;;
  esac
  tmp=$(mktemp -d)
  trap 'rm -rf "$tmp"' EXIT
  cd "$tmp"
  dist=https://nodejs.org/dist/latest-v22.x
  curl -fsSLO "$dist/SHASUMS256.txt"
  archive=$(awk -v end="-linux-$arch.tar.xz" 'index($2,end) && substr($2,length($2)-length(end)+1)==end {print $2; exit}' SHASUMS256.txt)
  test -n "$archive"
  curl -fsSLO "$dist/$archive"
  awk -v name="$archive" '$2 == name' SHASUMS256.txt | sha256sum -c -
  sudo install -d /opt/cherry-sync-node
  sudo tar -xJf "$archive" --strip-components=1 -C /opt/cherry-sync-node
)
export PATH="/opt/cherry-sync-node/bin:$PATH"
npm ci
npm run build
npm prune --omit=dev
id cherry-sync >/dev/null 2>&1 || sudo useradd --system --user-group --no-create-home --home-dir /var/lib/cherry-sync --shell /usr/sbin/nologin cherry-sync
sudo install -d /opt/cherry-sync
sudo cp -a package.json dist node_modules /opt/cherry-sync/
sudo install -D -m 0644 deploy/sync-server.service /etc/systemd/system/sync-server.service
sudo systemctl daemon-reload
sudo systemctl enable --now sync-server
curl -fsS http://127.0.0.1:8787/api/v1/health
sudo cat /var/lib/cherry-sync/token.txt
```

To override defaults for systemd, create `/etc/cherry-sync/server.env` as root, mode 0600, with one `NAME=value` per line, then restart the unit. Keep the code root-owned. The service user needs write access only to its state directory.

## Connect two desktops

1. On each device, either use an HTTPS reverse proxy or open an SSH tunnel to the Ubuntu host: `ssh -NT -L 18787:127.0.0.1:8787 user@your-ubuntu-host`. With a tunnel, use `http://127.0.0.1:18787` on that device. Do not expose a plaintext bearer endpoint on the LAN or internet.
2. In **Settings → Data → Self-hosted sync**, enter the URL and token, test the connection, choose categories and enable sync. Use two independent profiles, not two windows sharing a database. On a development installation, `CS_DEV_PROFILE_ROOT` can isolate unpackaged instances.
3. Enable **Chats & Messages** on both. Create and finish a chat on A. Use **Sync Now** and confirm that the topic, messages and search results appear on B. The two status panels should show different device IDs.
4. Disconnect B, rename the chat there, and reconnect. Confirm the new name reaches A. Delete the chat and verify that it disappears on the peer. Enable **Files & Attachments** to repeat with a Cherry-managed attachment. External path-backed files are not transferred.
5. Disable a category on B and edit that category on A. B should not apply it until re-enabled. **Reset local sync state** resets only the device's replication history; it does not delete either dataset.

For Caddy, a minimal HTTPS site block is:

```caddyfile
sync.example.com {
  reverse_proxy 127.0.0.1:8787
}
```

Point DNS at the host and permit HTTPS to the proxy. With Nginx, disable proxy buffering for the event stream. Do not log Authorization headers.

## Configuration and storage

| Variable                 | Default       | Purpose                                                                                                                                    |
| ------------------------ | ------------- | ------------------------------------------------------------------------------------------------------------------------------------------ |
| `SYNC_BIND`              | `127.0.0.1`   | Listen address; the container listens on all interfaces inside its private network.                                                        |
| `SYNC_PORT`              | `8787`        | Listen port.                                                                                                                               |
| `SYNC_TOKEN`             | generated     | Shared 32–512 character bearer token. Set to a strong URL-safe secret to manage rotation yourself.                                         |
| `SYNC_DATA_DIR`          | `./data`      | Contains `sync.sqlite`, `blobs/<sha256>` and, for generated tokens, `token.txt`. Docker uses `/data`; systemd uses `/var/lib/cherry-sync`. |
| `SYNC_MAX_BLOB_MB`       | `100`         | Maximum size of one blob, in MiB.                                                                                                          |
| `SYNC_MAX_JSON_BYTES`    | `8388608`     | Maximum JSON batch body. At most 100 changes per push.                                                                                     |
| `SYNC_MAX_STORAGE_BYTES` | `10737418240` | Storage budget including retained revision history.                                                                                        |
| `SYNC_HEARTBEAT_MS`      | `20000`       | SSE heartbeat interval in milliseconds.                                                                                                    |

Health is public. Status, push, pull, events and every blob operation require bearer authentication. Clients push idempotent batches and pull revision-ordered changes with `since`, optional comma-separated `categories`, and `limit`. The highest source timestamp wins; device ID breaks ties, including deletes and revivals. Events only announce a revision: missed events are recovered by pulling from the saved cursor.

For a consistent server backup, stop the process/container and copy the entire data directory, including SQLite sidecars, blobs and any generated token. Restore that directory with its permissions while the service is stopped. To rotate a token, set a new `SYNC_TOKEN`, restart, and reconfigure each desktop. Do not later unset the environment override without also rotating the old on-disk token.

## Verification

From this directory, run `npm ci && npm run build && npm run typecheck && npm test`. For a disposable running server only, `SYNC_TEST_URL` and `SYNC_TEST_TOKEN` configure `npm run smoke:two-device`, which creates, updates and tombstones a test entity.
