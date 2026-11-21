import { randomUUID } from 'node:crypto'

import type Database from 'better-sqlite3'
import { safeStorage } from 'electron'

import type { SyncCategory, SyncConfig, SyncConfigUpdate } from '@shared/ipc/schemas/selfHostedSync'

import { assertSyncToken, normalizeServerUrl } from './transport'

interface KeyStore {
  available(): Promise<boolean>
  encrypt(token: string): Promise<Buffer>
  decrypt(sealed: Buffer): Promise<string>
}

const osKeyStore: KeyStore = {
  async available() {
    if (process.platform === 'linux') {
      return (
        safeStorage.isEncryptionAvailable() &&
        ['gnome_libsecret', 'kwallet', 'kwallet5', 'kwallet6'].includes(safeStorage.getSelectedStorageBackend())
      )
    }
    return safeStorage.isAsyncEncryptionAvailable()
  },
  encrypt: (value) => safeStorage.encryptStringAsync(value),
  decrypt: async (value) => (await safeStorage.decryptStringAsync(value)).result
}

interface ConfigRow {
  enabled: number
  serverUrl: string
  sealed: Buffer | null
  deviceId: string
  intervalSeconds: number
  categories: string
}

interface SyncStats {
  serverRevision: number
  lastSyncedAt: number | null
  lastError: string | null
  nextRetryAt: number | null
  attempts: number
}

export class SettingsStore {
  private sessionToken?: string

  constructor(
    private readonly db: Database.Database,
    private readonly secrets: KeyStore = osKeyStore
  ) {
    db.exec(`
      CREATE TABLE IF NOT EXISTS sync_config (
        singleton INTEGER PRIMARY KEY CHECK (singleton = 1), enabled INTEGER NOT NULL DEFAULT 0,
        serverUrl TEXT NOT NULL DEFAULT '', sealed BLOB, deviceId TEXT NOT NULL,
        intervalSeconds INTEGER NOT NULL DEFAULT 60,
        categories TEXT NOT NULL,
        serverRevision INTEGER NOT NULL DEFAULT 0, lastSyncedAt INTEGER,
        lastError TEXT, nextRetryAt INTEGER, attempts INTEGER NOT NULL DEFAULT 0
      )
    `)
    db.prepare('INSERT OR IGNORE INTO sync_config (singleton,deviceId,categories) VALUES (1,?,?)').run(
      randomUUID(),
      JSON.stringify(['chats', 'assistants'] satisfies SyncCategory[])
    )
  }

  private row(): ConfigRow {
    return this.db
      .prepare<[], ConfigRow>(
        'SELECT enabled,serverUrl,sealed,deviceId,intervalSeconds,categories FROM sync_config WHERE singleton=1'
      )
      .get()!
  }

  config(): SyncConfig {
    const { enabled, serverUrl, sealed, deviceId, intervalSeconds, categories } = this.row()
    return {
      enabled: Boolean(enabled),
      serverUrl,
      hasToken: Boolean(this.sessionToken || sealed),
      deviceId,
      intervalSeconds,
      categories: JSON.parse(categories)
    }
  }

  async token(): Promise<string> {
    if (this.sessionToken) return this.sessionToken
    const { sealed } = this.row()
    if (!sealed) return ''
    if (!(await this.secrets.available())) throw new Error('The system key store is unavailable.')
    return this.secrets.decrypt(sealed)
  }

  async update(patch: SyncConfigUpdate, onServerChange?: () => void): Promise<SyncConfig> {
    const old = this.row()
    const enabled = patch.enabled ?? Boolean(old.enabled)
    const serverUrl =
      patch.serverUrl === undefined ? old.serverUrl : patch.serverUrl ? normalizeServerUrl(patch.serverUrl) : ''
    const sameServer = serverUrl === old.serverUrl
    let sealed = sameServer ? old.sealed : null
    let sessionToken = sameServer ? this.sessionToken : undefined
    if (patch.token !== undefined) {
      sealed = null
      sessionToken = undefined
      if (patch.token) {
        assertSyncToken(patch.token)
        if (await this.secrets.available()) sealed = await this.secrets.encrypt(patch.token)
        else sessionToken = patch.token
      }
    }
    if (enabled && (!serverUrl || !(sealed || sessionToken)))
      throw new Error('A server URL and token are required to enable sync.')
    this.db.transaction(() => {
      if (!sameServer) {
        onServerChange?.()
        this.clearStats()
      }
      this.db
        .prepare(
          'UPDATE sync_config SET enabled=?,serverUrl=?,sealed=?,intervalSeconds=?,categories=? WHERE singleton=1'
        )
        .run(
          Number(enabled),
          serverUrl,
          sealed,
          patch.intervalSeconds ?? old.intervalSeconds,
          patch.categories === undefined ? old.categories : JSON.stringify([...new Set(patch.categories)])
        )
    })()
    this.sessionToken = sessionToken
    return this.config()
  }

  stats(): SyncStats {
    return this.db
      .prepare<[], SyncStats>(
        'SELECT serverRevision,lastSyncedAt,lastError,nextRetryAt,attempts FROM sync_config WHERE singleton=1'
      )
      .get()!
  }

  failed(message: string, retryAt: number): void {
    this.db
      .prepare('UPDATE sync_config SET lastError=?,nextRetryAt=?,attempts=attempts+1 WHERE singleton=1')
      .run(message, retryAt)
  }

  succeeded(revision: number): void {
    this.db
      .prepare(
        'UPDATE sync_config SET serverRevision=?,lastSyncedAt=?,lastError=NULL,nextRetryAt=NULL,attempts=0 WHERE singleton=1'
      )
      .run(revision, Date.now())
  }

  clearStats(): void {
    this.db
      .prepare(
        'UPDATE sync_config SET serverRevision=0,lastSyncedAt=NULL,lastError=NULL,nextRetryAt=NULL,attempts=0 WHERE singleton=1'
      )
      .run()
  }
}
