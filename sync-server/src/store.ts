import { createHash } from 'node:crypto'
import { chmodSync, createReadStream, linkSync, lstatSync, mkdirSync, readdirSync, statSync, unlinkSync } from 'node:fs'
import { join, resolve } from 'node:path'

import Database from 'better-sqlite3'

import { ApiError, CATEGORIES, type Category, type IncomingChange, type JsonObject, type Winner } from './types.js'

interface StoredRow {
  entity_type: string
  entity_id: string
  category: Category
  device_id: string
  updated_at: number
  payload: string | null
  deleted: number
  deleted_at: number | null
  server_rev: number
}

interface StoredBlob {
  sha256: string
  size: number
}

interface Candidate {
  category: Category
  deviceId: string
  updatedAt: number
  payloadJson: string | null
  deleted: boolean
  deletedAt: number | null
  serverRev: number
}

interface PreparedChange {
  change: IncomingChange
  payloadJson: string | null
}

export interface BlobInfo {
  sha256: string
  size: number
  filePath: string
}

export interface PushResult {
  revision: number
  changes: Winner[]
  acceptedRevisions: number[]
}

export interface PullResult {
  changes: Winner[]
  cursor: number
  hasMore: boolean
}

export interface StoreStats {
  entities: number
  tombstones: number
  blobs: number
  blobBytes: number
  storageBytes: number
  categories: Record<Category, number>
}

const MAX_SQLITE_PAGE_COUNT = 4_294_967_294
const SQLITE_PAGE_SIZE = 4096
const BLOB_METADATA_RESERVE = SQLITE_PAGE_SIZE * 2

function fileSize(path: string): number {
  try {
    return statSync(path).size
  } catch (error) {
    if (typeof error === 'object' && error !== null && 'code' in error && error.code === 'ENOENT') {
      return 0
    }
    throw error
  }
}

async function hashFile(path: string): Promise<string> {
  const hash = createHash('sha256')
  for await (const chunk of createReadStream(path)) {
    hash.update(chunk as Buffer)
  }
  return hash.digest('hex')
}

function compareVersion(
  left: Pick<Candidate, 'updatedAt' | 'deviceId'>,
  right: Pick<Candidate, 'updatedAt' | 'deviceId'>
): number {
  if (left.updatedAt !== right.updatedAt) return left.updatedAt > right.updatedAt ? 1 : -1
  if (left.deviceId === right.deviceId) return 0
  return left.deviceId > right.deviceId ? 1 : -1
}

function candidateFromRow(row: StoredRow): Candidate {
  return {
    category: row.category,
    deviceId: row.device_id,
    updatedAt: row.updated_at,
    payloadJson: row.payload,
    deleted: row.deleted === 1,
    deletedAt: row.deleted_at,
    serverRev: row.server_rev
  }
}

function toWinner(candidate: Candidate, entityType: string, entityId: string): Winner {
  const common = {
    category: candidate.category,
    entityType,
    entityId,
    deviceId: candidate.deviceId,
    updatedAt: candidate.updatedAt,
    serverRev: candidate.serverRev
  }

  if (candidate.deleted) {
    return {
      ...common,
      deleted: true,
      deletedAt: candidate.deletedAt as number
    }
  }

  return {
    ...common,
    payload: JSON.parse(candidate.payloadJson as string) as JsonObject
  }
}

export class SyncStore {
  readonly dataDir: string
  readonly databasePath: string
  readonly blobsDir: string
  readonly tokenPath: string
  private readonly db: Database.Database
  private readonly maxStorageBytes: number
  private readonly maxBlobBytes: number
  private reservedBytes = 0

  constructor(options: { dataDir: string; tokenPath: string; maxStorageBytes: number; maxBlobBytes: number }) {
    this.dataDir = resolve(options.dataDir)
    this.databasePath = join(this.dataDir, 'sync.sqlite')
    this.blobsDir = join(this.dataDir, 'blobs')
    this.tokenPath = options.tokenPath
    this.maxStorageBytes = options.maxStorageBytes
    this.maxBlobBytes = options.maxBlobBytes

    mkdirSync(this.dataDir, { recursive: true, mode: 0o700 })
    chmodSync(this.dataDir, 0o700)
    mkdirSync(this.blobsDir, { recursive: true, mode: 0o700 })
    chmodSync(this.blobsDir, 0o700)

    this.db = new Database(this.databasePath)
    chmodSync(this.databasePath, 0o600)
    this.db.pragma('journal_mode = WAL')
    this.db.pragma('synchronous = FULL')
    this.db.pragma('wal_autocheckpoint = 1000')

    this.db.exec(
      [
        'CREATE TABLE IF NOT EXISTS sync_meta (key TEXT PRIMARY KEY, value INTEGER NOT NULL);',
        'CREATE TABLE IF NOT EXISTS entities (',
        '  entity_type TEXT NOT NULL,',
        '  entity_id TEXT NOT NULL,',
        '  category TEXT NOT NULL,',
        '  device_id TEXT NOT NULL,',
        '  updated_at INTEGER NOT NULL,',
        '  payload TEXT,',
        '  deleted INTEGER NOT NULL CHECK (deleted IN (0, 1)),',
        '  deleted_at INTEGER,',
        '  server_rev INTEGER NOT NULL,',
        '  PRIMARY KEY (entity_type, entity_id),',
        '  CHECK ((deleted = 0 AND payload IS NOT NULL AND deleted_at IS NULL) OR',
        '         (deleted = 1 AND payload IS NULL AND deleted_at IS NOT NULL))',
        ');',
        'CREATE TABLE IF NOT EXISTS change_log (',
        '  server_rev INTEGER PRIMARY KEY,',
        '  entity_type TEXT NOT NULL,',
        '  entity_id TEXT NOT NULL,',
        '  category TEXT NOT NULL,',
        '  device_id TEXT NOT NULL,',
        '  updated_at INTEGER NOT NULL,',
        '  payload TEXT,',
        '  deleted INTEGER NOT NULL CHECK (deleted IN (0, 1)),',
        '  deleted_at INTEGER,',
        '  CHECK ((deleted = 0 AND payload IS NOT NULL AND deleted_at IS NULL) OR',
        '         (deleted = 1 AND payload IS NULL AND deleted_at IS NOT NULL))',
        ');',
        'CREATE INDEX IF NOT EXISTS change_log_category_revision ON change_log (category, server_rev);',
        'CREATE TABLE IF NOT EXISTS blobs (sha256 TEXT PRIMARY KEY, size INTEGER NOT NULL CHECK (size >= 0));'
      ].join('\n')
    )
    this.db.prepare('INSERT OR IGNORE INTO sync_meta (key, value) VALUES (?, 0)').run('revision')

    const pageLimit = Math.max(1, Math.min(MAX_SQLITE_PAGE_COUNT, Math.floor(this.maxStorageBytes / SQLITE_PAGE_SIZE)))
    this.db.pragma('max_page_count = ' + pageLimit)
  }

  async initialize(): Promise<void> {
    const knownRows = this.db.prepare('SELECT sha256, size FROM blobs').all() as StoredBlob[]
    const known = new Map(knownRows.map((row) => [row.sha256, row]))
    const present = new Set<string>()

    for (const name of readdirSync(this.blobsDir)) {
      const path = join(this.blobsDir, name)
      const stat = lstatSync(path)

      if (name.startsWith('.upload-')) {
        if (!stat.isFile()) throw new Error('Unexpected temporary blob entry')
        unlinkSync(path)
        continue
      }

      if (!/^[a-f0-9]{64}$/.test(name) || !stat.isFile()) {
        throw new Error('Unexpected entry in blob directory')
      }

      const registered = known.get(name)
      if (registered) {
        if (stat.size !== registered.size) throw new Error('Registered blob size does not match its file')
        present.add(name)
        continue
      }

      const validOrphan = stat.size <= this.maxBlobBytes && (await hashFile(path)) === name
      if (
        validOrphan &&
        this.storageBytes() + this.reservedBytes + stat.size + BLOB_METADATA_RESERVE <= this.maxStorageBytes
      ) {
        this.db.prepare('INSERT INTO blobs (sha256, size) VALUES (?, ?)').run(name, stat.size)
        present.add(name)
      } else {
        unlinkSync(path)
      }
    }

    for (const row of knownRows) {
      if (!present.has(row.sha256)) {
        this.db.prepare('DELETE FROM blobs WHERE sha256 = ?').run(row.sha256)
      }
    }
  }

  getRevision(): number {
    const row = this.db.prepare('SELECT value FROM sync_meta WHERE key = ?').get('revision') as { value: number }
    return row.value
  }

  push(changes: IncomingChange[], requestBytes: number): PushResult {
    const prepared: PreparedChange[] = changes.map((change) => ({
      change,
      payloadJson: 'payload' in change ? JSON.stringify(change.payload) : null
    }))

    const apply = this.db.transaction(() => {
      let revision = this.getRevision()
      const virtual = new Map<string, Candidate>()
      const accepted: Array<{
        prepared: PreparedChange
        candidate: Candidate
      }> = []

      for (const item of prepared) {
        const change = item.change
        const key = change.entityType + '\u0000' + change.entityId
        let current = virtual.get(key)
        if (!current) {
          const row = this.db
            .prepare('SELECT * FROM entities WHERE entity_type = ? AND entity_id = ?')
            .get(change.entityType, change.entityId) as StoredRow | undefined
          if (row) current = candidateFromRow(row)
        }

        if (current && current.category !== change.category) {
          throw new ApiError(409, 'category_conflict', 'An entity cannot change category')
        }

        const incoming: Candidate = {
          category: change.category,
          deviceId: change.deviceId,
          updatedAt: change.updatedAt,
          payloadJson: item.payloadJson,
          deleted: 'deleted' in change && change.deleted === true,
          deletedAt: change.deleted ? change.deletedAt : null,
          serverRev: 0
        }

        if (!current || compareVersion(incoming, current) > 0) {
          if (revision >= Number.MAX_SAFE_INTEGER) {
            throw new ApiError(507, 'revision_limit', 'The server revision limit has been reached')
          }
          revision += 1
          incoming.serverRev = revision
          virtual.set(key, incoming)
          accepted.push({ prepared: item, candidate: incoming })
        } else if (current) {
          virtual.set(key, current)
        }
      }

      if (accepted.length > 0) {
        const estimatedGrowth = Math.max(
          requestBytes,
          accepted.reduce(
            (total, item) => total + Buffer.byteLength(item.prepared.payloadJson ?? '', 'utf8') * 2 + 16_384,
            0
          ) + 16_384
        )
        if (this.storageBytes() + this.reservedBytes + estimatedGrowth > this.maxStorageBytes) {
          throw new ApiError(507, 'storage_limit', 'The server storage limit would be exceeded')
        }
      }

      const upsertEntity = this.db.prepare(
        [
          'INSERT INTO entities (',
          '  entity_type, entity_id, category, device_id, updated_at, payload, deleted, deleted_at, server_rev',
          ') VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)',
          'ON CONFLICT (entity_type, entity_id) DO UPDATE SET',
          '  category = excluded.category,',
          '  device_id = excluded.device_id,',
          '  updated_at = excluded.updated_at,',
          '  payload = excluded.payload,',
          '  deleted = excluded.deleted,',
          '  deleted_at = excluded.deleted_at,',
          '  server_rev = excluded.server_rev'
        ].join('\n')
      )
      const appendChange = this.db.prepare(
        [
          'INSERT INTO change_log (',
          '  server_rev, entity_type, entity_id, category, device_id, updated_at, payload, deleted, deleted_at',
          ') VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)'
        ].join('\n')
      )

      for (const item of accepted) {
        const change = item.prepared.change
        const candidate = item.candidate
        const values = [
          change.entityType,
          change.entityId,
          candidate.category,
          candidate.deviceId,
          candidate.updatedAt,
          candidate.payloadJson,
          candidate.deleted ? 1 : 0,
          candidate.deletedAt,
          candidate.serverRev
        ]
        upsertEntity.run(...values)
        appendChange.run(
          candidate.serverRev,
          change.entityType,
          change.entityId,
          candidate.category,
          candidate.deviceId,
          candidate.updatedAt,
          candidate.payloadJson,
          candidate.deleted ? 1 : 0,
          candidate.deletedAt
        )
      }

      if (accepted.length > 0) {
        this.db.prepare('UPDATE sync_meta SET value = ? WHERE key = ?').run(revision, 'revision')
      }

      const winners = prepared.map(({ change }) => {
        const key = change.entityType + '\u0000' + change.entityId
        const candidate = virtual.get(key)
        if (!candidate) throw new Error('A pushed entity has no authoritative winner')
        return toWinner(candidate, change.entityType, change.entityId)
      })

      return {
        revision,
        changes: winners,
        acceptedRevisions: accepted.map(({ candidate }) => candidate.serverRev)
      }
    })

    try {
      return apply.immediate()
    } catch (error) {
      if (error instanceof Error && 'code' in error && error.code === 'SQLITE_FULL') {
        throw new ApiError(507, 'storage_limit', 'The server storage limit would be exceeded')
      }
      throw error
    }
  }

  pull(cursor: number, categories: Set<Category> | undefined, limit: number): PullResult {
    const revision = this.getRevision()
    if (cursor > revision) throw new ApiError(400, 'invalid_cursor', 'The cursor is ahead of the server revision')

    const rows = this.db
      .prepare(
        [
          'SELECT * FROM change_log',
          'WHERE server_rev > ? AND server_rev <= ?',
          'ORDER BY server_rev ASC',
          'LIMIT ?'
        ].join('\n')
      )
      .all(cursor, revision, limit + 1) as StoredRow[]
    const page = rows.slice(0, limit)
    const nextCursor = page.length === 0 ? cursor : page[page.length - 1]!.server_rev

    return {
      changes: page
        .filter((row) => categories === undefined || categories.has(row.category))
        .map((row) => toWinner(candidateFromRow(row), row.entity_type, row.entity_id)),
      cursor: nextCursor,
      hasMore: rows.length > limit
    }
  }

  stats(): StoreStats {
    const aggregate = this.db
      .prepare('SELECT COUNT(*) AS entities, COALESCE(SUM(deleted), 0) AS tombstones FROM entities')
      .get() as { entities: number; tombstones: number }
    const blobStats = this.db.prepare('SELECT COUNT(*) AS count, COALESCE(SUM(size), 0) AS bytes FROM blobs').get() as {
      count: number
      bytes: number
    }
    const categoryRows = this.db
      .prepare('SELECT category, COUNT(*) AS count FROM entities GROUP BY category')
      .all() as Array<{ category: Category; count: number }>
    const categories = Object.fromEntries(CATEGORIES.map((category) => [category, 0])) as Record<Category, number>
    for (const row of categoryRows) categories[row.category] = row.count

    return {
      entities: aggregate.entities,
      tombstones: aggregate.tombstones,
      blobs: blobStats.count,
      blobBytes: blobStats.bytes,
      storageBytes: this.storageBytes() + this.reservedBytes,
      categories
    }
  }

  reserveStorage(bytes: number): void {
    if (this.storageBytes() + this.reservedBytes + bytes > this.maxStorageBytes) {
      throw new ApiError(507, 'storage_limit', 'The server storage limit would be exceeded')
    }
    this.reservedBytes += bytes
  }

  releaseStorage(bytes: number): void {
    this.reservedBytes -= bytes
    if (this.reservedBytes < 0) throw new Error('Blob storage reservation underflow')
  }

  findBlob(sha256: string): BlobInfo | null {
    const row = this.db.prepare('SELECT sha256, size FROM blobs WHERE sha256 = ?').get(sha256) as StoredBlob | undefined
    if (!row) return null

    const filePath = join(this.blobsDir, row.sha256)
    try {
      if (lstatSync(filePath).isFile() && statSync(filePath).size === row.size) {
        return { ...row, filePath }
      }
    } catch (error) {
      if (!(typeof error === 'object' && error !== null && 'code' in error && error.code === 'ENOENT')) {
        throw error
      }
    }

    this.db.prepare('DELETE FROM blobs WHERE sha256 = ?').run(sha256)
    return null
  }

  commitBlob(temporaryPath: string, sha256: string, size: number): boolean {
    if (size > this.maxBlobBytes)
      throw new ApiError(413, 'blob_too_large', 'The blob exceeds the configured size limit')

    const commit = this.db.transaction(() => {
      const existing = this.db.prepare('SELECT sha256 FROM blobs WHERE sha256 = ?').get(sha256)
      if (existing) return false

      if (this.storageBytes() + this.reservedBytes + BLOB_METADATA_RESERVE > this.maxStorageBytes) {
        throw new ApiError(507, 'storage_limit', 'The server storage limit would be exceeded')
      }

      const finalPath = join(this.blobsDir, sha256)
      linkSync(temporaryPath, finalPath)
      try {
        this.db.prepare('INSERT INTO blobs (sha256, size) VALUES (?, ?)').run(sha256, size)
        unlinkSync(temporaryPath)
      } catch (error) {
        unlinkSync(finalPath)
        throw error
      }
      return true
    })

    try {
      return commit.immediate()
    } catch (error) {
      const row = this.db.prepare('SELECT sha256 FROM blobs WHERE sha256 = ?').get(sha256)
      if (!row) {
        try {
          unlinkSync(join(this.blobsDir, sha256))
        } catch (unlinkError) {
          if (
            !(
              typeof unlinkError === 'object' &&
              unlinkError !== null &&
              'code' in unlinkError &&
              unlinkError.code === 'ENOENT'
            )
          ) {
            throw unlinkError
          }
        }
      }
      throw error
    }
  }

  close(): void {
    this.db.close()
  }

  private storageBytes(): number {
    const databaseBytes =
      fileSize(this.databasePath) +
      fileSize(this.databasePath + '-wal') +
      fileSize(this.databasePath + '-shm') +
      fileSize(this.databasePath + '-journal') +
      fileSize(this.tokenPath)
    const blobBytes = this.db.prepare('SELECT COALESCE(SUM(size), 0) AS bytes FROM blobs').get() as { bytes: number }
    return databaseBytes + blobBytes.bytes
  }
}
