import { createHash } from 'node:crypto'

import type Database from 'better-sqlite3'

import type { DbService } from '@data/db/DbService'
import { SYNC_CATEGORIES, type SyncCategory } from '@shared/ipc/schemas/selfHostedSync'

import { assertHistoryRoots } from './activeHistory'
import type { EntityAdapter, LocalEntity, RemoteChange, SyncChange } from './entity'
import type { FileTransferHooks } from './fileTransfer'
import { hasTranslationTarget, plannedTranslationTargets, translationReady } from './translationFiles'
import type { SyncTransport } from './transport'

const BATCH_SIZE = 100
const ABSENT = '\u0000absent'
const WITHHELD = '\u0000withheld'

interface Baseline {
  entity_type: string
  entity_id: string
  category: SyncCategory
  content_hash: string
  change_json: string
  updated_at: number
  synced_rev: number
}

interface OutboxRow {
  entity_type: string
  entity_id: string
  category: SyncCategory
  content_hash: string
  change_json: string
}

interface InboxRow {
  server_rev: number
  entity_type: string
  entity_id: string
  category: SyncCategory
  change_json: string
  local_hash: string
  local_state_json: string | null
}

interface Snapshot {
  hash: string
  change?: SyncChange
}

interface PendingChange {
  latest: InboxRow
  first: InboxRow
  change: RemoteChange
  adapter: EntityAdapter
  localState?: SyncChange
}

type ApplyStatus = 'applied' | 'partial' | 'deferred' | 'same' | 'local-wins'
type EngineTransport = Pick<SyncTransport, 'push' | 'pull' | 'hasBlob' | 'putBlob' | 'getBlob'>

interface ApplyOutcome {
  pending: PendingChange
  status: ApplyStatus
  wrote: boolean
  localChange?: SyncChange
}

function canonicalJson(value: unknown): string {
  if (value === null || typeof value !== 'object') {
    if (typeof value === 'number' && !Number.isFinite(value)) return 'null'
    const result = JSON.stringify(value)
    if (result === undefined) throw new Error('Cannot canonicalize sync data')
    return result
  }
  if (Array.isArray(value)) return `[${value.map((item) => canonicalJson(item)).join(',')}]`
  const record = value as Record<string, unknown>
  const fields = Object.keys(record)
    .filter((key) => record[key] !== undefined && typeof record[key] !== 'function' && typeof record[key] !== 'symbol')
    .sort()
    .map((key) => `${JSON.stringify(key)}:${canonicalJson(record[key])}`)
  return `{${fields.join(',')}}`
}

// Source/effective clocks are carried on the envelope; they are not portable content edits.
function portableContent(payload: Record<string, unknown>) {
  return Object.fromEntries(Object.entries(payload).filter(([key]) => key !== 'updatedAt' && key !== 'createdAt'))
}

function contentHash(change: SyncChange | RemoteChange): string {
  const portable = change.deleted
    ? {
        entityType: change.entityType,
        entityId: change.entityId,
        category: change.category,
        updatedAt: change.updatedAt,
        deleted: true,
        deletedAt: change.deletedAt
      }
    : {
        entityType: change.entityType,
        entityId: change.entityId,
        category: change.category,
        updatedAt: change.updatedAt,
        payload: portableContent(change.payload)
      }
  return createHash('sha256').update(canonicalJson(portable)).digest('hex')
}

function keyOf(entityType: string, entityId: string): string {
  return JSON.stringify([entityType, entityId])
}

function parseChange<T extends SyncChange | RemoteChange>(value: string): T {
  return JSON.parse(value) as T
}

function timestamp(change: SyncChange): number {
  return change.updatedAt
}

function compareChanges(left: SyncChange, right: SyncChange): number {
  const time = timestamp(left) - timestamp(right)
  if (time) return Math.sign(time)
  return left.deviceId === right.deviceId ? 0 : left.deviceId > right.deviceId ? 1 : -1
}

function sameContent(local: LocalEntity, change: SyncChange): boolean {
  if (local.withheld) return false
  if (change.deleted) return local.deletedAt === change.deletedAt
  return (
    local.deletedAt === undefined &&
    local.payload !== null &&
    canonicalJson(portableContent(local.payload)) === canonicalJson(portableContent(change.payload))
  )
}

function snapshotChange(change: SyncChange): Snapshot {
  return { hash: contentHash(change), change }
}

function parseOptionalChange(value: string | null): SyncChange | undefined {
  return value ? parseChange<SyncChange>(value) : undefined
}

function rowChange<T extends SyncChange | RemoteChange>(row: { change_json: string }): T {
  return parseChange<T>(row.change_json)
}

function isValidTimestamp(value: number | undefined): value is number {
  return value !== undefined && Number.isSafeInteger(value) && value >= 0
}

export class SyncEngine {
  private running = false
  private readonly files?: FileTransferHooks

  constructor(
    private readonly sqlite: Database.Database,
    private readonly db: Pick<DbService, 'getDb' | 'withWriteTx'>,
    private readonly entities: readonly EntityAdapter[],
    files?: FileTransferHooks
  ) {
    this.files = files
    sqlite.exec(`
      CREATE TABLE IF NOT EXISTS sync_baseline (
        entity_type TEXT NOT NULL,
        entity_id TEXT NOT NULL,
        category TEXT NOT NULL,
        content_hash TEXT NOT NULL,
        change_json TEXT NOT NULL,
        updated_at INTEGER NOT NULL,
        synced_rev INTEGER NOT NULL,
        PRIMARY KEY (entity_type, entity_id)
      );
      CREATE TABLE IF NOT EXISTS sync_engine_outbox (
        entity_type TEXT NOT NULL,
        entity_id TEXT NOT NULL,
        category TEXT NOT NULL,
        content_hash TEXT NOT NULL,
        change_json TEXT NOT NULL,
        detected_at INTEGER NOT NULL,
        PRIMARY KEY (entity_type, entity_id)
      );
      CREATE TABLE IF NOT EXISTS sync_engine_inbox (
        server_rev INTEGER NOT NULL,
        entity_type TEXT NOT NULL,
        entity_id TEXT NOT NULL,
        category TEXT NOT NULL,
        change_json TEXT NOT NULL,
        local_hash TEXT NOT NULL,
        local_state_json TEXT,
        PRIMARY KEY (server_rev, entity_type, entity_id)
      );
      CREATE TABLE IF NOT EXISTS sync_engine_cursor (
        category TEXT PRIMARY KEY,
        revision INTEGER NOT NULL
      );
    `)
  }

  async run(
    deviceId: string,
    categories: SyncCategory[],
    transport: EngineTransport,
    signal: AbortSignal
  ): Promise<number> {
    if (this.running) throw new Error('A sync run is already in progress')
    this.running = true
    try {
      const enabled = new Set(SYNC_CATEGORIES.filter((category) => categories.includes(category)))
      this.throwIfAborted(signal)
      await this.captureLocalState(deviceId, enabled, signal)
      let revision = this.currentRevision(enabled)
      revision = Math.max(revision, await this.pushPending(deviceId, enabled, transport, signal))

      const pulledCursors = new Map<SyncCategory, number>()
      for (const category of SYNC_CATEGORIES) {
        if (!enabled.has(category)) continue
        const pulled = await this.pullCategory(deviceId, category, transport, signal)
        pulledCursors.set(category, pulled)
        revision = Math.max(revision, pulled)
      }

      this.throwIfAborted(signal)
      await this.captureLocalState(deviceId, enabled, signal)
      await this.applyInbox(deviceId, enabled, transport, signal, pulledCursors)
      return revision
    } finally {
      this.running = false
    }
  }

  reset(): void {
    if (this.running) throw new Error('Cannot reset sync state during a sync run')
    this.sqlite.transaction(() => {
      this.files?.reset?.()
      this.sqlite.exec(`
        DELETE FROM sync_baseline;
        DELETE FROM sync_engine_outbox;
        DELETE FROM sync_engine_inbox;
        DELETE FROM sync_engine_cursor;
      `)
    })()
  }

  private throwIfAborted(signal: AbortSignal): void {
    if (signal.aborted) throw signal.reason ?? new Error('Sync was aborted')
  }

  private baseline(entityType: string, entityId: string): Baseline | undefined {
    return this.sqlite
      .prepare('SELECT * FROM sync_baseline WHERE entity_type=? AND entity_id=?')
      .get(entityType, entityId) as Baseline | undefined
  }

  private outbox(entityType: string, entityId: string): OutboxRow | undefined {
    return this.sqlite
      .prepare('SELECT * FROM sync_engine_outbox WHERE entity_type=? AND entity_id=?')
      .get(entityType, entityId) as OutboxRow | undefined
  }

  private stateSnapshot(
    adapter: EntityAdapter,
    entityId: string,
    local: LocalEntity | undefined,
    deviceId: string,
    baseline: Baseline | undefined,
    outbox: OutboxRow | undefined,
    reference?: SyncChange
  ): Snapshot {
    if (local?.withheld || (local && local.payload === null && local.deletedAt === undefined)) return { hash: WITHHELD }

    if (!local) {
      const previous = outbox
        ? rowChange<SyncChange>(outbox)
        : (reference ?? (baseline && rowChange<RemoteChange>(baseline)))
      if (!previous) return { hash: ABSENT }
      if (previous.deleted) return snapshotChange(previous)
      const deletedAt = Math.max(Date.now(), timestamp(previous) + 1)
      return snapshotChange({
        entityType: adapter.type,
        entityId,
        category: adapter.category,
        deviceId,
        updatedAt: deletedAt,
        deleted: true,
        deletedAt
      })
    }

    const previous = outbox
      ? rowChange<SyncChange>(outbox)
      : (reference ?? (baseline && rowChange<RemoteChange>(baseline)))
    const deleted = local.deletedAt !== undefined
    const updatedAt = deleted ? (local.updatedAt ?? local.deletedAt) : local.updatedAt
    const matchingPrevious = previous && sameContent(local, previous)
    const previousUpdatedAt = Math.max(previous?.updatedAt ?? 0, baseline?.updated_at ?? 0)
    const hasPrevious = previous !== undefined || baseline !== undefined
    const timestampValue = matchingPrevious
      ? previous.updatedAt
      : isValidTimestamp(updatedAt) && (!hasPrevious || updatedAt > previousUpdatedAt)
        ? updatedAt
        : Math.max(Date.now(), hasPrevious ? previousUpdatedAt + 1 : 0)

    if (deleted) {
      return snapshotChange({
        entityType: adapter.type,
        entityId,
        category: adapter.category,
        deviceId,
        updatedAt: timestampValue,
        deleted: true,
        deletedAt: local.deletedAt!
      })
    }
    return snapshotChange({
      entityType: adapter.type,
      entityId,
      category: adapter.category,
      deviceId,
      updatedAt: timestampValue,
      payload: local.payload!
    })
  }

  private insertOutbox(change: SyncChange): void {
    this.sqlite
      .prepare(
        `INSERT INTO sync_engine_outbox
          (entity_type,entity_id,category,content_hash,change_json,detected_at)
         VALUES (?,?,?,?,?,?) ON CONFLICT(entity_type,entity_id) DO UPDATE SET
           category=excluded.category,content_hash=excluded.content_hash,change_json=excluded.change_json
         WHERE excluded.content_hash<>sync_engine_outbox.content_hash`
      )
      .run(change.entityType, change.entityId, change.category, contentHash(change), JSON.stringify(change), Date.now())
  }

  private captureLocalChanges(deviceId: string, categories: ReadonlySet<SyncCategory>): void {
    if (!categories.size) return
    const appDb = this.db.getDb()
    const transaction = this.sqlite.transaction(() => {
      for (const adapter of this.entities) {
        if (!categories.has(adapter.category)) continue
        const localById = new Map(adapter.scan(appDb).map((local) => [local.id, local]))
        const baselines = this.sqlite
          .prepare('SELECT * FROM sync_baseline WHERE entity_type=? AND category=?')
          .all(adapter.type, adapter.category) as Baseline[]
        const outboxes = this.sqlite
          .prepare('SELECT * FROM sync_engine_outbox WHERE entity_type=? AND category=?')
          .all(adapter.type, adapter.category) as OutboxRow[]
        const inboxes = this.sqlite
          .prepare(
            `SELECT i.* FROM sync_engine_inbox i
             WHERE i.entity_type=? AND i.category=? AND i.server_rev=(
               SELECT MIN(i2.server_rev) FROM sync_engine_inbox i2
               WHERE i2.entity_type=i.entity_type AND i2.entity_id=i.entity_id
             )`
          )
          .all(adapter.type, adapter.category) as InboxRow[]
        const baselineById = new Map(baselines.map((row) => [row.entity_id, row]))
        const outboxById = new Map(outboxes.map((row) => [row.entity_id, row]))
        const inboxById = new Map(inboxes.map((row) => [row.entity_id, row]))
        const ids = new Set([...localById.keys(), ...baselineById.keys(), ...outboxById.keys(), ...inboxById.keys()])

        for (const entityId of ids) {
          const local = localById.get(entityId)
          const baseline = baselineById.get(entityId)
          const outbox = outboxById.get(entityId)
          const inbox = inboxById.get(entityId)
          const reference = inbox ? parseOptionalChange(inbox.local_state_json) : undefined
          const snapshot = this.stateSnapshot(adapter, entityId, local, deviceId, baseline, outbox, reference)
          if (snapshot.hash === WITHHELD) {
            if (outbox)
              this.sqlite
                .prepare('DELETE FROM sync_engine_outbox WHERE entity_type=? AND entity_id=?')
                .run(adapter.type, entityId)
            continue
          }

          if (inbox) {
            if (snapshot.hash !== inbox.local_hash && snapshot.change) this.insertOutbox(snapshot.change)
            continue
          }
          if (outbox) {
            if (snapshot.change && snapshot.hash !== outbox.content_hash) this.insertOutbox(snapshot.change)
            continue
          }
          if (!baseline || snapshot.hash !== baseline.content_hash) {
            if (snapshot.change) this.insertOutbox(snapshot.change)
          }
        }
      }
    })
    transaction()
  }

  private localSnapshots(deviceId: string, category: SyncCategory): Map<string, Snapshot> {
    const appDb = this.db.getDb()
    const snapshots = new Map<string, Snapshot>()
    for (const adapter of this.entities) {
      if (adapter.category !== category) continue
      const locals = new Map(adapter.scan(appDb).map((local) => [local.id, local]))
      const baselines = this.sqlite
        .prepare('SELECT * FROM sync_baseline WHERE entity_type=? AND category=?')
        .all(adapter.type, category) as Baseline[]
      const outboxes = this.sqlite
        .prepare('SELECT * FROM sync_engine_outbox WHERE entity_type=? AND category=?')
        .all(adapter.type, category) as OutboxRow[]
      const inboxes = this.sqlite
        .prepare(
          `SELECT i.* FROM sync_engine_inbox i
           WHERE i.entity_type=? AND i.category=? AND i.server_rev=(
             SELECT MIN(i2.server_rev) FROM sync_engine_inbox i2
             WHERE i2.entity_type=i.entity_type AND i2.entity_id=i.entity_id
           )`
        )
        .all(adapter.type, category) as InboxRow[]
      const baselineById = new Map(baselines.map((row) => [row.entity_id, row]))
      const outboxById = new Map(outboxes.map((row) => [row.entity_id, row]))
      const inboxById = new Map(inboxes.map((row) => [row.entity_id, row]))
      const ids = new Set([...locals.keys(), ...baselineById.keys(), ...outboxById.keys(), ...inboxById.keys()])
      for (const entityId of ids) {
        const inbox = inboxById.get(entityId)
        const snapshot = this.stateSnapshot(
          adapter,
          entityId,
          locals.get(entityId),
          deviceId,
          baselineById.get(entityId),
          outboxById.get(entityId),
          inbox ? parseOptionalChange(inbox.local_state_json) : undefined
        )
        snapshots.set(keyOf(adapter.type, entityId), snapshot)
      }
    }
    return snapshots
  }

  private async pushPending(
    deviceId: string,
    categories: ReadonlySet<SyncCategory>,
    transport: EngineTransport,
    signal: AbortSignal
  ): Promise<number> {
    const attempted = new Set<string>()
    let revision = 0
    while (true) {
      this.throwIfAborted(signal)
      const categoryList = [...categories]
      if (!categoryList.length) break
      const placeholders = categoryList.map(() => '?').join(',')
      const rows = this.sqlite
        .prepare(
          `SELECT * FROM sync_engine_outbox WHERE category IN (${placeholders})
           ORDER BY detected_at,entity_type,entity_id LIMIT ${BATCH_SIZE}`
        )
        .all(...categoryList) as OutboxRow[]
      const batchRows = rows.filter((row) => !attempted.has(keyOf(row.entity_type, row.entity_id)))
      if (!batchRows.length) break
      const changes = batchRows.map((row) => rowChange<SyncChange>(row))
      for (const change of changes) attempted.add(keyOf(change.entityType, change.entityId))

      for (const change of changes) {
        if (change.entityType === 'file_entry') await this.files?.upload(change, transport, signal)
      }
      const result = await transport.push(deviceId, changes)
      this.acceptPushResult(changes, result.changes, result.revision)
      revision = Math.max(revision, result.revision)
    }
    return revision
  }

  private acceptPushResult(sent: SyncChange[], winners: RemoteChange[], revision: number): void {
    const sentByKey = new Map(sent.map((change) => [keyOf(change.entityType, change.entityId), change]))
    const winnerByKey = new Map<string, RemoteChange>()
    for (const winner of winners) {
      const key = keyOf(winner.entityType, winner.entityId)
      const request = sentByKey.get(key)
      if (!request || request.category !== winner.category || winner.serverRev > revision)
        throw new Error('Sync push returned a winner for an unknown change')
      const prior = winnerByKey.get(key)
      if (!prior || prior.serverRev < winner.serverRev) winnerByKey.set(key, winner)
    }

    if (winnerByKey.size !== sentByKey.size) throw new Error('Sync push omitted a record outcome.')
    this.sqlite.transaction(() => {
      for (const [key, winner] of winnerByKey) {
        const request = sentByKey.get(key)!
        const hash = contentHash(request)
        this.sqlite
          .prepare(
            `INSERT OR IGNORE INTO sync_engine_inbox
              (server_rev,entity_type,entity_id,category,change_json,local_hash,local_state_json)
             VALUES (?,?,?,?,?,?,?)`
          )
          .run(
            winner.serverRev,
            winner.entityType,
            winner.entityId,
            winner.category,
            JSON.stringify(winner),
            hash,
            JSON.stringify(request)
          )
        this.sqlite
          .prepare('DELETE FROM sync_engine_outbox WHERE entity_type=? AND entity_id=? AND content_hash=?')
          .run(request.entityType, request.entityId, hash)
      }
    })()
  }

  private async pullCategory(
    deviceId: string,
    category: SyncCategory,
    transport: EngineTransport,
    signal: AbortSignal
  ): Promise<number> {
    let cursor = this.cursor(category)
    let revision = cursor
    while (true) {
      this.throwIfAborted(signal)
      await this.prepareFileScan(new Set([category]), signal)
      const localSnapshots = this.localSnapshots(deviceId, category)
      const page = await transport.pull(cursor, [category], BATCH_SIZE)
      if (page.cursor < cursor || (page.hasMore && page.cursor <= cursor))
        throw new Error('Sync pull cursor did not advance')
      if (page.changes.some((change) => change.category !== category))
        throw new Error('Sync pull returned a change outside the requested category')

      this.acceptPullPage(category, page.changes, localSnapshots)
      revision = Math.max(revision, page.cursor)
      if (!page.hasMore) break
      cursor = page.cursor
    }
    return revision
  }

  private acceptPullPage(
    category: SyncCategory,
    changes: RemoteChange[],
    snapshots: ReadonlyMap<string, Snapshot>
  ): void {
    this.sqlite.transaction(() => {
      for (const change of changes) {
        const snapshot = snapshots.get(keyOf(change.entityType, change.entityId)) ?? { hash: ABSENT }
        this.sqlite
          .prepare(
            `INSERT OR IGNORE INTO sync_engine_inbox
              (server_rev,entity_type,entity_id,category,change_json,local_hash,local_state_json)
             VALUES (?,?,?,?,?,?,?)`
          )
          .run(
            change.serverRev,
            change.entityType,
            change.entityId,
            category,
            JSON.stringify(change),
            snapshot.hash,
            snapshot.change ? JSON.stringify(snapshot.change) : null
          )
      }
    })()
  }

  private cursor(category: SyncCategory): number {
    return (
      (
        this.sqlite.prepare('SELECT revision FROM sync_engine_cursor WHERE category=?').get(category) as
          | { revision: number }
          | undefined
      )?.revision ?? 0
    )
  }

  private currentRevision(categories: ReadonlySet<SyncCategory>): number {
    return Math.max(0, ...[...categories].map((category) => this.cursor(category)))
  }

  private pendingChanges(categories: ReadonlySet<SyncCategory>): PendingChange[] {
    if (!categories.size) return []
    const list = [...categories]
    const rows = this.sqlite
      .prepare(
        `SELECT * FROM sync_engine_inbox WHERE category IN (${list.map(() => '?').join(',')}) ORDER BY server_rev`
      )
      .all(...list) as InboxRow[]
    const grouped = new Map<string, { first: InboxRow; latest: InboxRow }>()
    for (const row of rows) {
      const key = keyOf(row.entity_type, row.entity_id)
      const group = grouped.get(key)
      if (group) group.latest = row
      else grouped.set(key, { first: row, latest: row })
    }

    const adapters = new Map(this.entities.map((adapter) => [adapter.type, adapter]))
    const pending: PendingChange[] = []
    for (const { first, latest } of grouped.values()) {
      const adapter = adapters.get(latest.entity_type)
      if (!adapter || adapter.category !== latest.category) continue
      pending.push({
        first,
        latest,
        change: rowChange<RemoteChange>(latest),
        adapter,
        localState: parseOptionalChange(first.local_state_json)
      })
    }

    const rank = new Map(this.entities.map((adapter, index) => [adapter.type, index]))
    return pending.sort((left, right) => {
      if (left.change.deleted !== right.change.deleted) return left.change.deleted ? 1 : -1
      const order = rank.get(left.adapter.type)! - rank.get(right.adapter.type)!
      return left.change.deleted
        ? -order || left.change.serverRev - right.change.serverRev
        : order || left.change.serverRev - right.change.serverRev
    })
  }

  private async applyInbox(
    deviceId: string,
    categories: ReadonlySet<SyncCategory>,
    transport: EngineTransport,
    signal: AbortSignal,
    pulledCursors: ReadonlyMap<SyncCategory, number>
  ): Promise<void> {
    const pending = this.pendingChanges(categories)
    const fileChanges = pending.filter((item) => item.change.entityType === 'file_entry').map((item) => item.change)
    const filePreparation = fileChanges.length
      ? await this.files?.prepareApply(fileChanges, transport, signal)
      : undefined
    const baselines = new Map<string, Baseline | undefined>()
    const outboxes = new Map<string, OutboxRow | undefined>()
    const targets = plannedTranslationTargets(pending.map((item) => item.change))
    const outcomes = new Map<string, ApplyOutcome>()
    let filesCommitted = false
    try {
      this.throwIfAborted(signal)
      for (const item of pending) {
        const key = keyOf(item.change.entityType, item.change.entityId)
        baselines.set(key, this.baseline(item.change.entityType, item.change.entityId))
        outboxes.set(key, this.outbox(item.change.entityType, item.change.entityId))
      }
      if (pending.length)
        this.db.withWriteTx((tx) => {
          let remaining = pending
          while (remaining.length) {
            const next: PendingChange[] = []
            let progress = false
            for (const item of remaining) {
              const key = keyOf(item.change.entityType, item.change.entityId)
              const baseline = baselines.get(key)
              const outbox = outboxes.get(key)
              const local = item.adapter.get(tx, item.change.entityId)
              const snapshot = this.stateSnapshot(
                item.adapter,
                item.change.entityId,
                local,
                deviceId,
                baseline,
                outbox,
                item.localState
              )
              const remoteHash = contentHash(item.change)
              if (snapshot.hash === remoteHash || (item.change.deleted && snapshot.hash === ABSENT)) {
                outcomes.set(key, { pending: item, status: 'same', wrote: false })
                continue
              }

              const localChanged = Boolean(outbox) || snapshot.hash !== item.first.local_hash
              if (localChanged && snapshot.change && compareChanges(snapshot.change, item.change) >= 0) {
                outcomes.set(key, { pending: item, status: 'local-wins', wrote: false, localChange: snapshot.change })
                continue
              }

              const status = translationReady(tx, item.change, targets)
                ? item.adapter.apply(tx, item.change)
                : 'deferred'
              const previous = outcomes.get(key)
              if (status === 'applied') {
                outcomes.set(key, { pending: item, status, wrote: true })
                progress = true
              } else {
                outcomes.set(key, {
                  pending: item,
                  status: status === 'partial' || previous?.wrote ? 'partial' : 'deferred',
                  wrote: status === 'partial' || Boolean(previous?.wrote)
                })
                next.push(item)
              }
            }
            if (!progress) break
            remaining = next
          }
          for (const outcome of outcomes.values()) {
            const c = outcome.pending.change
            if (
              outcome.wrote &&
              c.entityType === 'translate_history' &&
              c.payload?.kind === 'file' &&
              !hasTranslationTarget(tx, c.entityId)
            )
              throw new Error('Waiting for the translated attachment.')
          }
          assertHistoryRoots(
            tx,
            [...outcomes.values()].filter((outcome) => outcome.wrote).map((o) => o.pending.change)
          )
        })

      const installed = [...outcomes.values()]
        .filter(
          (outcome) =>
            outcome.pending.change.entityType === 'file_entry' &&
            (outcome.status === 'applied' || outcome.status === 'partial')
        )
        .map((outcome) => outcome.pending.change)
      await filePreparation?.commit(installed)
      filesCommitted = true

      this.sqlite.transaction(() => {
        for (const [, outcome] of outcomes) {
          const change = outcome.pending.change
          const latestRev = outcome.pending.latest.server_rev
          if (outcome.status === 'same' || outcome.status === 'applied' || outcome.status === 'local-wins') {
            this.setBaseline(change)
            if (outcome.status === 'local-wins' && outcome.localChange) this.insertOutbox(outcome.localChange)
            else this.removeOutboxNotNewer(change)
            this.sqlite
              .prepare('DELETE FROM sync_engine_inbox WHERE entity_type=? AND entity_id=? AND server_rev<=?')
              .run(change.entityType, change.entityId, latestRev)
            continue
          }

          this.sqlite
            .prepare('DELETE FROM sync_engine_inbox WHERE entity_type=? AND entity_id=? AND server_rev<?')
            .run(change.entityType, change.entityId, latestRev)
          this.removeOutboxNotNewer(change)
          if (outcome.status === 'partial') {
            const snapshot = this.stateSnapshot(
              outcome.pending.adapter,
              change.entityId,
              outcome.pending.adapter.get(this.db.getDb(), change.entityId),
              deviceId,
              this.baseline(change.entityType, change.entityId),
              this.outbox(change.entityType, change.entityId),
              change
            )
            this.sqlite
              .prepare(
                `UPDATE sync_engine_inbox SET local_hash=?,local_state_json=?
               WHERE server_rev=? AND entity_type=? AND entity_id=?`
              )
              .run(
                snapshot.hash,
                snapshot.change ? JSON.stringify(snapshot.change) : null,
                latestRev,
                change.entityType,
                change.entityId
              )
          }
        }
        for (const [category, revision] of pulledCursors) {
          this.sqlite
            .prepare(
              `INSERT INTO sync_engine_cursor (category,revision) VALUES (?,?)
               ON CONFLICT(category) DO UPDATE SET revision=MAX(revision,excluded.revision)`
            )
            .run(category, revision)
        }
      })()

      const changedByAdapter = new Map<EntityAdapter, SyncChange[]>()
      for (const outcome of outcomes.values()) {
        if (!outcome.wrote) continue
        const changes = changedByAdapter.get(outcome.pending.adapter) ?? []
        changes.push(outcome.pending.change)
        changedByAdapter.set(outcome.pending.adapter, changes)
      }
      for (const [adapter, changes] of changedByAdapter) adapter.afterCommit?.(changes)
    } catch (error) {
      if (!filesCommitted) {
        try {
          await filePreparation?.abort()
        } catch {
          // Preserve the sync failure; prepared files are reconciled on retry.
        }
      }
      throw error
    }
  }

  private setBaseline(change: RemoteChange): void {
    this.sqlite
      .prepare(
        `INSERT INTO sync_baseline
          (entity_type,entity_id,category,content_hash,change_json,updated_at,synced_rev)
         VALUES (?,?,?,?,?,?,?)
         ON CONFLICT(entity_type,entity_id) DO UPDATE SET
           category=excluded.category,content_hash=excluded.content_hash,
           change_json=excluded.change_json,updated_at=excluded.updated_at,synced_rev=excluded.synced_rev
         WHERE excluded.synced_rev>=sync_baseline.synced_rev`
      )
      .run(
        change.entityType,
        change.entityId,
        change.category,
        contentHash(change),
        JSON.stringify(change),
        change.updatedAt,
        change.serverRev
      )
  }

  private async prepareFileScan(categories: ReadonlySet<SyncCategory>, signal: AbortSignal): Promise<void> {
    if (categories.has('files') && this.entities.some((adapter) => adapter.type === 'file_entry')) {
      await this.files?.prepareScan(this.db.getDb(), signal)
    }
  }

  private async captureLocalState(
    deviceId: string,
    categories: ReadonlySet<SyncCategory>,
    signal: AbortSignal
  ): Promise<void> {
    await this.prepareFileScan(categories, signal)
    this.throwIfAborted(signal)
    this.captureLocalChanges(deviceId, categories)
  }

  private removeOutboxNotNewer(change: SyncChange): void {
    const pending = this.outbox(change.entityType, change.entityId)
    if (!pending || compareChanges(rowChange<SyncChange>(pending), change) > 0) return
    this.sqlite
      .prepare('DELETE FROM sync_engine_outbox WHERE entity_type=? AND entity_id=?')
      .run(change.entityType, change.entityId)
  }
}
