import { Readable } from 'node:stream'

import { setupTestDatabase } from '@test-helpers/db'
import Database from 'better-sqlite3'
import { eq } from 'drizzle-orm'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'

import type { DbService } from '@data/db/DbService'
import { groupTable } from '@data/db/schemas/group'
import type { DbOrTx, DbType } from '@data/db/types'

import { SyncEngine } from '../engine'
import type { EntityAdapter, LocalEntity, RemoteChange, SyncChange } from '../entity'
import type { FileTransferHooks } from '../fileTransfer'
import type { SyncTransport } from '../transport'

const GROUP_ID = '00000000-0000-4000-8000-000000000001'

const groupAdapter = {
  type: 'group',
  category: 'other',
  scan: (db) =>
    db
      .select()
      .from(groupTable)
      .all()
      .map((row) => ({ id: row.id, payload: row, updatedAt: row.updatedAt })),
  get: (db, id) => {
    const row = db.select().from(groupTable).where(eq(groupTable.id, id)).get()
    return row && { id: row.id, payload: row, updatedAt: row.updatedAt }
  },
  apply: (tx, change) => {
    if (change.deleted) {
      tx.delete(groupTable).where(eq(groupTable.id, change.entityId)).run()
      return 'applied'
    }
    const row = change.payload as typeof groupTable.$inferInsert
    tx.insert(groupTable)
      .values(row)
      .onConflictDoUpdate({
        target: groupTable.id,
        set: {
          entityType: row.entityType,
          name: row.name,
          orderKey: row.orderKey,
          createdAt: row.createdAt,
          updatedAt: change.updatedAt
        }
      })
      .run()
    return 'applied'
  }
} satisfies EntityAdapter

function dbService(db: DbType): Pick<DbService, 'getDb' | 'withWriteTx'> {
  return {
    getDb: () => db,
    withWriteTx: <T>(callback: (tx: DbOrTx) => T) => db.transaction(callback, { behavior: 'immediate' })
  }
}

function groupChange(name: string, updatedAt: number, deviceId = 'device-a'): SyncChange {
  return {
    entityType: 'group',
    entityId: GROUP_ID,
    category: 'other',
    deviceId,
    updatedAt,
    payload: {
      id: GROUP_ID,
      entityType: 'topic',
      name,
      orderKey: 'a0',
      createdAt: 1,
      updatedAt
    }
  }
}

function insertGroup(db: DbType, change: SyncChange): void {
  db.insert(groupTable)
    .values(change.payload as typeof groupTable.$inferInsert)
    .onConflictDoUpdate({
      target: groupTable.id,
      set: {
        name: (change.payload as typeof groupTable.$inferSelect).name,
        updatedAt: change.updatedAt
      }
    })
    .run()
}

function readGroup(db: DbType): typeof groupTable.$inferSelect | undefined {
  return db.select().from(groupTable).where(eq(groupTable.id, GROUP_ID)).get()
}

class MemoryServer {
  private revision = 0
  private pushCount = 0
  private readonly history: RemoteChange[] = []
  private readonly current = new Map<string, RemoteChange>()

  get pushes(): number {
    return this.pushCount
  }

  currentChange(entityType: string, entityId: string): RemoteChange | undefined {
    return this.current.get(`${entityType}:${entityId}`)
  }

  readonly transport: Pick<SyncTransport, 'push' | 'pull' | 'hasBlob' | 'putBlob' | 'getBlob'> = {
    push: async (_deviceId, changes) => {
      this.pushCount += 1
      const winners = changes.map((change) => {
        const key = `${change.entityType}:${change.entityId}`
        const current = this.current.get(key)
        if (!current || this.compare(change, current) > 0) {
          const winner = { ...change, serverRev: ++this.revision }
          this.current.set(key, winner)
          this.history.push(winner)
          return winner
        }
        return current
      })
      return { changes: winners, revision: this.revision }
    },
    pull: async (since, categories, limit) => {
      const matching = this.history.filter((change) => change.serverRev > since && categories.includes(change.category))
      const changes = matching.slice(0, limit)
      const hasMore = matching.length > changes.length
      const cursor = hasMore ? changes[changes.length - 1].serverRev : this.revision
      return { changes, cursor, hasMore }
    },
    hasBlob: async () => false,
    putBlob: async () => {},
    getBlob: async () => Readable.from([])
  }

  private compare(left: SyncChange, right: SyncChange): number {
    return left.updatedAt - right.updatedAt || left.deviceId.localeCompare(right.deviceId)
  }
}

describe('self-hosted sync engine', () => {
  const dbh = setupTestDatabase()
  let sidecar: Database.Database

  beforeEach(() => {
    sidecar = new Database(':memory:')
  })

  afterEach(() => {
    sidecar.close()
  })

  it('replays local edits, applies a newer remote winner, and syncs a deletion', async () => {
    const server = new MemoryServer()
    const engine = new SyncEngine(sidecar, dbService(dbh.db), [groupAdapter])
    const signal = new AbortController().signal

    const local = groupChange('Local', 10)
    insertGroup(dbh.db, local)
    await engine.run('device-a', ['other'], server.transport, signal)
    expect(server.currentChange('group', GROUP_ID)?.payload).toMatchObject({ name: 'Local' })

    await server.transport.push('device-b', [groupChange('Remote', 20, 'device-b')])
    await engine.run('device-a', ['other'], server.transport, signal)
    expect(readGroup(dbh.db)?.name).toBe('Remote')

    dbh.db.delete(groupTable).where(eq(groupTable.id, GROUP_ID)).run()
    await engine.run('device-a', ['other'], server.transport, signal)
    const tombstone = server.currentChange('group', GROUP_ID)
    expect(tombstone).toMatchObject({ deleted: true, deviceId: 'device-a' })
    expect(readGroup(dbh.db)).toBeUndefined()
  })

  it('advances same-millisecond edits from the synced baseline without echoing stable content', async () => {
    const server = new MemoryServer()
    const engine = new SyncEngine(sidecar, dbService(dbh.db), [groupAdapter])
    const signal = new AbortController().signal
    insertGroup(dbh.db, groupChange('Initial', 100))
    await engine.run('device-a', ['other'], server.transport, signal)

    insertGroup(dbh.db, groupChange('First edit', 100))
    await engine.run('device-a', ['other'], server.transport, signal)
    const firstEdit = server.currentChange('group', GROUP_ID)!
    expect(firstEdit.payload).toMatchObject({ name: 'First edit' })
    expect(firstEdit.updatedAt).toBeGreaterThan(100)

    insertGroup(dbh.db, groupChange('Second edit', 100))
    await engine.run('device-a', ['other'], server.transport, signal)
    const secondEdit = server.currentChange('group', GROUP_ID)!
    expect(secondEdit.payload).toMatchObject({ name: 'Second edit' })
    expect(secondEdit.updatedAt).toBeGreaterThan(firstEdit.updatedAt)
    expect(
      sidecar
        .prepare('SELECT updated_at,synced_rev FROM sync_baseline WHERE entity_type=? AND entity_id=?')
        .get('group', GROUP_ID)
    ).toEqual({ updated_at: secondEdit.updatedAt, synced_rev: secondEdit.serverRev })

    const pushes = server.pushes
    await engine.run('device-a', ['other'], server.transport, signal)
    expect(server.pushes).toBe(pushes)
  })

  it('orders tombstone conflicts by updatedAt rather than deletedAt', async () => {
    let local: LocalEntity | undefined
    let applyCount = 0
    const adapter: EntityAdapter = {
      type: 'group',
      category: 'other',
      scan: () => (local ? [local] : []),
      get: () => local,
      apply: (_tx, change) => {
        applyCount += 1
        local = change.deleted
          ? { id: change.entityId, payload: null, deletedAt: change.deletedAt, updatedAt: change.updatedAt }
          : { id: change.entityId, payload: change.payload, updatedAt: change.updatedAt }
        return 'applied'
      }
    }
    const server = new MemoryServer()
    const engine = new SyncEngine(sidecar, dbService(dbh.db), [adapter])
    await server.transport.push('device-b', [groupChange('Synced', 10, 'device-b')])
    await engine.run('device-a', ['other'], server.transport, new AbortController().signal)
    expect(local?.payload).toMatchObject({ name: 'Synced' })

    const remoteTombstone: RemoteChange = {
      entityType: 'group',
      entityId: GROUP_ID,
      category: 'other',
      deviceId: 'device-b',
      updatedAt: 20,
      deleted: true,
      deletedAt: 1000,
      serverRev: 2
    }
    const transport: Pick<SyncTransport, 'push' | 'pull' | 'hasBlob' | 'putBlob' | 'getBlob'> = {
      ...server.transport,
      pull: async () => {
        local = { id: GROUP_ID, payload: null, updatedAt: 30, deletedAt: 1 }
        return { changes: [remoteTombstone], cursor: 2, hasMore: false }
      }
    }

    await engine.run('device-a', ['other'], transport, new AbortController().signal)

    expect(applyCount).toBe(1)
    expect(local).toMatchObject({ deletedAt: 1, updatedAt: 30 })
  })

  it('keeps an outbox change for retry after the transport fails', async () => {
    const server = new MemoryServer()
    const engine = new SyncEngine(sidecar, dbService(dbh.db), [groupAdapter])
    insertGroup(dbh.db, groupChange('Retry me', 10))
    const failedTransport = {
      ...server.transport,
      push: async () => {
        throw new Error('offline')
      }
    }

    await expect(engine.run('device-a', ['other'], failedTransport, new AbortController().signal)).rejects.toThrow(
      'offline'
    )
    expect(sidecar.prepare('SELECT COUNT(*) AS count FROM sync_engine_outbox').get()).toEqual({ count: 1 })

    await engine.run('device-a', ['other'], server.transport, new AbortController().signal)
    expect(sidecar.prepare('SELECT COUNT(*) AS count FROM sync_engine_outbox').get()).toEqual({ count: 0 })
    expect(server.currentChange('group', GROUP_ID)?.payload).toMatchObject({ name: 'Retry me' })
  })

  it('does not push a queued row after it becomes machine-local, and backfills a newly enabled category', async () => {
    const server = new MemoryServer()
    let withheld = false
    const adapter: EntityAdapter = {
      ...groupAdapter,
      scan(db) {
        return groupAdapter.scan(db).map((row) => (withheld ? { id: row.id, payload: null, withheld: true } : row))
      },
      get(db, id) {
        return withheld ? { id, payload: null, withheld: true } : groupAdapter.get(db, id)
      }
    }
    const engine = new SyncEngine(sidecar, dbService(dbh.db), [adapter])
    const signal = new AbortController().signal
    insertGroup(dbh.db, groupChange('Local', 10))
    await expect(
      engine.run(
        'device-a',
        ['other'],
        {
          ...server.transport,
          push: async () => {
            throw new Error('offline')
          }
        },
        signal
      )
    ).rejects.toThrow('offline')
    withheld = true
    await engine.run('device-a', ['other'], server.transport, signal)
    expect(server.currentChange('group', GROUP_ID)).toBeUndefined()
    withheld = false
    await server.transport.push('device-b', [groupChange('Another device', 20, 'device-b')])
    await engine.run('device-a', ['chats'], server.transport, signal)
    expect(readGroup(dbh.db)?.name).toBe('Local')
    await engine.run('device-a', ['other'], server.transport, signal)
    expect(readGroup(dbh.db)?.name).toBe('Another device')
  })

  it('retains remote changes deferred by an adapter until the domain is ready', async () => {
    const server = new MemoryServer()
    await server.transport.push('device-b', [groupChange('Wait for domain', 20, 'device-b')])
    let defer = true
    const adapter: EntityAdapter = {
      ...groupAdapter,
      apply: (tx, change) => (defer ? 'deferred' : groupAdapter.apply(tx, change))
    }
    const engine = new SyncEngine(sidecar, dbService(dbh.db), [adapter])
    const signal = new AbortController().signal

    await engine.run('device-a', ['other'], server.transport, signal)
    expect(readGroup(dbh.db)).toBeUndefined()
    expect(sidecar.prepare('SELECT COUNT(*) AS count FROM sync_engine_inbox').get()).toEqual({ count: 1 })

    defer = false
    await engine.run('device-a', ['other'], server.transport, signal)
    expect(readGroup(dbh.db)?.name).toBe('Wait for domain')
    expect(sidecar.prepare('SELECT COUNT(*) AS count FROM sync_engine_inbox').get()).toEqual({ count: 0 })
  })

  it('aborts the whole apply before starting its database transaction', async () => {
    const server = new MemoryServer()
    const remote: RemoteChange = {
      ...groupChange('Pause before apply', 20, 'device-b'),
      entityType: 'file_entry',
      category: 'files',
      serverRev: 1
    }
    await server.transport.push('device-b', [remote])
    const controller = new AbortController()
    const events: string[] = []
    const files: FileTransferHooks = {
      prepareScan: async () => {},
      upload: async () => {},
      prepareApply: async () => {
        controller.abort(new Error('paused'))
        return {
          commit: async () => {
            events.push('commit')
          },
          abort: async () => {
            events.push('abort')
          }
        }
      }
    }
    const fileAdapter: EntityAdapter = {
      type: 'file_entry',
      category: 'files',
      scan: () => [],
      get: () => undefined,
      apply: () => {
        events.push('apply')
        return 'applied'
      }
    }
    const engine = new SyncEngine(sidecar, dbService(dbh.db), [fileAdapter], files)

    await expect(engine.run('device-a', ['files'], server.transport, controller.signal)).rejects.toThrow('paused')

    expect(events).toEqual(['abort'])
    expect(readGroup(dbh.db)).toBeUndefined()
    expect(sidecar.prepare('SELECT COUNT(*) AS count FROM sync_engine_inbox').get()).toEqual({ count: 1 })
  })

  it('runs file preparation around network and database application', async () => {
    const events: string[] = []
    const remote: RemoteChange = {
      ...groupChange('From file transport', 20, 'device-b'),
      entityType: 'file_entry',
      category: 'files',
      serverRev: 1
    }
    const server = new MemoryServer()
    await server.transport.push('device-b', [remote])
    const local = {
      id: GROUP_ID,
      payload: groupChange('Local file', 10).payload as Record<string, unknown>,
      updatedAt: 10
    }
    const transport: Pick<SyncTransport, 'push' | 'pull' | 'hasBlob' | 'putBlob' | 'getBlob'> = {
      ...server.transport,
      push: async (deviceId, changes) => {
        events.push('push')
        return server.transport.push(deviceId, changes)
      },
      pull: async (since, categories, limit) => {
        events.push('pull')
        return server.transport.pull(since, categories, limit)
      }
    }
    let committed: RemoteChange[] = []
    const files: FileTransferHooks = {
      prepareScan: async () => {
        events.push('scan')
      },
      upload: async () => {
        events.push('upload')
      },
      prepareApply: async () => {
        events.push('prepare')
        return {
          commit: async (changes) => {
            committed = changes
            events.push('commit')
          },
          abort: async () => {
            events.push('abort')
          }
        }
      }
    }
    const adapter: EntityAdapter = {
      category: 'files',
      type: 'file_entry',
      scan: () => [local],
      get: () => local,
      apply: (tx, change) => {
        events.push('apply')
        return groupAdapter.apply(tx, { ...change, entityType: 'group', category: 'other' })
      }
    }
    const engine = new SyncEngine(sidecar, dbService(dbh.db), [adapter], files)

    await engine.run('device-a', ['files'], transport, new AbortController().signal)

    expect(events[0]).toBe('scan')
    expect(events.filter((event) => event !== 'scan')).toEqual(['upload', 'push', 'pull', 'prepare', 'apply', 'commit'])
    expect(readGroup(dbh.db)?.name).toBe('From file transport')
    expect(committed).toMatchObject([{ entityType: 'file_entry', entityId: GROUP_ID }])
  })

  it('does not install staged files for a remote winner that loses to a local edit', async () => {
    let local: LocalEntity | undefined
    let applyCount = 0
    let committed: RemoteChange[] = []
    const remote: RemoteChange = {
      entityType: 'file_entry',
      entityId: GROUP_ID,
      category: 'files',
      deviceId: 'device-b',
      updatedAt: 20,
      payload: { id: GROUP_ID, name: 'remote bytes' },
      serverRev: 1
    }
    const adapter: EntityAdapter = {
      type: 'file_entry',
      category: 'files',
      scan: () => (local ? [local] : []),
      get: () => local,
      apply: () => {
        applyCount += 1
        return 'applied'
      }
    }
    const files: FileTransferHooks = {
      prepareScan: async () => {},
      upload: async () => {},
      prepareApply: async () => ({
        commit: async (changes) => {
          committed = changes
        },
        abort: async () => {}
      })
    }
    const transport: Pick<SyncTransport, 'push' | 'pull' | 'hasBlob' | 'putBlob' | 'getBlob'> = {
      push: async () => ({ changes: [], revision: 0 }),
      pull: async () => {
        local = {
          id: GROUP_ID,
          payload: { id: GROUP_ID, name: 'local bytes' },
          updatedAt: 30
        }
        return { changes: [remote], cursor: 1, hasMore: false }
      },
      hasBlob: async () => false,
      putBlob: async () => {},
      getBlob: async () => Readable.from([])
    }
    const engine = new SyncEngine(sidecar, dbService(dbh.db), [adapter], files)

    await engine.run('device-a', ['files'], transport, new AbortController().signal)

    expect(applyCount).toBe(0)
    expect(committed).toEqual([])
    expect(sidecar.prepare('SELECT COUNT(*) AS count FROM sync_engine_outbox').get()).toEqual({ count: 1 })
  })
})
