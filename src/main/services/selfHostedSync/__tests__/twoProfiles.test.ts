import { randomUUID } from 'node:crypto'
import { mkdir, mkdtemp, readFile, rm, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'

import { setupTestDatabase } from '@test-helpers/db'
import { MockApplicationUtils } from '@test-mocks/main/application'
import { MockMainDbServiceUtils } from '@test-mocks/main/DbService'
import Database from 'better-sqlite3'
import { eq } from 'drizzle-orm'
import { drizzle } from 'drizzle-orm/better-sqlite3'
import { expect, it, vi } from 'vitest'

import { application } from '@application'
import { fileEntryTable } from '@data/db/schemas/file'
import type { DbType } from '@data/db/types'
import { BaseService } from '@main/core/lifecycle'
import { FileManager } from '@main/services/file'

import { startSyncServer } from '../../../../../sync-server/src/server'
import { SyncEngine } from '../engine'
import { ManagedFiles } from '../managedFiles'
import { SyncTransport } from '../transport'

const dbh = setupTestDatabase()

it('replicates an attachment, offline edit and deletion across two profiles using the deployed HTTP contract', async () => {
  const root = await mkdtemp(join(tmpdir(), 'cherry-two-profiles-'))
  const token = 'test-only-token-for-two-isolated-device-profiles'
  const server = await startSyncServer({ dataDir: join(root, 'server'), host: '127.0.0.1', port: 0, token })
  const controller = new AbortController()
  const transport = new SyncTransport(server.url, token, controller.signal)
  await dbh.sqlite.backup(join(root, 'second.sqlite'))
  const sqliteB = new Database(join(root, 'second.sqlite'))
  sqliteB.pragma('foreign_keys = ON')
  const dbB = drizzle({ client: sqliteB, casing: 'snake_case' })
  const sidecars: Database.Database[] = []
  const pathSpy = vi.spyOn(application, 'getPath')
  const profile = async (name: string, db: DbType) => {
    const base = join(root, name)
    const dir = join(base, 'Data', 'Files')
    await mkdir(dir, { recursive: true })
    const sidecar = new Database(join(base, 'self-hosted-sync.sqlite'))
    sidecars.push(sidecar)
    const blobs = new ManagedFiles()
    const engine = new SyncEngine(
      sidecar,
      { getDb: () => db, withWriteTx: (fn) => db.transaction(fn, { behavior: 'immediate' }) },
      blobs.entities,
      blobs
    )
    BaseService.resetInstances()
    const manager = new FileManager()
    const run = async () => {
      MockMainDbServiceUtils.setDb(db)
      MockApplicationUtils.setService('FileManager', manager)
      pathSpy.mockImplementation((key, file) => join(key === 'feature.files.data' ? dir : base, file ?? ''))
      return engine.run(name, ['files'], transport, controller.signal)
    }
    return { dir, run }
  }

  try {
    const a = await profile('a', dbh.db)
    const b = await profile('b', dbB)
    const id = randomUUID()
    await writeFile(join(a.dir, id), 'first')
    dbh.db
      .insert(fileEntryTable)
      .values({ id, name: 'shared', origin: 'internal', size: 5, createdAt: 1, updatedAt: 1 })
      .run()
    await a.run()
    await b.run()
    expect(await readFile(join(b.dir, id), 'utf8')).toBe('first')
    expect(dbB.select().from(fileEntryTable).get()?.updatedAt).toBe(1)
    await writeFile(join(b.dir, id), 'other')
    await b.run()
    await a.run()
    expect(await readFile(join(a.dir, id), 'utf8')).toBe('other')
    const deletedAt = Date.now() + 1
    dbB.update(fileEntryTable).set({ deletedAt, updatedAt: deletedAt }).where(eq(fileEntryTable.id, id)).run()
    await b.run()
    await a.run()
    expect(dbh.db.select().from(fileEntryTable).get()?.deletedAt).toBe(deletedAt)
    const before = await transport.status()
    await a.run()
    await b.run()
    expect((await transport.status()).revision).toBe(before.revision)
  } finally {
    controller.abort()
    pathSpy.mockRestore()
    MockApplicationUtils.resetServices()
    MockMainDbServiceUtils.setDb(dbh.db)
    for (const sidecar of sidecars) sidecar.close()
    sqliteB.close()
    await server.close()
    await rm(root, { recursive: true, force: true })
  }
}, 30_000)
