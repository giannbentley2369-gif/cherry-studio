import { createHash, randomUUID } from 'node:crypto'
import { mkdir, mkdtemp, readFile, rm, symlink, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { Readable } from 'node:stream'
import { buffer } from 'node:stream/consumers'

import { setupTestDatabase } from '@test-helpers/db'
import { MockApplicationUtils } from '@test-mocks/main/application'
import Database from 'better-sqlite3'
import { eq } from 'drizzle-orm'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import { application } from '@application'
import { fileEntryTable } from '@data/db/schemas/file'
import { BaseService } from '@main/core/lifecycle'
import { FileManager } from '@main/services/file'

import type { RemoteChange } from '../entity'
import { ManagedFiles } from '../managedFiles'

const signal = new AbortController().signal
const digest = (bytes: string) => createHash('sha256').update(bytes).digest('hex')

describe('managed attachment transfer', () => {
  const dbh = setupTestDatabase()
  let root: string
  let dir: string
  beforeEach(async () => {
    root = await mkdtemp(join(tmpdir(), 'cherry-sync-blobs-'))
    dir = join(root, 'Data', 'Files')
    await mkdir(dir, { recursive: true })
    vi.spyOn(application, 'getPath').mockImplementation((key, name) =>
      join(key === 'feature.files.data' ? dir : root, name ?? '')
    )
    BaseService.resetInstances()
    MockApplicationUtils.setService('FileManager', new FileManager())
  })
  afterEach(async () => {
    vi.restoreAllMocks()
    MockApplicationUtils.resetServices()
    await rm(root, { recursive: true, force: true })
  })

  it('uploads only internal bytes; unchanged size and metadata do not hide an edit', async () => {
    const id = randomUUID()
    const outside = join(root, 'private.txt')
    await writeFile(outside, 'private')
    await writeFile(join(dir, id), 'one')
    dbh.db
      .insert(fileEntryTable)
      .values([
        { id, name: 'note', origin: 'internal', size: 3 },
        { id: randomUUID(), name: 'private', origin: 'external', externalPath: outside }
      ])
      .run()
    const files = new ManagedFiles()
    await files.prepareScan(dbh.db, signal)
    const [entry] = files.entities[0].scan(dbh.db)
    expect(entry.payload?.blobSha256).toBe(digest('one'))
    const bytes: Buffer[] = []
    await files.upload(
      {
        entityId: id,
        entityType: 'file_entry',
        category: 'files',
        deviceId: 'a',
        updatedAt: 1,
        payload: entry.payload!
      },
      {
        hasBlob: async () => false,
        putBlob: async (_sha, source) => {
          bytes.push(await buffer(source))
        }
      },
      signal
    )
    expect(bytes).toEqual([Buffer.from('one')])
    await writeFile(join(dir, id), 'two')
    await files.prepareScan(dbh.db, signal)
    expect(files.entities[0].scan(dbh.db).map((item) => item.payload?.blobSha256)).toEqual([digest('two')])
    if (process.platform !== 'win32') {
      await rm(join(dir, id))
      await symlink(outside, join(dir, id))
      await files.prepareScan(dbh.db, signal)
      expect(files.entities[0].get(dbh.db, id)).toMatchObject({ withheld: true, payload: null })
    }
  })

  it('verifies the digest before DB apply and publishes verified bytes only for the winning revision', async () => {
    const id = randomUUID()
    const files = new ManagedFiles()
    const change: RemoteChange = {
      deviceId: 'device-b',
      category: 'files',
      entityType: 'file_entry',
      entityId: id,
      updatedAt: 12,
      serverRev: 4,
      payload: {
        id,
        origin: 'internal',
        name: 'picture',
        ext: 'txt',
        cleanupPolicy: 'manual',
        size: 4,
        createdAt: 10,
        updatedAt: 12,
        blobSha256: digest('good')
      }
    }
    await expect(
      files.prepareApply([change], { getBlob: async () => Readable.from([Buffer.from('junk')]) }, signal)
    ).rejects.toThrow(/integrity/)
    expect(dbh.db.select().from(fileEntryTable).get()).toBeUndefined()
    const prepare = () =>
      files.prepareApply([change], { getBlob: async () => Readable.from([Buffer.from('good')]) }, signal)
    const ignored = await prepare()
    await ignored.commit([])
    await expect(readFile(join(dir, `${id}.txt`))).rejects.toMatchObject({ code: 'ENOENT' })
    const staged = await prepare()
    expect(dbh.db.transaction((tx) => files.entities[0].apply(tx, change))).toBe('applied')
    await staged.commit([change])
    expect(await readFile(join(dir, `${id}.txt`), 'utf8')).toBe('good')
    expect(dbh.db.select().from(fileEntryTable).where(eq(fileEntryTable.id, id)).get()).toMatchObject({
      id,
      updatedAt: 12,
      createdAt: 10,
      externalPath: null,
      size: 4
    })
    expect(files.entities[0].get(dbh.db, id)?.payload?.blobSha256).toBe(digest('good'))
  })
  it('does not advertise old bytes as an edit if the process stops between SQLite commit and rename', async () => {
    const journal = new Database(':memory:')
    try {
      const id = randomUUID()
      await writeFile(join(dir, id), 'old')
      dbh.db
        .insert(fileEntryTable)
        .values({ id, name: 'shared', origin: 'internal', size: 3, createdAt: 1, updatedAt: 1 })
        .run()
      const remote: RemoteChange = {
        deviceId: 'peer',
        category: 'files',
        entityType: 'file_entry',
        entityId: id,
        updatedAt: 2,
        serverRev: 5,
        payload: {
          id,
          origin: 'internal',
          name: 'shared',
          ext: null,
          cleanupPolicy: 'manual',
          size: 3,
          createdAt: 1,
          updatedAt: 2,
          blobSha256: digest('new')
        }
      }
      const transport = { getBlob: async () => Readable.from([Buffer.from('new')]) }
      const first = new ManagedFiles(journal)
      await first.prepareScan(dbh.db, signal)
      const pending = await first.prepareApply([remote], transport, signal)
      dbh.db.transaction((tx) => first.entities[0].apply(tx, remote))
      await pending.abort()
      const reopened = new ManagedFiles(journal)
      await reopened.prepareScan(dbh.db, signal)
      expect(reopened.entities[0].get(dbh.db, id)).toMatchObject({ withheld: true, payload: null })
      const retry = await reopened.prepareApply([remote], transport, signal)
      dbh.db.transaction((tx) => reopened.entities[0].apply(tx, remote))
      await retry.commit([remote])
      expect(await readFile(join(dir, id), 'utf8')).toBe('new')
      expect(reopened.entities[0].get(dbh.db, id)?.payload?.blobSha256).toBe(digest('new'))
    } finally {
      journal.close()
    }
  })
})
