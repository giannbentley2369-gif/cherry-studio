import { createHash } from 'node:crypto'
import { lstat, realpath } from 'node:fs/promises'
import { dirname, join, relative } from 'node:path'
import { Transform } from 'node:stream'
import { pipeline } from 'node:stream/promises'

import type Database from 'better-sqlite3'
import { eq } from 'drizzle-orm'
import * as z from 'zod'

import { application } from '@application'
import { fileEntryTable } from '@data/db/schemas/file'
import type { DbOrTx } from '@data/db/types'
import { fileEntryService } from '@data/services/FileEntryService'
import type { FileManager } from '@main/services/file'
import type { PreparedAtomicWrite } from '@main/utils/file'
import { CleanupPolicySchema, FileEntryIdSchema, SafeNameSchema } from '@shared/data/types/file'
import { SafeExtSchema } from '@shared/types/file'

import type { EntityAdapter, LocalEntity, RemoteChange, SyncChange } from './entity'
import type { FileTransferHooks } from './fileTransfer'
import type { SyncTransport } from './transport'

type Row = typeof fileEntryTable.$inferSelect
type Access = ReturnType<FileManager['syncInternalFile']>
const maxBytes = 1024 * 1024 * 1024
const payloadSchema = z.strictObject({
  id: FileEntryIdSchema,
  origin: z.literal('internal'),
  name: SafeNameSchema,
  ext: SafeExtSchema.nullable(),
  size: z.int().min(0).max(maxBytes),
  cleanupPolicy: CleanupPolicySchema,
  createdAt: z.int().nonnegative(),
  updatedAt: z.int().nonnegative(),
  blobSha256: z.string().regex(/^[a-f0-9]{64}$/)
})

class UnsafeFile extends Error {}

async function inspectTarget(access: Access) {
  const profile = await realpath(application.getPath('app.userdata'))
  if (relative(join(profile, 'Data', 'Files'), await realpath(dirname(access.target))) !== '')
    throw new Error('Unsafe managed files directory.')
  const stat = await lstat(access.target, { bigint: true }).catch((error: NodeJS.ErrnoException) => {
    if (error.code === 'ENOENT') return null
    throw error
  })
  if (stat && (!stat.isFile() || stat.nlink !== 1n)) throw new UnsafeFile('Unsafe managed file.')
  return stat
}

async function snapshot(access: Access) {
  const before = await inspectTarget(access)
  if (!before) return null
  const file = await access.snapshot()
  try {
    const after = await inspectTarget(access)
    if (
      !after ||
      before.ino !== file.ino ||
      after.ino !== file.ino ||
      before.dev !== file.dev ||
      after.dev !== file.dev ||
      before.ctimeNs !== after.ctimeNs
    )
      throw new Error('Managed file changed.')
    return { file, stamp: [after.dev, after.ino, after.ctimeNs, after.mtimeNs, after.size].join(':') }
  } catch (error) {
    await file.close()
    throw error
  }
}

interface Staged {
  access: Access
  file?: PreparedAtomicWrite
  release: () => void
  revision: number
  installedSha?: string
  previousStamp?: string
}

export class ManagedFiles implements FileTransferHooks {
  private readonly digests = new Map<string, { stamp: string; sha: string; size: number; ext: string | null }>()
  private readonly staging = new Map<string, Staged>()

  constructor(private readonly state?: Database.Database) {
    state?.exec(`CREATE TABLE IF NOT EXISTS sync_file_install (
      id TEXT PRIMARY KEY, updated_at INTEGER NOT NULL, old_stamp TEXT NOT NULL, new_hash TEXT NOT NULL
    )`)
  }

  reset(): void {
    this.state?.exec('DELETE FROM sync_file_install')
    this.digests.clear()
  }

  private incomplete(row: Row, stamp?: string): boolean {
    const intent = this.state
      ?.prepare<[string], { updated_at: number; old_stamp: string; new_hash: string }>(
        'SELECT updated_at,old_stamp,new_hash FROM sync_file_install WHERE id=?'
      )
      .get(row.id)
    return (
      !!intent &&
      intent.updated_at === row.updatedAt &&
      intent.old_stamp === (stamp ?? '') &&
      this.digests.get(row.id)?.sha !== intent.new_hash
    )
  }

  private access(id: string, ext: string | null): Access {
    return application
      .get('FileManager')
      .syncInternalFile(FileEntryIdSchema.parse(id), SafeExtSchema.nullable().parse(ext))
  }

  async prepareScan(db: DbOrTx, signal: AbortSignal): Promise<void> {
    const rows = db.select().from(fileEntryTable).where(eq(fileEntryTable.origin, 'internal')).all()
    const alive = new Set<string>()
    for (const row of rows) {
      signal.throwIfAborted()
      if (
        row.deletedAt !== null ||
        !FileEntryIdSchema.safeParse(row.id).success ||
        !SafeExtSchema.nullable().safeParse(row.ext).success
      )
        continue
      const access = this.access(row.id, row.ext)
      const opened = await snapshot(access).catch((error: unknown) => {
        if (error instanceof UnsafeFile) return null
        throw error
      })
      if (!opened) continue
      try {
        if (opened.file.size !== row.size || row.size > maxBytes) continue
        let sha = this.digests.get(row.id)?.stamp === opened.stamp ? this.digests.get(row.id)?.sha : undefined
        if (!sha) {
          const hash = createHash('sha256')
          await pipeline(opened.file.createReadStream(), hash, { signal })
          sha = hash.digest('hex')
        }
        this.digests.set(row.id, { sha, stamp: opened.stamp, size: row.size, ext: row.ext })
        alive.add(row.id)
      } finally {
        await opened.file.close()
      }
    }
    for (const key of this.digests.keys()) if (!alive.has(key)) this.digests.delete(key)
  }

  private describe(row: Row): LocalEntity {
    const digest = this.digests.get(row.id)
    if (row.origin !== 'internal') return { id: row.id, payload: null, withheld: true }
    if (row.deletedAt !== null) return { id: row.id, payload: null, deletedAt: row.deletedAt, updatedAt: row.updatedAt }
    if (!digest || digest.ext !== row.ext || digest.size !== row.size || this.incomplete(row, digest.stamp))
      return { id: row.id, payload: null, withheld: true }
    const { id, origin, name, ext, size, cleanupPolicy, createdAt, updatedAt } = row
    return {
      id,
      updatedAt,
      payload: payloadSchema.parse({
        id,
        origin,
        name,
        ext,
        size,
        cleanupPolicy,
        createdAt,
        updatedAt,
        blobSha256: digest.sha
      })
    }
  }

  readonly entities: EntityAdapter[] = [
    {
      type: 'file_entry',
      category: 'files',
      scan: (db) =>
        db
          .select()
          .from(fileEntryTable)
          .where(eq(fileEntryTable.origin, 'internal'))
          .all()
          .map((r) => this.describe(r)),
      get: (db, id) => {
        const row = db.select().from(fileEntryTable).where(eq(fileEntryTable.id, id)).get()
        return row && this.describe(row)
      },
      apply: (tx, change) => {
        if (change.entityType !== 'file_entry' || change.category !== 'files')
          throw new Error('Unexpected file record.')
        const id = FileEntryIdSchema.parse(change.entityId)
        const current = fileEntryService.findByIdTx(tx, id)
        if (current && current.origin !== 'internal') return 'deferred'
        if (change.deleted) {
          if (current && !this.staging.has(id)) return 'deferred'
          fileEntryService.importSyncedInternalTx(tx, id, null, change.deletedAt)
        } else {
          const { blobSha256, ...row } = payloadSchema.parse(change.payload)
          const staged = this.staging.get(id)
          if (row.id !== id || !staged || staged.installedSha !== blobSha256) return 'deferred'
          // Journal the pre-rename disk identity before publishing its new SQLite metadata.
          this.state
            ?.prepare('INSERT OR REPLACE INTO sync_file_install (id,updated_at,old_stamp,new_hash) VALUES (?,?,?,?)')
            .run(id, change.updatedAt, staged.previousStamp ?? '', blobSha256)
          fileEntryService.importSyncedInternalTx(
            tx,
            id,
            { ...row, contentHash: staged.file?.contentHash ?? current?.contentHash ?? null },
            change.updatedAt
          )
        }
        return 'applied'
      },
      afterCommit: (changes) => {
        for (const c of changes) if (c.deleted) this.digests.delete(c.entityId)
      }
    }
  ]

  async upload(
    change: SyncChange,
    transport: Pick<SyncTransport, 'hasBlob' | 'putBlob'>,
    signal: AbortSignal
  ): Promise<void> {
    if (change.entityType !== 'file_entry' || change.deleted) return
    const row = payloadSchema.parse(change.payload)
    if (row.id !== change.entityId) throw new Error('Mismatched file identity.')
    if (await transport.hasBlob(row.blobSha256)) return
    const opened = await snapshot(this.access(row.id, row.ext))
    if (!opened) throw new Error('Managed file is unavailable.')
    try {
      signal.throwIfAborted()
      if (opened.stamp !== this.digests.get(row.id)?.stamp || opened.file.size !== row.size)
        throw new Error('Managed file changed before upload.')
      await transport.putBlob(row.blobSha256, opened.file.createReadStream(), row.size)
    } finally {
      await opened.file.close()
    }
  }

  async prepareApply(changes: RemoteChange[], transport: Pick<SyncTransport, 'getBlob'>, signal: AbortSignal) {
    const plans = this.staging
    const abort = async () => {
      try {
        await Promise.all([...plans.values()].flatMap((p) => (p.file ? [p.file.abort()] : [])))
      } finally {
        for (const p of plans.values()) p.release()
        plans.clear()
      }
    }
    try {
      for (const change of changes) {
        signal.throwIfAborted()
        if (change.entityType !== 'file_entry' || change.category !== 'files') continue
        const id = FileEntryIdSchema.parse(change.entityId)
        if (plans.has(id)) throw new Error('Duplicate file in sync batch.')
        const existing = fileEntryService.findById(id)
        if (existing && existing.origin !== 'internal') continue
        const row = change.deleted ? null : payloadSchema.parse(change.payload)
        if (row && row.id !== id) throw new Error('Mismatched file identity.')
        if (!row && !existing) continue
        const access = this.access(id, row ? row.ext : existing!.ext)
        const release = await access.lock()
        const plan: Staged = { access, release, revision: change.serverRev, installedSha: row?.blobSha256 }
        plans.set(id, plan)
        const scanned = this.digests.get(id)
        if (existing && scanned) {
          const old = await snapshot(this.access(id, existing.ext))
          await old?.file.close()
          if (old?.stamp !== scanned.stamp) throw new Error('Local file changed during sync.')
        }
        if (!row) continue
        const current = await snapshot(access)
        if (current) {
          await current.file.close()
          plan.previousStamp = current.stamp
          if (!existing || existing.ext !== row.ext)
            throw new Error('An unrelated managed file already occupies this location.')
          if (scanned?.stamp === current.stamp && scanned.sha === row.blobSha256) continue
        }
        const hash = createHash('sha256')
        let bytes = 0
        const verify = new Transform({
          transform(chunk: Buffer, _encoding, done) {
            bytes += chunk.length
            if (bytes > row.size) return done(new Error('Sync blob exceeds declared size.'))
            hash.update(chunk)
            done(null, chunk)
          }
        })
        const stream = await transport.getBlob(row.blobSha256)
        await inspectTarget(access).catch((error: unknown) => {
          stream.destroy()
          throw error
        })
        await pipeline(
          stream,
          verify,
          access.stage(async (file) => {
            plan.file = file
          }),
          { signal }
        )
        if (!plan.file || bytes !== row.size || hash.digest('hex') !== row.blobSha256)
          throw new Error('Sync blob failed integrity verification.')
      }
      return {
        abort,
        commit: async (applied: RemoteChange[]) => {
          try {
            for (const change of applied) {
              const plan = change.entityType === 'file_entry' ? plans.get(change.entityId) : undefined
              if (!plan || plan.revision !== change.serverRev) continue
              if (change.deleted) {
                this.digests.delete(change.entityId)
                this.state?.prepare('DELETE FROM sync_file_install WHERE id=?').run(change.entityId)
                plan.access.changed()
                continue
              }
              await inspectTarget(plan.access)
              await plan.file?.commit()
              const installed = await snapshot(plan.access)
              if (!installed) throw new Error('Sync file was not installed.')
              await installed.file.close()
              const { ext, size, blobSha256 } = payloadSchema.parse(change.payload)
              this.digests.set(change.entityId, { sha: blobSha256, stamp: installed.stamp, size, ext })
              this.state?.prepare('DELETE FROM sync_file_install WHERE id=?').run(change.entityId)
              plan.access.changed()
            }
          } finally {
            await abort()
          }
        }
      }
    } catch (error) {
      await abort()
      throw error
    }
  }
}
