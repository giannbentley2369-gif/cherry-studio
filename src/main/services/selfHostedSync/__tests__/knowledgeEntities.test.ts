import { mkdtempSync, mkdirSync, rmSync, symlinkSync, writeFileSync } from 'node:fs'
import os from 'node:os'
import path from 'node:path'

import { setupTestDatabase } from '@test-helpers/db'
import { eq } from 'drizzle-orm'

import { application } from '@application'
import { knowledgeBaseTable, knowledgeItemTable } from '@data/db/schemas/knowledge'
import { noteTable } from '@data/db/schemas/note'
import { userModelTable } from '@data/db/schemas/userModel'
import { userProviderTable } from '@data/db/schemas/userProvider'
import { getKnowledgeVectorStoreFilePathSync } from '@main/features/knowledge/pathStorage'

import type { EntityAdapter, SyncChange } from '../entity'
import { knowledgeEntities } from '../knowledgeEntities'

const BASE_ID = '00000000-0000-4000-8000-000000000001'
const MODEL_ID = 'openai::embed-model'
const NOTE_ITEM_ID = '018f0000-0000-7000-8000-000000000001'
const NOTE_ID = '00000000-0000-4000-8000-000000000002'
const REMOTE_DEVICE = 'remote-device'
const NOW = 1_700_000_000_000

function entity(type: string): EntityAdapter {
  const found = knowledgeEntities.find((candidate) => candidate.type === type)
  if (!found) throw new Error(`Missing sync entity ${type}`)
  return found
}

function changeFor(adapter: EntityAdapter, id: string, payload: Record<string, unknown>): SyncChange {
  return {
    entityType: adapter.type,
    entityId: id,
    category: 'knowledge',
    deviceId: REMOTE_DEVICE,
    updatedAt: NOW,
    payload
  }
}

function basePayload(id = BASE_ID, embeddingModelId: string | null = null) {
  return {
    id,
    name: 'Shared base',
    orderKey: 'a0',
    groupId: null,
    embeddingModelId,
    rerankModelId: null,
    fileProcessorId: null,
    chunkSize: 256,
    chunkOverlap: 20,
    chunkStrategy: 'structured',
    chunkSeparator: '\\n\\n',
    threshold: null,
    createdAt: NOW,
    updatedAt: NOW
  }
}

function insertBase(db: ReturnType<typeof setupTestDatabase>['db'], embeddingModelId: string | null = null): void {
  const row = {
    ...basePayload(BASE_ID, embeddingModelId),
    status: 'completed',
    error: null,
    dimensions: null,
    documentCount: null
  } as typeof knowledgeBaseTable.$inferInsert
  db.insert(knowledgeBaseTable).values(row).run()
}

function insertNoteItem(db: ReturnType<typeof setupTestDatabase>['db'], overrides: Record<string, unknown> = {}): void {
  db.insert(knowledgeItemTable)
    .values({
      id: NOTE_ITEM_ID,
      baseId: BASE_ID,
      groupId: null,
      type: 'note',
      data: { source: 'Meeting notes', content: 'Original note', relativePath: 'snapshot.md' },
      status: 'idle',
      error: null,
      createdAt: NOW,
      updatedAt: NOW,
      ...overrides
    } as typeof knowledgeItemTable.$inferInsert)
    .run()
}

describe('self-hosted sync knowledge and notes entities', () => {
  const dbh = setupTestDatabase()
  let workspace = ''
  let notesRoot = ''
  let knowledgeRoot = ''
  let getPathSpy: { mockRestore: () => void } | undefined

  beforeAll(() => {
    workspace = mkdtempSync(path.join(os.tmpdir(), 'cs-sync-knowledge-'))
    notesRoot = path.join(workspace, 'Notes')
    knowledgeRoot = path.join(workspace, 'KnowledgeBase')
    getPathSpy = vi.spyOn(application, 'getPath').mockImplementation((key, filename) => {
      const root =
        key === 'feature.notes.data' ? notesRoot : key === 'feature.knowledgebase.data' ? knowledgeRoot : workspace
      return filename ? path.join(root, filename) : root
    })
  })

  beforeEach(() => {
    rmSync(notesRoot, { recursive: true, force: true })
    rmSync(knowledgeRoot, { recursive: true, force: true })
    mkdirSync(notesRoot, { recursive: true })
    mkdirSync(knowledgeRoot, { recursive: true })
  })

  afterAll(() => {
    getPathSpy?.mockRestore()
    rmSync(workspace, { recursive: true, force: true })
  })

  it('imports knowledge base settings without claiming a remote vector index exists', () => {
    dbh.db.insert(userProviderTable).values({ providerId: 'openai', name: 'OpenAI', orderKey: 'a0' }).run()
    dbh.db
      .insert(userModelTable)
      .values({
        id: MODEL_ID,
        providerId: 'openai',
        modelId: 'embed-model',
        presetModelId: 'embed-model',
        orderKey: 'a0'
      })
      .run()

    const adapter = entity('knowledge_base')
    const payload = basePayload(BASE_ID, MODEL_ID)
    expect(Object.keys(payload).sort()).not.toContain('dimensions')
    expect(Object.keys(payload).sort()).not.toContain('documentCount')
    expect(Object.keys(payload).sort()).not.toContain('status')

    const result = dbh.db.transaction((tx) => adapter.apply(tx, changeFor(adapter, BASE_ID, payload)))
    const row = dbh.db.select().from(knowledgeBaseTable).where(eq(knowledgeBaseTable.id, BASE_ID)).get()

    expect(result).toBe('applied')
    expect(row).toMatchObject({
      status: 'failed',
      error: 'missing_vector_store',
      dimensions: null,
      documentCount: null
    })
  })

  it('preserves indexed knowledge settings and defers remote reindexing changes', () => {
    dbh.db.insert(userProviderTable).values({ providerId: 'openai', name: 'OpenAI', orderKey: 'a0' }).run()
    dbh.db
      .insert(userModelTable)
      .values({
        id: MODEL_ID,
        providerId: 'openai',
        modelId: 'embed-model',
        presetModelId: 'embed-model',
        orderKey: 'a0'
      })
      .run()
    insertBase(dbh.db)
    dbh.db
      .update(knowledgeBaseTable)
      .set({ embeddingModelId: MODEL_ID, dimensions: 768, documentCount: 9 })
      .where(eq(knowledgeBaseTable.id, BASE_ID))
      .run()
    const vectorPath = getKnowledgeVectorStoreFilePathSync(BASE_ID)
    mkdirSync(path.dirname(vectorPath), { recursive: true })
    writeFileSync(vectorPath, 'local vector index')

    const adapter = entity('knowledge_base')
    const metadata = basePayload(BASE_ID, MODEL_ID)
    const rename = changeFor(adapter, BASE_ID, { ...metadata, name: 'Renamed base' })
    expect(dbh.db.transaction((tx) => adapter.apply(tx, rename))).toBe('applied')
    expect(dbh.db.select().from(knowledgeBaseTable).where(eq(knowledgeBaseTable.id, BASE_ID)).get()).toMatchObject({
      name: 'Renamed base',
      dimensions: 768,
      documentCount: 9
    })

    const incompatible = changeFor(adapter, BASE_ID, { ...metadata, chunkSize: 512 })
    expect(dbh.db.transaction((tx) => adapter.apply(tx, incompatible))).toBe('deferred')
    expect(dbh.db.select().from(knowledgeBaseTable).where(eq(knowledgeBaseTable.id, BASE_ID)).get()?.chunkSize).toBe(
      256
    )
  })

  it('syncs only portable note/url items and resets their local indexing lifecycle', () => {
    insertBase(dbh.db)
    insertNoteItem(dbh.db, { status: 'failed', error: 'indexing_interrupted' })

    const adapter = entity('knowledge_item')
    const local = adapter.get(dbh.db, NOTE_ITEM_ID)
    expect(local?.payload).toMatchObject({
      type: 'note',
      data: { source: 'Meeting notes', content: 'Original note' }
    })
    const localData = local?.payload?.data as Record<string, unknown> | undefined
    expect(localData?.relativePath).toBeUndefined()

    const changed = changeFor(adapter, NOTE_ITEM_ID, {
      id: NOTE_ITEM_ID,
      baseId: BASE_ID,
      groupId: null,
      type: 'note',
      data: { source: 'Meeting notes', content: 'Restored note' },
      createdAt: NOW,
      updatedAt: NOW + 1
    })
    const result = dbh.db.transaction((tx) => adapter.apply(tx, changed))
    const row = dbh.db.select().from(knowledgeItemTable).where(eq(knowledgeItemTable.id, NOTE_ITEM_ID)).get()

    expect(result).toBe('applied')
    expect(row).toMatchObject({
      status: 'idle',
      error: null,
      data: { source: 'Meeting notes', content: 'Restored note' }
    })
    const rowData = row?.data as Record<string, unknown> | undefined
    expect(rowData?.relativePath).toBeUndefined()
  })

  it('withholds external paths and URL credentials while keeping safe URLs portable', () => {
    insertBase(dbh.db)
    const fileId = '018f0000-0000-7000-8000-000000000002'
    const urlId = '018f0000-0000-7000-8000-000000000003'
    const noteId = '018f0000-0000-7000-8000-000000000004'
    const safeUrlId = '018f0000-0000-7000-8000-000000000005'
    const items = [
      {
        id: fileId,
        baseId: BASE_ID,
        groupId: null,
        type: 'file',
        data: { source: '/Users/alice/private/report.pdf', relativePath: 'report.pdf' },
        status: 'idle',
        error: null,
        createdAt: NOW,
        updatedAt: NOW
      },
      {
        id: urlId,
        baseId: BASE_ID,
        groupId: null,
        type: 'url',
        data: { source: 'https://example.com/?token=private', url: 'https://example.com/?token=private' },
        status: 'idle',
        error: null,
        createdAt: NOW,
        updatedAt: NOW
      },
      {
        id: noteId,
        baseId: BASE_ID,
        groupId: null,
        type: 'note',
        data: { source: 'C:\\Users\\alice\\notes.md', content: 'Local path label' },
        status: 'idle',
        error: null,
        createdAt: NOW,
        updatedAt: NOW
      },
      {
        id: safeUrlId,
        baseId: BASE_ID,
        groupId: null,
        type: 'url',
        data: { source: 'Reference page', url: 'https://example.com/article' },
        status: 'idle',
        error: null,
        createdAt: NOW,
        updatedAt: NOW
      }
    ] as (typeof knowledgeItemTable.$inferInsert)[]
    dbh.db.insert(knowledgeItemTable).values(items).run()

    const adapter = entity('knowledge_item')
    expect(adapter.get(dbh.db, fileId)).toMatchObject({ payload: null, withheld: true })
    expect(adapter.get(dbh.db, urlId)).toMatchObject({ payload: null, withheld: true })
    expect(adapter.get(dbh.db, noteId)).toMatchObject({ payload: null, withheld: true })
    expect(adapter.get(dbh.db, safeUrlId)?.payload).toMatchObject({
      type: 'url',
      data: { source: 'Reference page', url: 'https://example.com/article' }
    })
  })

  it('maps only default-root note metadata to relative paths and defers path collisions', () => {
    const externalRoot = path.join(workspace, 'ExternalNotes')
    dbh.db
      .insert(noteTable)
      .values({
        id: NOTE_ID,
        rootPath: externalRoot,
        path: path.join(externalRoot, 'secret.md'),
        isStarred: true,
        isExpanded: false,
        createdAt: NOW,
        updatedAt: NOW
      })
      .run()
    const adapter = entity('note')
    expect(adapter.get(dbh.db, NOTE_ID)).toMatchObject({ payload: null, withheld: true })

    const remotePath = 'project/idea.md'
    const remoteId = '00000000-0000-4000-8000-000000000003'
    const payload = {
      id: remoteId,
      path: remotePath,
      isStarred: true,
      isExpanded: false,
      createdAt: NOW,
      updatedAt: NOW
    }
    const result = dbh.db.transaction((tx) => adapter.apply(tx, changeFor(adapter, remoteId, payload)))
    const imported = dbh.db.select().from(noteTable).where(eq(noteTable.id, remoteId)).get()
    expect(result).toBe('applied')
    expect(imported).toMatchObject({ rootPath: notesRoot, path: path.join(notesRoot, 'project', 'idea.md') })

    const collision = {
      ...payload,
      id: '00000000-0000-4000-8000-000000000004',
      path: remotePath
    }
    expect(dbh.db.transaction((tx) => adapter.apply(tx, changeFor(adapter, collision.id, collision)))).toBe('deferred')
    expect(dbh.db.select().from(noteTable).where(eq(noteTable.id, collision.id)).get()).toBeUndefined()
  })

  it('withholds note metadata whose path traverses a symlink', () => {
    const externalRoot = path.join(workspace, 'outside-notes')
    mkdirSync(externalRoot)
    symlinkSync(externalRoot, path.join(notesRoot, 'linked'))
    dbh.db
      .insert(noteTable)
      .values({
        id: NOTE_ID,
        rootPath: notesRoot,
        path: path.join(notesRoot, 'linked', 'secret.md'),
        isStarred: true,
        isExpanded: false,
        createdAt: NOW,
        updatedAt: NOW
      })
      .run()

    expect(entity('note').get(dbh.db, NOTE_ID)).toMatchObject({ payload: null, withheld: true })
    expect(
      dbh.db.transaction((tx) =>
        entity('note').apply(
          tx,
          changeFor(entity('note'), '00000000-0000-4000-8000-000000000003', {
            id: '00000000-0000-4000-8000-000000000003',
            path: 'linked/remote.md',
            isStarred: true,
            isExpanded: false,
            createdAt: NOW,
            updatedAt: NOW
          })
        )
      )
    ).toBe('deferred')
  })

  it('keeps Markdown note contents outside the portable record set', () => {
    expect(knowledgeEntities.map((adapter) => adapter.type)).not.toContain('note_content')
  })

  it('defers knowledge content changes while the local vector index exists', () => {
    insertBase(dbh.db)
    insertNoteItem(dbh.db)
    const vectorPath = getKnowledgeVectorStoreFilePathSync(BASE_ID)
    mkdirSync(path.dirname(vectorPath), { recursive: true })
    writeFileSync(vectorPath, 'local vector index')

    const adapter = entity('knowledge_item')
    const metadataChange = changeFor(adapter, NOTE_ITEM_ID, {
      id: NOTE_ITEM_ID,
      baseId: BASE_ID,
      groupId: null,
      type: 'note',
      data: { source: 'Shared note', content: 'Original note' },
      createdAt: NOW,
      updatedAt: NOW + 1
    })
    const change = changeFor(adapter, NOTE_ITEM_ID, {
      id: NOTE_ITEM_ID,
      baseId: BASE_ID,
      groupId: null,
      type: 'note',
      data: { source: 'Meeting notes', content: 'Remote changed note' },
      createdAt: NOW,
      updatedAt: NOW + 1
    })

    expect(dbh.db.transaction((tx) => adapter.apply(tx, metadataChange))).toBe('applied')
    expect(dbh.db.transaction((tx) => adapter.apply(tx, change))).toBe('deferred')
    expect(dbh.db.select().from(knowledgeItemTable).where(eq(knowledgeItemTable.id, NOTE_ITEM_ID)).get()).toMatchObject(
      {
        status: 'idle',
        data: { source: 'Shared note', content: 'Original note', relativePath: 'snapshot.md' }
      }
    )
  })

  it('defers knowledge item deletion while indexing is active', () => {
    insertBase(dbh.db)
    insertNoteItem(dbh.db, { status: 'processing' })
    const adapter = entity('knowledge_item')
    const change: SyncChange = {
      entityType: adapter.type,
      entityId: NOTE_ITEM_ID,
      category: 'knowledge',
      deviceId: REMOTE_DEVICE,
      updatedAt: NOW + 1,
      deleted: true,
      deletedAt: NOW + 1
    }

    expect(dbh.db.transaction((tx) => adapter.apply(tx, change))).toBe('deferred')
    expect(dbh.db.select().from(knowledgeItemTable).where(eq(knowledgeItemTable.id, NOTE_ITEM_ID)).get()).toBeDefined()
  })
})
