import { randomUUID } from 'node:crypto'

import { setupTestDatabase } from '@test-helpers/db'
import { describe, expect, it } from 'vitest'

import { fileEntryTable } from '@data/db/schemas/file'
import { paintingFileRefTable } from '@data/db/schemas/fileRelations'
import { paintingTable } from '@data/db/schemas/painting'
import type { SyncCategory } from '@shared/ipc/schemas/selfHostedSync'

import type { SyncChange } from '../entity'
import { createFileRelations } from '../fileRelations'
import { createLibraryEntities } from '../libraryEntities'
import { hasTranslationTarget, plannedTranslationTargets, translationReady } from '../translationFiles'

describe('attachment owner consent', () => {
  const dbh = setupTestDatabase()
  it('only applies links for internal files and selected owners, preserving the original ref ID', () => {
    const sourceId = randomUUID(),
      fileEntryId = randomUUID(),
      externalId = randomUUID(),
      id = randomUUID()
    dbh.db.insert(paintingTable).values({ id: sourceId, providerId: 'test', prompt: 'sample', orderKey: 'a0' }).run()
    dbh.db
      .insert(fileEntryTable)
      .values([
        { id: fileEntryId, name: 'internal', origin: 'internal', size: 3 },
        { id: externalId, name: 'local', origin: 'external', externalPath: '/outside/example' }
      ])
      .run()
    let selected: SyncCategory[] = ['files']
    const adapter = createFileRelations(() => selected).find((a) => a.type === 'painting_file_ref')!
    const change = (file: string) => ({
      entityType: 'painting_file_ref',
      category: 'files' as const,
      entityId: JSON.stringify([file, sourceId, 'output']),
      updatedAt: 20,
      deviceId: 'a',
      payload: { id, fileEntryId: file, sourceId, role: 'output', createdAt: 17, updatedAt: 20 }
    })
    expect(adapter.apply(dbh.db, change(fileEntryId))).toBe('deferred')
    selected = ['files', 'other']
    expect(adapter.apply(dbh.db, change(externalId))).toBe('deferred')
    expect(dbh.db.select().from(paintingFileRefTable).all()).toEqual([])
    expect(adapter.apply(dbh.db, change(fileEntryId))).toBe('applied')
    expect(dbh.db.select().from(paintingFileRefTable).get()).toMatchObject({
      id,
      fileEntryId,
      sourceId,
      updatedAt: 20,
      createdAt: 17
    })
    selected = ['files']
    expect(adapter.scan(dbh.db)[0]).toMatchObject({ withheld: true, payload: null })
  })
  it('keeps a translated document and its internal target together without importing the source path', () => {
    const id = randomUUID(),
      file = randomUUID()
    const receipt: SyncChange = {
      category: 'other',
      entityType: 'translate_history',
      entityId: id,
      updatedAt: 10,
      deviceId: 'peer',
      payload: {
        id,
        kind: 'file',
        sourceText: 'source.pdf',
        targetText: 'translated.pdf',
        sourceLanguage: null,
        targetLanguage: null,
        star: false,
        createdAt: 1
      }
    }
    const link: SyncChange = {
      category: 'files',
      entityType: 'translate_history_file_ref',
      entityId: JSON.stringify([id, 'target']),
      updatedAt: 10,
      deviceId: 'peer',
      payload: { id: randomUUID(), fileEntryId: file, sourceId: id, role: 'target', createdAt: 1, updatedAt: 10 }
    }
    const targets = plannedTranslationTargets([link])
    expect(translationReady(dbh.db, receipt, targets)).toBe(false)
    dbh.db
      .insert(fileEntryTable)
      .values({ id: file, name: 'translated', ext: 'pdf', origin: 'internal', size: 3 })
      .run()
    expect(translationReady(dbh.db, receipt, targets)).toBe(true)
    const history = createLibraryEntities().find((a) => a.type === 'translate_history')!
    const refs = createFileRelations(() => ['other', 'files']).find((a) => a.type === link.entityType)!
    dbh.db.transaction((tx) => {
      expect(history.apply(tx, receipt)).toBe('applied')
      expect(refs.apply(tx, link)).toBe('applied')
      expect(hasTranslationTarget(tx, id)).toBe(true)
    })
    expect(history.get(dbh.db, id)?.payload?.sourceText).toBe('source.pdf')
    expect(() =>
      history.apply(dbh.db, { ...receipt, payload: { ...receipt.payload, sourceText: '/outside/source.pdf' } })
    ).toThrow(/names/)
  })
})
