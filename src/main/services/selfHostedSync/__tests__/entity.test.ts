import { setupTestDatabase } from '@test-helpers/db'
import { eq } from 'drizzle-orm'
import { describe, expect, it } from 'vitest'

import { groupTable } from '@data/db/schemas/group'
import { knowledgeBaseTable } from '@data/db/schemas/knowledge'

import { defineTable, type SyncChange } from '../entity'

const groups = defineTable({
  table: groupTable,
  category: 'other',
  keys: ['id'],
  fields: ['id', 'name', 'entityType', 'orderKey', 'createdAt', 'updatedAt'],
  write(tx, key, row) {
    if (row) tx.insert(groupTable).values(row).onConflictDoUpdate({ target: groupTable.id, set: row }).run()
    else tx.delete(groupTable).where(eq(groupTable.id, key.id)).run()
  }
})

const payload = { id: 'g', name: 'Reading', entityType: 'knowledge', orderKey: 'a0', createdAt: 10, updatedAt: 20 }
const change: SyncChange = {
  entityType: 'group',
  entityId: 'g',
  category: 'other',
  updatedAt: 20,
  deviceId: 'desktop-a',
  payload
}

describe('portable table boundary', () => {
  const dbh = setupTestDatabase()

  it('rejects unknown fields and mismatched identity before a live write', () => {
    expect(() => groups.apply(dbh.db, { ...change, entityId: 'another' })).toThrow(/identity/)
    expect(() => groups.apply(dbh.db, { ...change, payload: { ...payload, externalPath: '/outside' } })).toThrow(
      /fields/
    )
    expect(() => groups.apply(dbh.db, { ...change, payload: { ...payload, name: 123 } })).toThrow(/fields/)
    expect(dbh.db.select().from(groupTable).all()).toEqual([])
  })

  it('does not let a remote delete silently change a disabled category through ON DELETE SET NULL', () => {
    expect(groups.apply(dbh.db, change)).toBe('applied')
    dbh.db
      .insert(knowledgeBaseTable)
      .values({
        id: 'kb',
        name: 'Local knowledge',
        orderKey: 'a0',
        groupId: 'g',
        status: 'completed',
        chunkSize: 300,
        chunkOverlap: 20
      })
      .run()
    const deletion: SyncChange = {
      entityType: 'group',
      entityId: 'g',
      category: 'other',
      deviceId: 'desktop-b',
      updatedAt: 50,
      deleted: true,
      deletedAt: 50
    }
    expect(groups.apply(dbh.db, deletion)).toBe('deferred')
    expect(dbh.db.select().from(knowledgeBaseTable).get()?.groupId).toBe('g')
    dbh.db.delete(knowledgeBaseTable).where(eq(knowledgeBaseTable.id, 'kb')).run()
    expect(groups.apply(dbh.db, deletion)).toBe('applied')
    expect(groups.get(dbh.db, 'g')).toBeUndefined()
  })
})
