import { setupTestDatabase } from '@test-helpers/db'
import { MockApplicationUtils } from '@test-mocks/main/application'
import { eq } from 'drizzle-orm'
import { afterEach, describe, expect, it } from 'vitest'

import { topicTable } from '@data/db/schemas/topic'

import { assertHistoryRoots, guardLiveHistory } from '../activeHistory'
import type { SyncChange } from '../entity'

const change: SyncChange = {
  entityType: 'topic',
  entityId: 'topic-a',
  category: 'chats',
  deviceId: 'other-device',
  payload: { name: 'remote' },
  updatedAt: 10
}

describe('live history safety', () => {
  const dbh = setupTestDatabase()
  afterEach(() => MockApplicationUtils.resetServices())

  it('rolls back a new live topic if its root has not arrived yet', () => {
    expect(() =>
      dbh.db.transaction((tx) => {
        tx.insert(topicTable).values({ id: 'topic-a', orderKey: 'a0' }).run()
        assertHistoryRoots(tx, [change])
      })
    ).toThrow(/conversation root/)
    expect(dbh.db.select().from(topicTable).all()).toEqual([])
  })

  it('defers a topic while its terminal stream write is unsettled and can retry once idle', () => {
    let busy = true
    MockApplicationUtils.setService('AiStreamManager', {
      hasUnsettledTopicWork: (id: string) => busy && id === 'topic-a'
    })
    dbh.db.insert(topicTable).values({ id: 'topic-a', orderKey: 'a0', name: 'local' }).run()
    const guarded = guardLiveHistory({
      type: 'topic',
      category: 'chats',
      scan: () => [],
      get: () => undefined,
      apply(tx, remote) {
        tx.update(topicTable)
          .set({ name: String(remote.payload?.name) })
          .where(eq(topicTable.id, remote.entityId))
          .run()
        return 'applied'
      }
    })
    expect(guarded.apply(dbh.db, change)).toBe('deferred')
    expect(dbh.db.select().from(topicTable).get()?.name).toBe('local')
    busy = false
    expect(guarded.apply(dbh.db, change)).toBe('applied')
    expect(dbh.db.select().from(topicTable).get()?.name).toBe('remote')
  })
})
