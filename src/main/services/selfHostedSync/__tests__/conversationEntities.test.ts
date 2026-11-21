import { setupTestDatabase } from '@test-helpers/db'
import { describe, expect, it } from 'vitest'

import { agentSessionTable } from '@data/db/schemas/agentSession'
import { agentSessionMessageTable } from '@data/db/schemas/agentSessionMessage'
import { agentWorkspaceTable } from '@data/db/schemas/agentWorkspace'
import { messageTable } from '@data/db/schemas/message'
import { topicTable } from '@data/db/schemas/topic'

import { conversationEntities } from '../conversationEntities'

const TOPIC_ID = '00000000-0000-4000-8000-000000000011'
const TOPIC_ROOT_ID = '00000000-0000-4000-8000-000000000012'
const WORKSPACE_ID = '00000000-0000-4000-8000-000000000021'
const SESSION_ID = '00000000-0000-4000-8000-000000000022'
const SESSION_ROOT_ID = '00000000-0000-4000-8000-000000000023'

describe('conversation sync parent projections', () => {
  const dbh = setupTestDatabase()
  const topicAdapter = conversationEntities.find((adapter) => adapter.type === 'topic')!
  const sessionAdapter = conversationEntities.find((adapter) => adapter.type === 'agent_session')!

  it('withholds a topic until it has a portable root message', () => {
    dbh.db.insert(topicTable).values({ id: TOPIC_ID, orderKey: 'a0' }).run()

    expect(topicAdapter.get(dbh.db, TOPIC_ID)).toMatchObject({ payload: null, withheld: true })

    dbh.db
      .insert(messageTable)
      .values({
        id: TOPIC_ROOT_ID,
        topicId: TOPIC_ID,
        parentId: null,
        role: 'root',
        data: { parts: [] },
        status: 'success'
      })
      .run()

    expect(topicAdapter.get(dbh.db, TOPIC_ID)?.payload).toMatchObject({ id: TOPIC_ID })
  })

  it('withholds a conversation session until it has a portable root message', () => {
    dbh.db
      .insert(agentWorkspaceTable)
      .values({ id: WORKSPACE_ID, name: 'Workspace', path: '/workspace', orderKey: 'a0' })
      .run()
    dbh.db
      .insert(agentSessionTable)
      .values({ id: SESSION_ID, name: 'Session', workspaceId: WORKSPACE_ID, orderKey: 'a0' })
      .run()

    expect(sessionAdapter.get(dbh.db, SESSION_ID)).toMatchObject({ payload: null, withheld: true })

    dbh.db
      .insert(agentSessionMessageTable)
      .values({
        id: SESSION_ROOT_ID,
        sessionId: SESSION_ID,
        role: 'root',
        parentId: null,
        data: { parts: [] },
        status: 'success'
      })
      .run()

    expect(sessionAdapter.get(dbh.db, SESSION_ID)?.payload).toMatchObject({ id: SESSION_ID })
  })
})
