import { setupTestDatabase } from '@test-helpers/db'
import { eq } from 'drizzle-orm'
import { describe, expect, it } from 'vitest'

import { agentTable } from '@data/db/schemas/agent'
import { assistantTable } from '@data/db/schemas/assistant'
import { groupTable } from '@data/db/schemas/group'
import { promptBindingTable, promptTable } from '@data/db/schemas/prompt'
import { DEFAULT_ASSISTANT_SETTINGS } from '@shared/data/types/assistant'

import { conversationEntities } from '../conversationEntities'
import type { EntityAdapter, SyncChange } from '../entity'
import { createLibraryEntities } from '../libraryEntities'

const NOW = 1_700_000_000_000
const GROUP_ID = '00000000-0000-4000-8000-000000000101'
const AGENT_ID = '00000000-0000-4000-8000-000000000102'
const PROMPT_ID = '00000000-0000-4000-8000-000000000103'
const ASSISTANT_ID = '00000000-0000-4000-8000-000000000104'

function entity(entities: EntityAdapter[], type: string): EntityAdapter {
  const found = entities.find((candidate) => candidate.type === type)
  if (!found) throw new Error(`Missing sync entity ${type}`)
  return found
}

function changeFor(adapter: EntityAdapter, id: string, payload: Record<string, unknown>, updatedAt = NOW): SyncChange {
  return {
    entityType: adapter.type,
    entityId: id,
    category: adapter.category,
    deviceId: 'remote-device',
    updatedAt,
    payload
  }
}

describe('self-hosted sync library entities', () => {
  const dbh = setupTestDatabase()

  it('routes agent rows through the conversation adapter only', () => {
    const combined = [...createLibraryEntities(), ...conversationEntities]

    expect(createLibraryEntities().some((adapter) => adapter.type === 'agent')).toBe(false)
    expect(combined.filter((adapter) => adapter.type === 'agent')).toHaveLength(1)
  })

  it('preserves a group’s original creation time while applying a newer edit', () => {
    const adapters = createLibraryEntities()
    const adapter = entity(adapters, 'topic_group')
    expect(adapter.category).toBe('chats')
    const original = {
      id: GROUP_ID,
      entityType: 'topic',
      name: 'Shared group',
      orderKey: 'a0',
      createdAt: 10
    }
    expect(adapter.apply(dbh.db, changeFor(adapter, GROUP_ID, original))).toBe('applied')

    expect(
      adapter.apply(
        dbh.db,
        changeFor(adapter, GROUP_ID, { ...original, name: 'Renamed group', createdAt: 999 }, NOW + 10)
      )
    ).toBe('applied')

    expect(entity(adapters, 'group').get(dbh.db, GROUP_ID)).toMatchObject({ withheld: true, payload: null })
    const row = dbh.db.select().from(groupTable).where(eq(groupTable.id, GROUP_ID)).get()
    expect(row).toMatchObject({ name: 'Renamed group', createdAt: 10, updatedAt: NOW + 10 })
  })

  it('soft-deletes assistant rows and retains the remote deletion timestamps', () => {
    const adapter = entity(createLibraryEntities(), 'assistant')
    const payload = {
      id: ASSISTANT_ID,
      name: 'Shared assistant',
      prompt: '',
      emoji: '🌟',
      description: '',
      modelId: null,
      groupId: null,
      settings: DEFAULT_ASSISTANT_SETTINGS,
      orderKey: 'a0',
      createdAt: NOW
    }
    expect(adapter.apply(dbh.db, changeFor(adapter, ASSISTANT_ID, payload))).toBe('applied')

    const deletion: SyncChange = {
      entityType: adapter.type,
      entityId: ASSISTANT_ID,
      category: adapter.category,
      deviceId: 'remote-device',
      updatedAt: NOW + 20,
      deleted: true,
      deletedAt: NOW + 10
    }
    expect(adapter.apply(dbh.db, deletion)).toBe('applied')

    const row = dbh.db.select().from(assistantTable).where(eq(assistantTable.id, ASSISTANT_ID)).get()
    expect(row).toMatchObject({ deletedAt: NOW + 10, updatedAt: NOW + 20 })
  })

  it('withholds agent prompt bindings when agent sync is disabled', () => {
    dbh.db
      .insert(agentTable)
      .values({
        id: AGENT_ID,
        type: 'pi',
        name: 'Shared agent',
        description: '',
        instructions: 'Shared instructions',
        model: null,
        planModel: null,
        smallModel: null,
        disabledTools: [],
        configuration: {},
        orderKey: 'a0',
        createdAt: NOW,
        updatedAt: NOW
      })
      .run()
    dbh.db
      .insert(promptTable)
      .values({
        id: PROMPT_ID,
        title: 'Shared prompt',
        content: 'Hello',
        visibility: 'restricted',
        orderKey: 'a0',
        createdAt: NOW,
        updatedAt: NOW
      })
      .run()
    dbh.db
      .insert(promptBindingTable)
      .values({
        promptId: PROMPT_ID,
        targetType: 'agent',
        targetId: AGENT_ID,
        orderKey: 'a0',
        createdAt: NOW,
        updatedAt: NOW
      })
      .run()

    const binding = entity(createLibraryEntities({ includeAgentBindings: () => false }), 'prompt_binding')
    expect(binding.scan(dbh.db)).toMatchObject([
      {
        id: JSON.stringify([PROMPT_ID, 'agent', AGENT_ID]),
        payload: null,
        withheld: true
      }
    ])
  })
})
