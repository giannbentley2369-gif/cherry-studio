import { setupTestDatabase } from '@test-helpers/db'
import { eq } from 'drizzle-orm'

import { agentTable } from '@data/db/schemas/agent'
import { agentGlobalSkillTable } from '@data/db/schemas/agentGlobalSkill'
import { agentSessionTable } from '@data/db/schemas/agentSession'
import { agentSessionMessageTable } from '@data/db/schemas/agentSessionMessage'
import { agentSkillTable } from '@data/db/schemas/agentSkill'
import { agentWorkspaceTable } from '@data/db/schemas/agentWorkspace'
import { fileEntryTable } from '@data/db/schemas/file'
import { messageTable } from '@data/db/schemas/message'
import { topicTable } from '@data/db/schemas/topic'
import { messageService } from '@data/services/MessageService'
import { BUILTIN_AGENT_ROLE } from '@shared/ai/builtinAgent'
import type { MessageData } from '@shared/data/types/message'

import { conversationEntities } from '../conversationEntities'
import type { EntityAdapter, SyncChange } from '../entity'

const NOW = 1_700_000_000_000
const TOPIC_ID = '00000000-0000-4000-8000-000000000031'
const MESSAGE_ID = '00000000-0000-4000-8000-000000000033'
const FILE_ID = '00000000-0000-4000-8000-000000000034'
const EXTERNAL_FILE_ID = '00000000-0000-4000-8000-000000000035'
const MISSING_FILE_ID = '00000000-0000-4000-8000-000000000036'
const AGENT_ID = '00000000-0000-4000-8000-000000000037'
const BUILTIN_ID = '00000000-0000-4000-8000-000000000038'
const SESSION_ID = '00000000-0000-4000-8000-000000000039'
const SESSION_ROOT_ID = '00000000-0000-4000-8000-000000000040'
const SKILL_ID = '00000000-0000-4000-8000-000000000041'
const SESSION_MESSAGE_ID = '00000000-0000-4000-8000-000000000042'
const SESSION_PENDING_MESSAGE_ID = '00000000-0000-4000-8000-000000000043'
const SKILL_CONTENT_HASH = 'a'.repeat(64)

function adapterFor(type: string): EntityAdapter {
  const adapter = conversationEntities.find((candidate) => candidate.type === type)
  if (!adapter) throw new Error(`Missing sync entity ${type}`)
  return adapter
}

function syncChange(adapter: EntityAdapter, id: string, payload: Record<string, unknown>): SyncChange {
  return {
    entityType: adapter.type,
    entityId: id,
    category: adapter.category,
    deviceId: 'remote-device',
    updatedAt: NOW,
    payload
  }
}

function setupChat(db: ReturnType<typeof setupTestDatabase>['db']): string {
  db.insert(topicTable).values({ id: TOPIC_ID, orderKey: 'portable-topic' }).run()
  return messageService.createRootMessageTx(db, TOPIC_ID)
}

function insertFile(db: ReturnType<typeof setupTestDatabase>['db']): void {
  db.insert(fileEntryTable)
    .values({
      id: FILE_ID,
      origin: 'internal',
      name: 'secret',
      ext: 'txt',
      size: 10,
      contentHash: null,
      externalPath: null,
      cleanupPolicy: 'manual',
      createdAt: NOW,
      updatedAt: NOW,
      deletedAt: null
    })
    .run()
  db.insert(fileEntryTable)
    .values({
      id: EXTERNAL_FILE_ID,
      origin: 'external',
      name: 'outside',
      ext: 'txt',
      size: null,
      contentHash: null,
      externalPath: '/home/private/outside.txt',
      cleanupPolicy: 'manual',
      createdAt: NOW,
      updatedAt: NOW,
      deletedAt: null
    })
    .run()
}

function insertAgent(
  db: ReturnType<typeof setupTestDatabase>['db'],
  id: string,
  configuration: Record<string, unknown> = {},
  disabledTools: string[] = []
): void {
  db.insert(agentTable)
    .values({
      id,
      type: 'pi',
      name: 'Agent',
      description: '',
      instructions: 'Be useful.',
      model: null,
      planModel: null,
      smallModel: null,
      disabledTools,
      configuration,
      orderKey: id,
      createdAt: NOW,
      updatedAt: NOW
    })
    .run()
}

describe('portable conversation sync data', () => {
  const dbh = setupTestDatabase()

  it('exports internal attachments without local URLs or external file references', () => {
    const rootId = setupChat(dbh.db)
    insertFile(dbh.db)
    const data = {
      parts: [
        { type: 'text', text: 'Visible text', providerMetadata: { cherry: { originalPath: '/Users/private' } } },
        {
          type: 'file',
          url: 'file:///Users/private/secret.txt',
          mediaType: 'text/plain',
          filename: '/Users/private/secret.txt',
          providerMetadata: {
            cherry: {
              fileEntryId: FILE_ID,
              originalPath: '/Users/private/secret.txt',
              remoteAttachment: { sha256: 'b'.repeat(64), byteLength: 10 }
            }
          }
        },
        {
          type: 'file',
          url: 'file:///home/private/outside.txt',
          mediaType: 'text/plain',
          filename: '/home/private/outside.txt',
          providerMetadata: { cherry: { fileEntryId: EXTERNAL_FILE_ID } }
        },
        { type: 'data-agent-task-event', data: { taskId: 'runtime-task' } }
      ],
      modelSelection: 'explicit'
    } as unknown as MessageData
    dbh.db
      .insert(messageTable)
      .values({
        id: MESSAGE_ID,
        topicId: TOPIC_ID,
        parentId: rootId,
        role: 'user',
        data,
        status: 'success',
        siblingsGroupId: 0,
        modelId: null,
        messageSnapshot: null,
        stats: { inputTokens: 12, runtimeTiming: { startedAt: NOW, spans: [] }, timeFirstTokenMs: 9 },
        createdAt: NOW,
        updatedAt: NOW
      })
      .run()

    const payload = adapterFor('message').get(dbh.db, MESSAGE_ID)?.payload
    const serialized = JSON.stringify(payload)

    expect(serialized).not.toContain('/Users/private')
    expect(serialized).not.toContain('/home/private')
    expect(serialized).not.toContain(EXTERNAL_FILE_ID)
    expect(serialized).not.toContain('runtime-task')
    expect(serialized).not.toContain('runtimeTiming')
    expect(serialized).not.toContain('timeFirstTokenMs')
    expect(payload).toMatchObject({
      data: {
        parts: [
          { type: 'text', text: 'Visible text' },
          {
            type: 'file',
            filename: 'secret.txt',
            providerMetadata: { cherry: { fileEntryId: FILE_ID, remoteAttachment: { byteLength: 10 } } }
          }
        ],
        modelSelection: 'explicit'
      },
      stats: { inputTokens: 12 }
    })

    const adapter = adapterFor('message')
    const result = dbh.db.transaction((tx) => adapter.apply(tx, syncChange(adapter, MESSAGE_ID, payload!)))
    const materialized = dbh.db.select().from(messageTable).where(eq(messageTable.id, MESSAGE_ID)).get()

    expect(result).toBe('applied')
    expect(materialized?.data.parts?.[1]).toMatchObject({ url: `file:///mock/files/${FILE_ID}.webp` })
  })

  it('defers attachment import until its internal file entry is available', () => {
    const rootId = setupChat(dbh.db)
    const adapter = adapterFor('message')
    const payload = {
      id: MESSAGE_ID,
      topicId: TOPIC_ID,
      parentId: rootId,
      role: 'user',
      data: {
        parts: [
          {
            type: 'file',
            filename: 'secret.txt',
            providerMetadata: { cherry: { fileEntryId: MISSING_FILE_ID } }
          }
        ]
      },
      status: 'success',
      siblingsGroupId: 0,
      modelId: null,
      messageSnapshot: null,
      stats: null,
      createdAt: NOW,
      updatedAt: NOW
    }

    const result = dbh.db.transaction((tx) => adapter.apply(tx, syncChange(adapter, MESSAGE_ID, payload)))
    const local = dbh.db.select().from(messageTable).where(eq(messageTable.id, MESSAGE_ID)).get()

    expect(result).toBe('deferred')
    expect(local).toBeUndefined()
  })

  it('keeps local agent settings and creates a local workspace for imported sessions', () => {
    insertAgent(
      dbh.db,
      AGENT_ID,
      { permission_mode: 'plan', env_vars: { LOCAL_ONLY: 'preserve' }, reasoning_effort: 'low' },
      ['local-tool']
    )
    insertAgent(dbh.db, BUILTIN_ID, { builtin_role: BUILTIN_AGENT_ROLE.SUPPORT })
    const agentAdapter = adapterFor('agent')
    const agentPayload = {
      id: AGENT_ID,
      type: 'pi',
      name: 'Synced agent',
      description: 'Portable profile',
      instructions: 'Follow the shared instructions.',
      model: null,
      planModel: null,
      smallModel: null,
      configuration: { reasoning_effort: 'high' },
      orderKey: AGENT_ID,
      createdAt: NOW,
      updatedAt: NOW
    }
    const agentResult = dbh.db.transaction((tx) =>
      agentAdapter.apply(tx, syncChange(agentAdapter, AGENT_ID, agentPayload))
    )
    const agent = dbh.db.select().from(agentTable).where(eq(agentTable.id, AGENT_ID)).get()

    expect(agentResult).toBe('applied')
    expect(agent?.disabledTools).toEqual(['local-tool'])
    expect(agent?.configuration).toEqual({
      permission_mode: 'plan',
      env_vars: { LOCAL_ONLY: 'preserve' },
      reasoning_effort: 'high'
    })
    expect(agentAdapter.get(dbh.db, BUILTIN_ID)).toMatchObject({ payload: null, withheld: true })

    const sessionAdapter = adapterFor('agent_session')
    const sessionPayload = {
      id: SESSION_ID,
      agentId: null,
      model: null,
      name: 'Imported session',
      isNameManuallyEdited: false,
      description: '',
      activeNodeId: null,
      orderKey: SESSION_ID,
      lastActivityAt: NOW,
      createdAt: NOW,
      updatedAt: NOW
    }
    const sessionResult = dbh.db.transaction((tx) =>
      sessionAdapter.apply(tx, syncChange(sessionAdapter, SESSION_ID, sessionPayload))
    )
    const session = dbh.db.select().from(agentSessionTable).where(eq(agentSessionTable.id, SESSION_ID)).get()
    const workspace = session
      ? dbh.db.select().from(agentWorkspaceTable).where(eq(agentWorkspaceTable.id, session.workspaceId)).get()
      : undefined

    expect(sessionResult).toBe('applied')
    expect(workspace?.type).toBe('system')
    expect(workspace?.path).toContain(SESSION_ID)
    expect(sessionAdapter.get(dbh.db, SESSION_ID)?.payload).not.toHaveProperty('workspaceId')

    const messageAdapter = adapterFor('agent_session_message')
    const rootPayload = {
      id: SESSION_ROOT_ID,
      sessionId: SESSION_ID,
      role: 'root',
      parentId: null,
      data: { parts: [] },
      status: 'success',
      modelId: null,
      messageSnapshot: null,
      stats: null,
      createdAt: NOW,
      updatedAt: NOW
    }
    const rootResult = dbh.db.transaction((tx) =>
      messageAdapter.apply(tx, syncChange(messageAdapter, SESSION_ROOT_ID, rootPayload))
    )
    const importedRoot = dbh.db
      .select()
      .from(agentSessionMessageTable)
      .where(eq(agentSessionMessageTable.id, SESSION_ROOT_ID))
      .get()
    const updatedSession = dbh.db.select().from(agentSessionTable).where(eq(agentSessionTable.id, SESSION_ID)).get()

    expect(rootResult).toBe('applied')
    expect(importedRoot?.role).toBe('root')
    expect(updatedSession?.activeNodeId).toBe(SESSION_ROOT_ID)
  })

  it('maps per-agent skill state by content hash instead of installation id', () => {
    insertAgent(dbh.db, AGENT_ID)
    dbh.db
      .insert(agentGlobalSkillTable)
      .values({
        id: SKILL_ID,
        name: 'Shared skill',
        description: null,
        folderName: 'shared-skill',
        source: 'local',
        sourceUrl: null,
        namespace: null,
        author: null,
        version: null,
        tags: [],
        contentHash: SKILL_CONTENT_HASH,
        isEnabled: true,
        mirrorEnabled: false,
        createdAt: NOW,
        updatedAt: NOW
      })
      .run()
    dbh.db.insert(agentSkillTable).values({ agentId: AGENT_ID, skillId: SKILL_ID, isEnabled: true }).run()

    const adapter = adapterFor('agent_skill')
    const id = JSON.stringify([AGENT_ID, SKILL_CONTENT_HASH])
    const source = adapter.get(dbh.db, id)
    dbh.db.delete(agentSkillTable).where(eq(agentSkillTable.agentId, AGENT_ID)).run()
    const result = dbh.db.transaction((tx) => adapter.apply(tx, syncChange(adapter, id, source!.payload!)))
    const local = dbh.db
      .select({ isEnabled: agentSkillTable.isEnabled, skillId: agentSkillTable.skillId })
      .from(agentSkillTable)
      .where(eq(agentSkillTable.agentId, AGENT_ID))
      .get()

    expect(source?.payload).toMatchObject({ agentId: AGENT_ID, contentHash: SKILL_CONTENT_HASH, isEnabled: true })
    expect(source?.payload).not.toHaveProperty('skillId')
    expect(result).toBe('applied')
    expect(local).toEqual({ isEnabled: true, skillId: SKILL_ID })
  })

  it('defers imported agent history while a local stream is pending', () => {
    const sessionAdapter = adapterFor('agent_session')
    dbh.db.transaction((tx) =>
      sessionAdapter.apply(
        tx,
        syncChange(sessionAdapter, SESSION_ID, {
          id: SESSION_ID,
          agentId: null,
          model: null,
          name: 'Active session',
          isNameManuallyEdited: false,
          description: '',
          activeNodeId: null,
          orderKey: SESSION_ID,
          lastActivityAt: NOW,
          createdAt: NOW,
          updatedAt: NOW
        })
      )
    )
    const messageAdapter = adapterFor('agent_session_message')
    dbh.db.transaction((tx) =>
      messageAdapter.apply(
        tx,
        syncChange(messageAdapter, SESSION_ROOT_ID, {
          id: SESSION_ROOT_ID,
          sessionId: SESSION_ID,
          role: 'root',
          parentId: null,
          data: { parts: [] },
          status: 'success',
          modelId: null,
          messageSnapshot: null,
          stats: null,
          createdAt: NOW,
          updatedAt: NOW
        })
      )
    )
    dbh.db
      .insert(agentSessionMessageTable)
      .values({
        id: SESSION_MESSAGE_ID,
        sessionId: SESSION_ID,
        role: 'user',
        parentId: SESSION_ROOT_ID,
        data: { parts: [{ type: 'text', text: 'Before stream' }] },
        status: 'success',
        createdAt: NOW,
        updatedAt: NOW
      })
      .run()
    dbh.db
      .insert(agentSessionMessageTable)
      .values({
        id: SESSION_PENDING_MESSAGE_ID,
        sessionId: SESSION_ID,
        role: 'assistant',
        parentId: SESSION_MESSAGE_ID,
        data: { parts: [{ type: 'text', text: 'Working' }] },
        status: 'pending',
        runtimeResumeToken: 'local-resume-token',
        createdAt: NOW,
        updatedAt: NOW
      })
      .run()

    const before = messageAdapter.get(dbh.db, SESSION_MESSAGE_ID)
    const result = dbh.db.transaction((tx) =>
      messageAdapter.apply(
        tx,
        syncChange(messageAdapter, SESSION_MESSAGE_ID, {
          id: SESSION_MESSAGE_ID,
          sessionId: SESSION_ID,
          role: 'user',
          parentId: SESSION_ROOT_ID,
          data: { parts: [{ type: 'text', text: 'Remote edit' }] },
          status: 'success',
          modelId: null,
          messageSnapshot: null,
          stats: null,
          createdAt: NOW,
          updatedAt: NOW + 1
        })
      )
    )
    const local = dbh.db
      .select()
      .from(agentSessionMessageTable)
      .where(eq(agentSessionMessageTable.id, SESSION_MESSAGE_ID))
      .get()

    expect(before).toMatchObject({ payload: null, withheld: true })
    expect(result).toBe('deferred')
    expect(local?.data.parts?.[0]).toMatchObject({ text: 'Before stream' })
  })
})
