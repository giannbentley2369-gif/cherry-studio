import { and, eq, isNull } from 'drizzle-orm'

import { application } from '@application'
import { agentSessionTable } from '@data/db/schemas/agentSession'
import { agentSessionMessageTable } from '@data/db/schemas/agentSessionMessage'
import { messageTable } from '@data/db/schemas/message'
import { topicTable } from '@data/db/schemas/topic'
import type { DbOrTx } from '@data/db/types'

import type { EntityAdapter, SyncChange } from './entity'

function historyIds(tx: DbOrTx, change: SyncChange): string[] {
  if (change.entityType === 'topic' || change.entityType === 'agent_session') return [change.entityId]
  const chat = change.entityType === 'message'
  if (!chat && change.entityType !== 'agent_session_message') return []
  const incoming = change.payload?.[chat ? 'topicId' : 'sessionId']
  const existing = chat
    ? tx.select({ id: messageTable.topicId }).from(messageTable).where(eq(messageTable.id, change.entityId)).get()
    : tx
        .select({ id: agentSessionMessageTable.sessionId })
        .from(agentSessionMessageTable)
        .where(eq(agentSessionMessageTable.id, change.entityId))
        .get()
  return [...new Set([existing?.id, incoming].filter((id): id is string => typeof id === 'string'))]
}

export function guardLiveHistory(adapter: EntityAdapter): EntityAdapter {
  const chat = adapter.type === 'topic' || adapter.type === 'message'
  if (!chat && adapter.type !== 'agent_session' && adapter.type !== 'agent_session_message') return adapter
  return {
    ...adapter,
    apply(tx, change) {
      for (const id of historyIds(tx, change)) {
        const busy = chat
          ? application.get('AiStreamManager').hasUnsettledTopicWork(id)
          : application.get('AgentSessionRuntimeService').isSessionBusyForArchive(id)
        if (busy) return 'deferred'
      }
      return adapter.apply(tx, change)
    }
  }
}

// A page boundary, deferred parent or tombstone must never leave a live history without its root.
export function assertHistoryRoots(tx: DbOrTx, changes: Iterable<SyncChange>): void {
  const chats = new Set<string>()
  const sessions = new Set<string>()
  for (const change of changes) {
    const ids = change.entityType === 'topic' || change.entityType === 'message' ? chats : sessions
    for (const id of historyIds(tx, change)) ids.add(id)
  }
  for (const id of chats) {
    const live = tx
      .select({ id: topicTable.id })
      .from(topicTable)
      .where(and(eq(topicTable.id, id), isNull(topicTable.deletedAt)))
      .get()
    if (!live) continue
    const root = tx
      .select({ id: messageTable.id })
      .from(messageTable)
      .where(
        and(
          eq(messageTable.topicId, id),
          isNull(messageTable.parentId),
          eq(messageTable.role, 'root'),
          isNull(messageTable.deletedAt)
        )
      )
      .get()
    if (!root) throw new Error('Sync is waiting for the conversation root.')
  }
  for (const id of sessions) {
    const live = tx
      .select({ id: agentSessionTable.id })
      .from(agentSessionTable)
      .where(and(eq(agentSessionTable.id, id), isNull(agentSessionTable.deletedAt)))
      .get()
    if (!live) continue
    const root = tx
      .select({ id: agentSessionMessageTable.id })
      .from(agentSessionMessageTable)
      .where(
        and(
          eq(agentSessionMessageTable.sessionId, id),
          isNull(agentSessionMessageTable.parentId),
          eq(agentSessionMessageTable.role, 'root')
        )
      )
      .get()
    if (!root) throw new Error('Sync is waiting for the conversation root.')
  }
}
