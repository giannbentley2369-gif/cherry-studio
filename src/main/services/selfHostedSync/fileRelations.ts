import { and, eq } from 'drizzle-orm'

import { notifyDataApiDataChange } from '@data/dataApiDataChange'
import { fileEntryTable } from '@data/db/schemas/file'
import {
  chatMessageFileRefTable,
  agentSessionMessageFileRefTable,
  paintingFileRefTable,
  translateHistoryFileRefTable,
  providerLogoFileRefTable,
  assistantAvatarFileRefTable,
  agentAvatarFileRefTable,
  miniAppLogoFileRefTable
} from '@data/db/schemas/fileRelations'
import { miniAppTable } from '@data/db/schemas/miniApp'
import type { DbOrTx } from '@data/db/types'
import { agentService } from '@data/services/AgentService'
import { agentSessionMessageService } from '@data/services/AgentSessionMessageService'
import { assistantDataService } from '@data/services/AssistantService'
import { messageService } from '@data/services/MessageService'
import { miniAppService } from '@data/services/MiniAppService'
import { paintingService } from '@data/services/PaintingService'
import { providerService } from '@data/services/ProviderService'
import { translateHistoryService } from '@data/services/TranslateHistoryService'
import { CHERRY_SUPPORT_AGENT_ID } from '@shared/ai/builtinAgent'
import { isManagedCherryProviderId } from '@shared/data/presets/cherryai'
import { FileEntryIdSchema } from '@shared/data/types/file'
import type { SyncCategory } from '@shared/ipc/schemas/selfHostedSync'

import { defineTable, type EntityAdapter, type Payload } from './entity'

function validateRef(row: Payload): Payload {
  FileEntryIdSchema.parse(row.id)
  FileEntryIdSchema.parse(row.fileEntryId)
  if (typeof row.sourceId !== 'string' || !row.sourceId.length) throw new Error('Invalid attachment owner.')
  return row
}

export function createFileRelations(categories: () => readonly SyncCategory[]): EntityAdapter[] {
  function eligible(tx: DbOrTx, category: SyncCategory, id: string, sourceId: string, table: string): boolean {
    if (!categories().includes(category)) return false
    if (
      table === 'miniAppLogoFileRefTable' &&
      !tx
        .select({ id: miniAppTable.appId })
        .from(miniAppTable)
        .where(and(eq(miniAppTable.appId, sourceId), eq(miniAppTable.kind, 'site')))
        .get()
    )
      return false
    if (table === 'providerLogoFileRefTable' && isManagedCherryProviderId(sourceId)) return false
    if (table === 'agentAvatarFileRefTable' && sourceId === CHERRY_SUPPORT_AGENT_ID) return false
    return !!tx
      .select({ id: fileEntryTable.id })
      .from(fileEntryTable)
      .where(and(eq(fileEntryTable.id, id), eq(fileEntryTable.origin, 'internal')))
      .get()
  }
  return [
    defineTable({
      table: chatMessageFileRefTable,
      category: 'files',
      keys: ['fileEntryId', 'sourceId', 'role'],
      fields: ['id', 'fileEntryId', 'sourceId', 'role', 'createdAt', 'updatedAt'],
      project: (row, db) =>
        eligible(db, 'chats', row.fileEntryId, row.sourceId, 'chatMessageFileRefTable') ? row : null,
      validate: validateRef,
      materialize: (row, _existing, tx) =>
        eligible(tx, 'chats', String(row.fileEntryId), String(row.sourceId), 'chatMessageFileRefTable') ? row : false,
      write: (tx, key, row) =>
        messageService.applySyncedAttachmentTx(
          tx,
          key as Pick<typeof chatMessageFileRefTable.$inferSelect, 'fileEntryId' | 'sourceId' | 'role'>,
          row
        ),
      afterCommit: () =>
        notifyDataApiDataChange([
          { endpoint: '/topics/:topicId/messages', kind: 'membership' },
          { endpoint: '/files/entries', kind: 'membership' }
        ])
    }),
    defineTable({
      table: agentSessionMessageFileRefTable,
      category: 'files',
      keys: ['fileEntryId', 'sourceId', 'role'],
      fields: ['id', 'fileEntryId', 'sourceId', 'role', 'createdAt', 'updatedAt'],
      project: (row, db) =>
        eligible(db, 'agents', row.fileEntryId, row.sourceId, 'agentSessionMessageFileRefTable') ? row : null,
      validate: validateRef,
      materialize: (row, _existing, tx) =>
        eligible(tx, 'agents', String(row.fileEntryId), String(row.sourceId), 'agentSessionMessageFileRefTable')
          ? row
          : false,
      write: (tx, key, row) =>
        agentSessionMessageService.applySyncedAttachmentTx(
          tx,
          key as Pick<typeof agentSessionMessageFileRefTable.$inferSelect, 'fileEntryId' | 'sourceId' | 'role'>,
          row
        ),
      afterCommit: () =>
        notifyDataApiDataChange([
          { endpoint: '/agent-sessions/:sessionId/messages', kind: 'membership' },
          { endpoint: '/files/entries', kind: 'membership' }
        ])
    }),
    defineTable({
      table: paintingFileRefTable,
      category: 'files',
      keys: ['fileEntryId', 'sourceId', 'role'],
      fields: ['id', 'fileEntryId', 'sourceId', 'role', 'createdAt', 'updatedAt'],
      project: (row, db) => (eligible(db, 'other', row.fileEntryId, row.sourceId, 'paintingFileRefTable') ? row : null),
      validate: validateRef,
      materialize: (row, _existing, tx) =>
        eligible(tx, 'other', String(row.fileEntryId), String(row.sourceId), 'paintingFileRefTable') ? row : false,
      write: (tx, key, row) =>
        paintingService.applySyncedAttachmentTx(
          tx,
          key as Pick<typeof paintingFileRefTable.$inferSelect, 'fileEntryId' | 'sourceId' | 'role'>,
          row
        ),
      afterCommit: () =>
        notifyDataApiDataChange([
          { endpoint: '/paintings', kind: 'membership' },
          { endpoint: '/files/entries', kind: 'membership' }
        ])
    }),
    defineTable({
      table: translateHistoryFileRefTable,
      category: 'files',
      keys: ['sourceId', 'role'],
      fields: ['id', 'fileEntryId', 'sourceId', 'role', 'createdAt', 'updatedAt'],
      project: (row, db) =>
        eligible(db, 'other', row.fileEntryId, row.sourceId, 'translateHistoryFileRefTable') ? row : null,
      validate: validateRef,
      materialize: (row, _existing, tx) =>
        eligible(tx, 'other', String(row.fileEntryId), String(row.sourceId), 'translateHistoryFileRefTable')
          ? row
          : false,
      write: (tx, key, row) =>
        translateHistoryService.applySyncedAttachmentTx(
          tx,
          key as Pick<typeof translateHistoryFileRefTable.$inferSelect, 'sourceId' | 'role'>,
          row
        ),
      afterCommit: () =>
        notifyDataApiDataChange([
          { endpoint: '/translate/histories', kind: 'membership' },
          { endpoint: '/files/entries', kind: 'membership' }
        ])
    }),
    defineTable({
      table: providerLogoFileRefTable,
      category: 'files',
      keys: ['sourceId'],
      fields: ['id', 'fileEntryId', 'sourceId', 'createdAt', 'updatedAt'],
      project: (row, db) =>
        eligible(db, 'providers', row.fileEntryId, row.sourceId, 'providerLogoFileRefTable') ? row : null,
      validate: validateRef,
      materialize: (row, _existing, tx) =>
        eligible(tx, 'providers', String(row.fileEntryId), String(row.sourceId), 'providerLogoFileRefTable')
          ? row
          : false,
      write: (tx, key, row) =>
        providerService.applySyncedAttachmentTx(
          tx,
          key as Pick<typeof providerLogoFileRefTable.$inferSelect, 'sourceId'>,
          row
        ),
      afterCommit: () =>
        notifyDataApiDataChange([
          { endpoint: '/providers', kind: 'membership' },
          { endpoint: '/files/entries', kind: 'membership' }
        ])
    }),
    defineTable({
      table: assistantAvatarFileRefTable,
      category: 'files',
      keys: ['sourceId'],
      fields: ['id', 'fileEntryId', 'sourceId', 'createdAt', 'updatedAt'],
      project: (row, db) =>
        eligible(db, 'assistants', row.fileEntryId, row.sourceId, 'assistantAvatarFileRefTable') ? row : null,
      validate: validateRef,
      materialize: (row, _existing, tx) =>
        eligible(tx, 'assistants', String(row.fileEntryId), String(row.sourceId), 'assistantAvatarFileRefTable')
          ? row
          : false,
      write: (tx, key, row) =>
        assistantDataService.applySyncedAttachmentTx(
          tx,
          key as Pick<typeof assistantAvatarFileRefTable.$inferSelect, 'sourceId'>,
          row
        ),
      afterCommit: () =>
        notifyDataApiDataChange([
          { endpoint: '/assistants', kind: 'membership' },
          { endpoint: '/files/entries', kind: 'membership' }
        ])
    }),
    defineTable({
      table: agentAvatarFileRefTable,
      category: 'files',
      keys: ['sourceId'],
      fields: ['id', 'fileEntryId', 'sourceId', 'createdAt', 'updatedAt'],
      project: (row, db) =>
        eligible(db, 'agents', row.fileEntryId, row.sourceId, 'agentAvatarFileRefTable') ? row : null,
      validate: validateRef,
      materialize: (row, _existing, tx) =>
        eligible(tx, 'agents', String(row.fileEntryId), String(row.sourceId), 'agentAvatarFileRefTable') ? row : false,
      write: (tx, key, row) =>
        agentService.applySyncedAttachmentTx(
          tx,
          key as Pick<typeof agentAvatarFileRefTable.$inferSelect, 'sourceId'>,
          row
        ),
      afterCommit: () =>
        notifyDataApiDataChange([
          { endpoint: '/agents', kind: 'membership' },
          { endpoint: '/files/entries', kind: 'membership' }
        ])
    }),
    defineTable({
      table: miniAppLogoFileRefTable,
      category: 'files',
      keys: ['sourceId'],
      fields: ['id', 'fileEntryId', 'sourceId', 'createdAt', 'updatedAt'],
      project: (row, db) =>
        eligible(db, 'other', row.fileEntryId, row.sourceId, 'miniAppLogoFileRefTable') ? row : null,
      validate: validateRef,
      materialize: (row, _existing, tx) =>
        eligible(tx, 'other', String(row.fileEntryId), String(row.sourceId), 'miniAppLogoFileRefTable') ? row : false,
      write: (tx, key, row) =>
        miniAppService.applySyncedAttachmentTx(
          tx,
          key as Pick<typeof miniAppLogoFileRefTable.$inferSelect, 'sourceId'>,
          row
        ),
      afterCommit: () =>
        notifyDataApiDataChange([
          { endpoint: '/mini-apps', kind: 'membership' },
          { endpoint: '/files/entries', kind: 'membership' }
        ])
    })
  ]
}
