import { and, eq, inArray, isNull } from 'drizzle-orm'
import * as z from 'zod'

import { application } from '@application'
import { notifyDataApiDataChange } from '@data/dataApiDataChange'
import { agentTable } from '@data/db/schemas/agent'
import { agentGlobalSkillTable } from '@data/db/schemas/agentGlobalSkill'
import { agentSessionTable } from '@data/db/schemas/agentSession'
import { agentSessionMessageTable } from '@data/db/schemas/agentSessionMessage'
import { agentSkillTable } from '@data/db/schemas/agentSkill'
import { fileEntryTable } from '@data/db/schemas/file'
import { messageTable } from '@data/db/schemas/message'
import { topicTable } from '@data/db/schemas/topic'
import type { DbOrTx } from '@data/db/types'
import { agentGlobalSkillService } from '@data/services/AgentGlobalSkillService'
import { agentService } from '@data/services/AgentService'
import { agentSessionMessageService } from '@data/services/AgentSessionMessageService'
import { agentSessionService } from '@data/services/AgentSessionService'
import { messageService } from '@data/services/MessageService'
import { topicService } from '@data/services/TopicService'
import { CHERRY_SUPPORT_AGENT_ID, isProtectedBuiltinAgentRole } from '@shared/ai/builtinAgent'
import { AgentTypeSchema } from '@shared/data/api/schemas/agents'
import { AgentLanguageSchema } from '@shared/data/types/agentLanguage'
import { FileEntryIdSchema } from '@shared/data/types/file'
import {
  MessageRoleSchema,
  MessageSnapshotSchema,
  MessageStatsSchema,
  type CherryMessagePart,
  type MessageData,
  type MessageStats
} from '@shared/data/types/message'
import { ServiceTierSelectionSchema, UniqueModelIdSchema } from '@shared/data/types/model'
import { readCherryMeta } from '@shared/data/types/uiParts'
import { ReasoningEffortOptionSchema } from '@shared/types/aiSdk'

import { defineTable, type EntityAdapter, type LocalEntity } from './entity'

const AGENT_CATEGORY = 'agents' as const
const CHAT_CATEGORY = 'chats' as const
const TimestampSchema = z.number().int().nonnegative()
const PortableMessageStatusSchema = z.enum(['success', 'error', 'paused'])
const PortableSessionMessageStatusSchema = z.enum(['success', 'error', 'paused'])

const PortableAgentConfigurationSchema = z.strictObject({
  reasoning_effort: ReasoningEffortOptionSchema.optional(),
  service_tier: ServiceTierSelectionSchema.optional(),
  language: AgentLanguageSchema.nullable().optional()
})

const PortableAgentSchema = z.strictObject({
  id: z.uuid(),
  type: AgentTypeSchema,
  name: z.string(),
  description: z.string(),
  instructions: z.string(),
  model: UniqueModelIdSchema.nullable(),
  planModel: UniqueModelIdSchema.nullable(),
  smallModel: UniqueModelIdSchema.nullable(),
  configuration: PortableAgentConfigurationSchema,
  orderKey: z.string().min(1),
  createdAt: TimestampSchema,
  updatedAt: TimestampSchema
})

const PortableTopicSchema = z.strictObject({
  id: z.uuid(),
  name: z.string(),
  isNameManuallyEdited: z.boolean(),
  assistantId: z.string().min(1).nullable(),
  activeNodeId: z.uuid().nullable(),
  orderKey: z.string().min(1),
  lastActivityAt: TimestampSchema,
  createdAt: TimestampSchema,
  updatedAt: TimestampSchema
})

const SafeFilenameSchema = z
  .string()
  .max(512)
  .refine((value) => !/[\\/:]/.test(value) && value !== '.' && value !== '..')
const PortableFilePartSchema = z.strictObject({
  type: z.literal('file'),
  mediaType: z.string().optional(),
  filename: SafeFilenameSchema.optional(),
  providerMetadata: z.strictObject({
    cherry: z.strictObject({
      fileEntryId: FileEntryIdSchema,
      remoteAttachment: z
        .strictObject({ sha256: z.string().regex(/^[a-f\d]{64}$/i), byteLength: z.number().int().nonnegative() })
        .optional(),
      composerFileKind: z.literal('pasted-text').optional()
    })
  })
})

const NONPORTABLE_PART_TYPES = new Set([
  'data-agent-task-event',
  'data-agent-session-fork',
  'data-compaction-anchor',
  'data-knowledge-scope'
])
const NONPORTABLE_PART_FIELDS = ['cwd', 'filePath', 'originalPath', 'workingFileEntryId'] as const
const NONPORTABLE_STAT_FIELDS = new Set(['runtimeTiming', 'timeFirstTokenMs', 'timeCompletionMs', 'timeThinkingMs'])

const PortableMessageDataSchema = z
  .strictObject({
    parts: z.array(z.unknown()),
    modelSelection: z.enum(['default', 'explicit']).optional()
  })
  .superRefine((data, context) => {
    data.parts.forEach((part, index) => {
      if (!isRecord(part)) return
      if (typeof part.type === 'string' && NONPORTABLE_PART_TYPES.has(part.type)) {
        context.addIssue({ code: 'custom', path: ['parts', index], message: 'Runtime part is not portable' })
      }
      if (part.type === 'file') {
        if (!PortableFilePartSchema.safeParse(part).success) {
          context.addIssue({ code: 'custom', path: ['parts', index], message: 'File part is not portable' })
        }
        return
      }
      if ('providerMetadata' in part || NONPORTABLE_PART_FIELDS.some((field) => field in part)) {
        context.addIssue({ code: 'custom', path: ['parts', index], message: 'Local metadata is not portable' })
      }
      if (part.type === 'data-video' && 'url' in part) {
        context.addIssue({ code: 'custom', path: ['parts', index], message: 'Video URLs are not portable' })
      }
    })
  })

const PortableStatsSchema = MessageStatsSchema.omit({
  runtimeTiming: true,
  timeFirstTokenMs: true,
  timeCompletionMs: true,
  timeThinkingMs: true
})

const PortableMessageSchema = z
  .strictObject({
    id: z.uuid(),
    topicId: z.uuid(),
    parentId: z.uuid().nullable(),
    role: MessageRoleSchema,
    data: PortableMessageDataSchema,
    status: PortableMessageStatusSchema,
    siblingsGroupId: z.number().int().nonnegative(),
    modelId: UniqueModelIdSchema.nullable(),
    messageSnapshot: MessageSnapshotSchema.nullable(),
    stats: PortableStatsSchema.nullable(),
    createdAt: TimestampSchema,
    updatedAt: TimestampSchema
  })
  .superRefine((message, context) => {
    if ((message.role === 'root') !== (message.parentId === null)) {
      context.addIssue({ code: 'custom', path: ['parentId'], message: 'Invalid message tree relationship' })
    }
  })

const PortableAgentSessionSchema = z.strictObject({
  id: z.uuid(),
  agentId: z.uuid().nullable(),
  model: UniqueModelIdSchema.nullable(),
  name: z.string(),
  isNameManuallyEdited: z.boolean(),
  description: z.string(),
  activeNodeId: z.uuid().nullable(),
  orderKey: z.string().min(1),
  lastActivityAt: TimestampSchema,
  createdAt: TimestampSchema,
  updatedAt: TimestampSchema
})

const PortableAgentSessionMessageSchema = z
  .strictObject({
    id: z.uuid(),
    sessionId: z.uuid(),
    role: MessageRoleSchema,
    parentId: z.uuid().nullable(),
    data: PortableMessageDataSchema,
    status: PortableSessionMessageStatusSchema,
    modelId: UniqueModelIdSchema.nullable(),
    messageSnapshot: MessageSnapshotSchema.nullable(),
    stats: PortableStatsSchema.nullable(),
    createdAt: TimestampSchema,
    updatedAt: TimestampSchema
  })
  .superRefine((message, context) => {
    if ((message.role === 'root') !== (message.parentId === null)) {
      context.addIssue({ code: 'custom', path: ['parentId'], message: 'Invalid message tree relationship' })
    }
  })

const PortableAgentSkillSchema = z.strictObject({
  agentId: z.uuid(),
  contentHash: z.string().min(1),
  isEnabled: z.boolean(),
  createdAt: TimestampSchema,
  updatedAt: TimestampSchema
})

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
}

function isBuiltinAgentRow(row: { id: string; configuration: unknown }): boolean {
  const role = isRecord(row.configuration) ? row.configuration.builtin_role : undefined
  return row.id === CHERRY_SUPPORT_AGENT_ID || isProtectedBuiltinAgentRole(role)
}

function portableAgentConfiguration(value: unknown): z.infer<typeof PortableAgentConfigurationSchema> {
  const source = isRecord(value) ? value : {}
  return PortableAgentConfigurationSchema.parse({
    ...(source.reasoning_effort !== undefined ? { reasoning_effort: source.reasoning_effort } : {}),
    ...(source.service_tier !== undefined ? { service_tier: source.service_tier } : {}),
    ...(source.language !== undefined ? { language: source.language } : {})
  })
}

function sanitizeFilename(value: unknown): string | undefined {
  if (typeof value !== 'string') return undefined
  const name = value.split(/[\\/]/).at(-1)
  return name && name !== '.' && name !== '..' ? SafeFilenameSchema.parse(name) : undefined
}

function sanitizeFilePart(part: Record<string, unknown>, db: DbOrTx): Record<string, unknown> | undefined {
  const meta = readCherryMeta(part as unknown as Extract<CherryMessagePart, { type: 'file' }>)
  const entryId = FileEntryIdSchema.safeParse(meta?.fileEntryId)
  if (!entryId.success) return undefined
  const file = db
    .select({ id: fileEntryTable.id })
    .from(fileEntryTable)
    .where(
      and(eq(fileEntryTable.id, entryId.data), eq(fileEntryTable.origin, 'internal'), isNull(fileEntryTable.deletedAt))
    )
    .get()
  if (!file) return undefined

  const providerMetadata = {
    cherry: {
      fileEntryId: entryId.data,
      ...(meta?.remoteAttachment ? { remoteAttachment: meta.remoteAttachment } : {}),
      ...(meta?.composerFileKind ? { composerFileKind: meta.composerFileKind } : {})
    }
  }
  const filename = sanitizeFilename(part.filename)
  return PortableFilePartSchema.parse({
    type: 'file',
    ...(typeof part.mediaType === 'string' ? { mediaType: part.mediaType } : {}),
    ...(filename ? { filename } : {}),
    providerMetadata
  })
}

function sanitizeMessagePart(value: unknown, db: DbOrTx): unknown | undefined {
  if (!isRecord(value)) return value
  if (typeof value.type === 'string' && NONPORTABLE_PART_TYPES.has(value.type)) return undefined
  if (value.type === 'file') return sanitizeFilePart(value, db)

  const safe = Object.fromEntries(
    Object.entries(value).filter(
      ([key]) =>
        key !== 'providerMetadata' && !NONPORTABLE_PART_FIELDS.includes(key as (typeof NONPORTABLE_PART_FIELDS)[number])
    )
  )
  if (safe.type === 'data-video') delete safe.url
  if (safe.type === 'data-error' && isRecord(safe.data)) {
    safe.data = Object.fromEntries(Object.entries(safe.data).filter(([key]) => key !== 'cause' && key !== 'stack'))
  }
  return safe
}

function portableMessageData(value: MessageData, db: DbOrTx): z.infer<typeof PortableMessageDataSchema> {
  const parts = (Array.isArray(value.parts) ? value.parts : []).flatMap((part) => {
    const safe = sanitizeMessagePart(part, db)
    return safe === undefined ? [] : [safe]
  })
  return PortableMessageDataSchema.parse({
    parts,
    ...(value.modelSelection ? { modelSelection: value.modelSelection } : {})
  })
}

function materializeMessageData(value: unknown, db: DbOrTx): MessageData | false {
  const data = PortableMessageDataSchema.parse(value)
  for (const part of data.parts) {
    const parsed = PortableFilePartSchema.safeParse(part)
    if (!parsed.success) continue
    const fileId = parsed.data.providerMetadata.cherry.fileEntryId
    const file = db
      .select({ id: fileEntryTable.id })
      .from(fileEntryTable)
      .where(
        and(eq(fileEntryTable.id, fileId), eq(fileEntryTable.origin, 'internal'), isNull(fileEntryTable.deletedAt))
      )
      .get()
    if (!file) return false
  }
  const parts = data.parts.map((part) => {
    const parsed = PortableFilePartSchema.safeParse(part)
    if (!parsed.success) return part
    const fileId = parsed.data.providerMetadata.cherry.fileEntryId
    return { ...parsed.data, url: application.get('FileManager').getUrl(fileId) }
  })
  return {
    parts: parts as MessageData['parts'],
    ...(data.modelSelection ? { modelSelection: data.modelSelection } : {})
  }
}

function portableSnapshot(value: unknown) {
  const parsed = MessageSnapshotSchema.safeParse(value)
  return parsed.success ? parsed.data : null
}

function portableStats(value: unknown): MessageStats | null {
  if (!isRecord(value)) return null
  const safe = Object.fromEntries(Object.entries(value).filter(([key]) => !NONPORTABLE_STAT_FIELDS.has(key)))
  const parsed = PortableStatsSchema.safeParse(safe)
  return parsed.success ? parsed.data : null
}

function portableChatMessage(
  row: typeof messageTable.$inferSelect,
  db: DbOrTx
): z.infer<typeof PortableMessageSchema> | null {
  const status = PortableMessageStatusSchema.safeParse(row.status)
  if (!status.success) return null
  try {
    return PortableMessageSchema.parse({
      id: row.id,
      topicId: row.topicId,
      parentId: row.parentId,
      role: row.role,
      data: portableMessageData(row.data, db),
      status: status.data,
      siblingsGroupId: row.siblingsGroupId,
      modelId: row.modelId,
      messageSnapshot: portableSnapshot(row.messageSnapshot),
      stats: portableStats(row.stats),
      createdAt: row.createdAt,
      updatedAt: row.updatedAt
    })
  } catch {
    return null
  }
}

function hasPendingChatMessage(db: DbOrTx, topicId: string): boolean {
  return Boolean(
    db
      .select({ id: messageTable.id })
      .from(messageTable)
      .where(and(eq(messageTable.topicId, topicId), eq(messageTable.status, 'pending'), isNull(messageTable.deletedAt)))
      .limit(1)
      .get()
  )
}

function hasPortableChatRoot(db: DbOrTx, topicId: string): boolean {
  const root = db
    .select()
    .from(messageTable)
    .where(
      and(
        eq(messageTable.topicId, topicId),
        eq(messageTable.role, 'root'),
        isNull(messageTable.parentId),
        isNull(messageTable.deletedAt)
      )
    )
    .limit(1)
    .get()
  return Boolean(root && portableChatMessage(root, db))
}

function hasPendingAgentMessage(db: DbOrTx, agentId: string): boolean {
  return Boolean(
    db
      .select({ id: agentSessionMessageTable.id })
      .from(agentSessionTable)
      .innerJoin(agentSessionMessageTable, eq(agentSessionMessageTable.sessionId, agentSessionTable.id))
      .where(
        and(
          eq(agentSessionTable.agentId, agentId),
          eq(agentSessionTable.type, 'conversation'),
          isNull(agentSessionTable.deletedAt),
          eq(agentSessionMessageTable.status, 'pending')
        )
      )
      .limit(1)
      .get()
  )
}

function portableAgentId(db: DbOrTx, agentId: string | null): string | null {
  if (!agentId) return null
  const agent = db
    .select({ id: agentTable.id, configuration: agentTable.configuration })
    .from(agentTable)
    .where(eq(agentTable.id, agentId))
    .get()
  return agent && !isBuiltinAgentRow(agent) ? agentId : null
}

const agentTableAdapter = defineTable({
  table: agentTable,
  category: AGENT_CATEGORY,
  keys: ['id'],
  fields: [
    'id',
    'type',
    'name',
    'description',
    'instructions',
    'model',
    'planModel',
    'smallModel',
    'configuration',
    'orderKey',
    'createdAt',
    'updatedAt'
  ],
  project: (row, db) => {
    if (isBuiltinAgentRow(row) || hasPendingAgentMessage(db, row.id)) return null
    const type = AgentTypeSchema.safeParse(row.type)
    if (!type.success) return null
    return {
      id: row.id,
      type: type.data,
      name: row.name,
      description: row.description,
      instructions: row.instructions,
      model: row.model,
      planModel: row.planModel,
      smallModel: row.smallModel,
      configuration: portableAgentConfiguration(row.configuration),
      orderKey: row.orderKey,
      createdAt: row.createdAt,
      updatedAt: row.updatedAt
    }
  },
  validate: (payload) => PortableAgentSchema.parse(payload),
  write: (tx, key, row, change) =>
    agentService.applySelfHostedSyncTx(tx, key.id, row as Parameters<typeof agentService.applySelfHostedSyncTx>[2], {
      deletedAt: change.deleted ? change.deletedAt : change.updatedAt,
      updatedAt: change.updatedAt
    }),
  afterCommit: (changes) => {
    const ids = [...new Set(changes.map((change) => change.entityId))]
    agentService.notifyReadModelChange(ids, 'membership')
    const db = application.get('DbService').getDb()
    const sessionIds = db
      .select({ id: agentSessionTable.id })
      .from(agentSessionTable)
      .where(inArray(agentSessionTable.agentId, ids))
      .all()
      .map((session) => session.id)
    agentSessionService.notifyReadModelChange(sessionIds, 'projection')
  }
})

const topicTableAdapter = defineTable({
  table: topicTable,
  category: CHAT_CATEGORY,
  keys: ['id'],
  fields: [
    'id',
    'name',
    'isNameManuallyEdited',
    'assistantId',
    'activeNodeId',
    'orderKey',
    'lastActivityAt',
    'createdAt',
    'updatedAt'
  ],
  project: (row, db) => {
    if (hasPendingChatMessage(db, row.id) || (row.deletedAt === null && !hasPortableChatRoot(db, row.id))) {
      return null
    }
    return {
      id: row.id,
      name: row.name,
      isNameManuallyEdited: row.isNameManuallyEdited,
      assistantId: row.assistantId,
      activeNodeId: row.activeNodeId,
      orderKey: row.orderKey,
      lastActivityAt: row.lastActivityAt,
      createdAt: row.createdAt,
      updatedAt: row.updatedAt
    }
  },
  validate: (payload) => PortableTopicSchema.parse(payload),
  write: (tx, key, row, change) =>
    topicService.applySelfHostedSyncTx(tx, key.id, row as Parameters<typeof topicService.applySelfHostedSyncTx>[2], {
      deletedAt: change.deleted ? change.deletedAt : change.updatedAt,
      updatedAt: change.updatedAt
    }),
  afterCommit: (changes) => {
    const ids = [...new Set(changes.map((change) => change.entityId))]
    topicService.notifyReadModelChange(ids, 'membership')
  }
})

const messageTableAdapter = defineTable({
  table: messageTable,
  category: CHAT_CATEGORY,
  keys: ['id'],
  fields: [
    'id',
    'topicId',
    'parentId',
    'role',
    'data',
    'status',
    'siblingsGroupId',
    'modelId',
    'messageSnapshot',
    'stats',
    'createdAt',
    'updatedAt'
  ],
  project: (row, db) => {
    if (row.status === 'pending' || hasPendingChatMessage(db, row.topicId)) return null
    return portableChatMessage(row, db)
  },
  validate: (payload) => PortableMessageSchema.parse(payload),
  materialize: (payload, _existing, tx) => {
    const message = PortableMessageSchema.parse(payload)
    const data = materializeMessageData(message.data, tx)
    return data === false ? false : { ...message, data }
  },
  write: (tx, key, row, change) =>
    messageService.applySelfHostedSyncTx(
      tx,
      key.id,
      row as Parameters<typeof messageService.applySelfHostedSyncTx>[2],
      { deletedAt: change.deleted ? change.deletedAt : change.updatedAt, updatedAt: change.updatedAt }
    ),
  afterCommit: (changes) => {
    const db = application.get('DbService').getDb()
    const topicIds = [
      ...new Set(
        changes.flatMap((change) => {
          if (!change.deleted) return [String(change.payload.topicId)]
          const row = db
            .select({ topicId: messageTable.topicId })
            .from(messageTable)
            .where(eq(messageTable.id, change.entityId))
            .get()
          return row ? [row.topicId] : []
        })
      )
    ]
    topicService.notifyReadModelChange(topicIds, 'projection')
    notifyDataApiDataChange([
      { endpoint: '/topics/:topicId/messages', kind: 'membership' },
      { endpoint: '/topics/:topicId/tree' },
      { endpoint: '/messages/:id' }
    ])
  }
})

const applyMessageChange = messageTableAdapter.apply.bind(messageTableAdapter)
messageTableAdapter.apply = (tx, change) => {
  const remote = !change.deleted ? PortableMessageSchema.safeParse(change.payload) : undefined
  if (remote?.success && remote.data.role === 'root') {
    const currentRoot = tx
      .select({ id: messageTable.id })
      .from(messageTable)
      .where(
        and(
          eq(messageTable.topicId, remote.data.topicId),
          isNull(messageTable.parentId),
          isNull(messageTable.deletedAt)
        )
      )
      .get()
    if (currentRoot && currentRoot.id !== remote.data.id) {
      const child = tx
        .select({ id: messageTable.id })
        .from(messageTable)
        .where(eq(messageTable.parentId, currentRoot.id))
        .limit(1)
        .get()
      if (child) return 'deferred'
    }
  }
  return applyMessageChange(tx, change)
}

const agentSessionTableAdapter = defineTable({
  table: agentSessionTable,
  category: AGENT_CATEGORY,
  keys: ['id'],
  fields: [
    'id',
    'agentId',
    'model',
    'name',
    'isNameManuallyEdited',
    'description',
    'activeNodeId',
    'orderKey',
    'lastActivityAt',
    'createdAt',
    'updatedAt'
  ],
  project: (row, db) => {
    if (
      row.type !== 'conversation' ||
      row.ownerSessionId !== null ||
      row.taskId !== null ||
      row.isTaskReuseTarget ||
      hasPendingAgentSessionMessage(db, row.id) ||
      (row.deletedAt === null && !hasPortableAgentSessionRoot(db, row.id))
    ) {
      return null
    }
    return {
      id: row.id,
      agentId: portableAgentId(db, row.agentId),
      model: row.model,
      name: row.name,
      isNameManuallyEdited: row.isNameManuallyEdited,
      description: row.description,
      activeNodeId: row.activeNodeId,
      orderKey: row.orderKey,
      lastActivityAt: row.lastActivityAt,
      createdAt: row.createdAt,
      updatedAt: row.updatedAt
    }
  },
  validate: (payload) => PortableAgentSessionSchema.parse(payload),
  write: (tx, key, row, change) =>
    agentSessionService.applySelfHostedSyncTx(
      tx,
      key.id,
      row as Parameters<typeof agentSessionService.applySelfHostedSyncTx>[2],
      { deletedAt: change.deleted ? change.deletedAt : change.updatedAt, updatedAt: change.updatedAt }
    ),
  afterCommit: (changes) => {
    agentSessionService.notifyReadModelChange([...new Set(changes.map((change) => change.entityId))], 'membership')
  }
})

function hasPendingAgentSessionMessage(db: DbOrTx, sessionId: string): boolean {
  return Boolean(
    db
      .select({ id: agentSessionMessageTable.id })
      .from(agentSessionMessageTable)
      .where(and(eq(agentSessionMessageTable.sessionId, sessionId), eq(agentSessionMessageTable.status, 'pending')))
      .limit(1)
      .get()
  )
}

function portableAgentSessionMessage(
  row: typeof agentSessionMessageTable.$inferSelect,
  db: DbOrTx
): z.infer<typeof PortableAgentSessionMessageSchema> | null {
  if (
    row.status === 'pending' ||
    row.runtimeResumeToken !== null ||
    row.inputContextId !== null ||
    row.nativeInputId !== null ||
    row.replyId !== null ||
    row.delivery !== null ||
    row.deliveryStatus !== null ||
    row.deliveryTurnRef !== null ||
    row.deliveryInReplyTo !== null ||
    row.deliverySenderSessionId !== null
  ) {
    return null
  }
  const status = PortableSessionMessageStatusSchema.safeParse(row.status)
  if (!status.success) return null
  try {
    return PortableAgentSessionMessageSchema.parse({
      id: row.id,
      sessionId: row.sessionId,
      role: row.role,
      parentId: row.parentId,
      data: portableMessageData(row.data, db),
      status: status.data,
      modelId: row.modelId,
      messageSnapshot: portableSnapshot(row.messageSnapshot),
      stats: portableStats(row.stats),
      createdAt: row.createdAt,
      updatedAt: row.updatedAt
    })
  } catch {
    return null
  }
}

function hasPortableAgentSessionRoot(db: DbOrTx, sessionId: string): boolean {
  const root = db
    .select()
    .from(agentSessionMessageTable)
    .where(
      and(
        eq(agentSessionMessageTable.sessionId, sessionId),
        eq(agentSessionMessageTable.role, 'root'),
        isNull(agentSessionMessageTable.parentId)
      )
    )
    .limit(1)
    .get()
  return Boolean(root && portableAgentSessionMessage(root, db))
}

const agentSessionMessageTableAdapter = defineTable({
  table: agentSessionMessageTable,
  category: AGENT_CATEGORY,
  keys: ['id'],
  fields: [
    'id',
    'sessionId',
    'role',
    'parentId',
    'data',
    'status',
    'modelId',
    'messageSnapshot',
    'stats',
    'createdAt',
    'updatedAt'
  ],
  project: (row, db) => {
    const session = db
      .select({
        type: agentSessionTable.type,
        ownerSessionId: agentSessionTable.ownerSessionId,
        taskId: agentSessionTable.taskId,
        isTaskReuseTarget: agentSessionTable.isTaskReuseTarget,
        deletedAt: agentSessionTable.deletedAt
      })
      .from(agentSessionTable)
      .where(eq(agentSessionTable.id, row.sessionId))
      .get()
    if (
      !session ||
      session.type !== 'conversation' ||
      session.ownerSessionId !== null ||
      session.taskId !== null ||
      session.isTaskReuseTarget ||
      session.deletedAt !== null ||
      hasPendingAgentSessionMessage(db, row.sessionId)
    ) {
      return null
    }
    return portableAgentSessionMessage(row, db)
  },
  validate: (payload) => PortableAgentSessionMessageSchema.parse(payload),
  materialize: (payload, _existing, tx) => {
    const message = PortableAgentSessionMessageSchema.parse(payload)
    const data = materializeMessageData(message.data, tx)
    return data === false ? false : { ...message, data }
  },
  write: (tx, key, row) =>
    agentSessionMessageService.applySelfHostedSyncTx(
      tx,
      key.id,
      row as Parameters<typeof agentSessionMessageService.applySelfHostedSyncTx>[2]
    ),
  afterCommit: () => {
    notifyDataApiDataChange([
      { endpoint: '/agent-sessions', kind: 'projection' },
      { endpoint: '/agent-sessions', kind: 'order', dimension: 'lastActivityAt' },
      { endpoint: '/agent-sessions/:sessionId/messages', kind: 'membership' },
      { endpoint: '/agent-sessions/:sessionId/messages/:messageId' }
    ])
  }
})

const applyAgentSessionMessageChange = agentSessionMessageTableAdapter.apply.bind(agentSessionMessageTableAdapter)
agentSessionMessageTableAdapter.apply = (tx, change) => {
  const remote = !change.deleted ? PortableAgentSessionMessageSchema.safeParse(change.payload) : undefined
  if (remote?.success && remote.data.role === 'root') {
    const currentRoot = tx
      .select({ id: agentSessionMessageTable.id })
      .from(agentSessionMessageTable)
      .where(
        and(eq(agentSessionMessageTable.sessionId, remote.data.sessionId), eq(agentSessionMessageTable.role, 'root'))
      )
      .get()
    if (currentRoot && currentRoot.id !== remote.data.id) {
      const child = tx
        .select({ id: agentSessionMessageTable.id })
        .from(agentSessionMessageTable)
        .where(eq(agentSessionMessageTable.parentId, currentRoot.id))
        .limit(1)
        .get()
      if (child) return 'deferred'
    }
  }
  return applyAgentSessionMessageChange(tx, change)
}

function skillEntityId(agentId: string, contentHash: string): string {
  return JSON.stringify([agentId, contentHash])
}

function parseSkillEntityId(id: string): { agentId: string; contentHash: string } {
  let value: unknown
  try {
    value = JSON.parse(id)
  } catch {
    throw new Error('Invalid synced agent-skill identity')
  }
  if (
    !Array.isArray(value) ||
    value.length !== 2 ||
    typeof value[0] !== 'string' ||
    typeof value[1] !== 'string' ||
    !value[0] ||
    !value[1] ||
    JSON.stringify(value) !== id
  ) {
    throw new Error('Invalid synced agent-skill identity')
  }
  return { agentId: value[0], contentHash: value[1] }
}

function skillRows(db: DbOrTx, agentId: string, contentHash: string) {
  return db
    .select({
      agentId: agentSkillTable.agentId,
      skillId: agentSkillTable.skillId,
      isEnabled: agentSkillTable.isEnabled,
      createdAt: agentSkillTable.createdAt,
      updatedAt: agentSkillTable.updatedAt,
      contentHash: agentGlobalSkillTable.contentHash,
      agentDeletedAt: agentTable.deletedAt,
      agentConfiguration: agentTable.configuration
    })
    .from(agentSkillTable)
    .innerJoin(agentGlobalSkillTable, eq(agentSkillTable.skillId, agentGlobalSkillTable.id))
    .innerJoin(agentTable, eq(agentSkillTable.agentId, agentTable.id))
    .where(and(eq(agentSkillTable.agentId, agentId), eq(agentGlobalSkillTable.contentHash, contentHash)))
    .all()
}

function describeAgentSkill(db: DbOrTx, agentId: string, contentHash: string): LocalEntity | undefined {
  const id = skillEntityId(agentId, contentHash)
  const matches = skillRows(db, agentId, contentHash)
  if (matches.length === 0) return undefined
  if (
    matches.length !== 1 ||
    matches[0].agentDeletedAt !== null ||
    isBuiltinAgentRow({ id: agentId, configuration: matches[0].agentConfiguration })
  ) {
    return { id, payload: null, withheld: true }
  }
  const skillId = agentGlobalSkillService.findUniqueInstalledSkillIdByContentHashTx(db, contentHash)
  if (!skillId || skillId !== matches[0].skillId) return { id, payload: null, withheld: true }
  const payload = PortableAgentSkillSchema.parse({
    agentId,
    contentHash,
    isEnabled: matches[0].isEnabled,
    createdAt: matches[0].createdAt,
    updatedAt: matches[0].updatedAt
  })
  return { id, payload, updatedAt: matches[0].updatedAt }
}

const agentSkillEntity: EntityAdapter = {
  type: 'agent_skill',
  category: AGENT_CATEGORY,
  scan(db) {
    const rows = db
      .select({ agentId: agentSkillTable.agentId, contentHash: agentGlobalSkillTable.contentHash })
      .from(agentSkillTable)
      .innerJoin(agentGlobalSkillTable, eq(agentSkillTable.skillId, agentGlobalSkillTable.id))
      .all()
    const ids = new Map(rows.map((row) => [skillEntityId(row.agentId, row.contentHash), row]))
    return [...ids.values()].flatMap((row) => {
      const entity = describeAgentSkill(db, row.agentId, row.contentHash)
      return entity ? [entity] : []
    })
  },
  get(db, id) {
    const identity = parseSkillEntityId(id)
    return describeAgentSkill(db, identity.agentId, identity.contentHash)
  },
  apply(tx, change) {
    if (change.entityType !== this.type || change.category !== this.category) throw new Error('Unexpected sync entity')
    const identity = parseSkillEntityId(change.entityId)
    const agent = tx
      .select({ id: agentTable.id, configuration: agentTable.configuration, deletedAt: agentTable.deletedAt })
      .from(agentTable)
      .where(eq(agentTable.id, identity.agentId))
      .get()
    if (agent && isBuiltinAgentRow(agent)) return 'applied'
    if (!agent) return change.deleted ? 'applied' : 'deferred'
    if (agent.deletedAt !== null || hasPendingAgentMessage(tx, identity.agentId)) return 'deferred'

    if (change.deleted) {
      const matches = skillRows(tx, identity.agentId, identity.contentHash)
      if (matches.length === 0) return 'applied'
      if (matches.length !== 1) return 'deferred'
      agentGlobalSkillService.deleteSyncedJoinTx(tx, identity.agentId, matches[0].skillId)
      return 'applied'
    }

    const payload = PortableAgentSkillSchema.parse(change.payload)
    if (skillEntityId(payload.agentId, payload.contentHash) !== change.entityId) {
      throw new Error('Invalid synced agent-skill identity')
    }
    const skillId = agentGlobalSkillService.findUniqueInstalledSkillIdByContentHashTx(tx, payload.contentHash)
    if (!skillId) return 'deferred'
    agentGlobalSkillService.applySyncedJoinTx(tx, {
      agentId: payload.agentId,
      skillId,
      isEnabled: payload.isEnabled,
      createdAt: payload.createdAt,
      updatedAt: payload.updatedAt
    })
    return 'applied'
  },
  afterCommit(changes) {
    const db = application.get('DbService').getDb()
    for (const change of changes) {
      const identity = change.deleted
        ? parseSkillEntityId(change.entityId)
        : PortableAgentSkillSchema.parse(change.payload)
      const skillId = agentGlobalSkillService.findUniqueInstalledSkillIdByContentHashTx(db, identity.contentHash)
      if (skillId) agentGlobalSkillService.notifyAgentSkillChange(skillId)
    }
  }
}

export const conversationEntities: EntityAdapter[] = [
  agentTableAdapter,
  agentSkillEntity,
  topicTableAdapter,
  agentSessionTableAdapter,
  messageTableAdapter,
  agentSessionMessageTableAdapter
]
