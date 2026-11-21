import { and, eq, isNull, sql } from 'drizzle-orm'
import type { SQLiteColumn, SQLiteTable } from 'drizzle-orm/sqlite-core'
import { isEqual } from 'es-toolkit/compat'
import * as z from 'zod'

import { application } from '@application'
import { notifyDataApiDataChange } from '@data/dataApiDataChange'
import { agentTable } from '@data/db/schemas/agent'
import { agentSessionTable } from '@data/db/schemas/agentSession'
import { assistantTable } from '@data/db/schemas/assistant'
import {
  agentKnowledgeBaseTable,
  agentMcpServerTable,
  assistantKnowledgeBaseTable,
  assistantMcpServerTable
} from '@data/db/schemas/assistantRelations'
import { groupTable } from '@data/db/schemas/group'
import { knowledgeBaseTable, knowledgeItemTable } from '@data/db/schemas/knowledge'
import { mcpServerTable } from '@data/db/schemas/mcpServer'
import { miniAppTable } from '@data/db/schemas/miniApp'
import { paintingTable } from '@data/db/schemas/painting'
import { pinTable } from '@data/db/schemas/pin'
import { preferenceTable } from '@data/db/schemas/preference'
import { promptBindingTable, promptTable } from '@data/db/schemas/prompt'
import { entityTagTable, tagTable } from '@data/db/schemas/tagging'
import { topicTable } from '@data/db/schemas/topic'
import { translateHistoryTable } from '@data/db/schemas/translateHistory'
import { translateLanguageTable } from '@data/db/schemas/translateLanguage'
import { userModelTable } from '@data/db/schemas/userModel'
import { userProviderTable } from '@data/db/schemas/userProvider'
import type { DbOrTx } from '@data/db/types'
import { agentService } from '@data/services/AgentService'
import { assistantDataService } from '@data/services/AssistantService'
import { groupService } from '@data/services/GroupService'
import { mcpServerService } from '@data/services/McpServerService'
import { miniAppService } from '@data/services/MiniAppService'
import { modelService } from '@data/services/ModelService'
import { paintingService } from '@data/services/PaintingService'
import { pinService } from '@data/services/PinService'
import { promptService } from '@data/services/PromptService'
import { providerService } from '@data/services/ProviderService'
import { tagService } from '@data/services/TagService'
import { translateHistoryService } from '@data/services/TranslateHistoryService'
import { translateLanguageService } from '@data/services/TranslateLanguageService'
import { CHERRY_SUPPORT_AGENT_ID } from '@shared/ai/builtinAgent'
import type { DataApiDataChangeEffect } from '@shared/data/api/types'
import { DefaultPreferences } from '@shared/data/preference/preferenceSchemas'
import type { PreferenceKeyType } from '@shared/data/preference/preferenceTypes'
import { isManagedCherryProviderId } from '@shared/data/presets/cherryai'
import { AssistantSettingsSchema } from '@shared/data/types/assistant'
import { EntityIdSchema, EntityTypeSchema } from '@shared/data/types/entityType'
import {
  ParameterSupportDbSchema,
  ReasoningConfigSchema,
  RuntimeModelPricingSchema,
  UniqueModelIdSchema
} from '@shared/data/types/model'
import { ApiKeyEntrySchema, EndpointConfigOverrideSchema, ProviderSettingsSchema } from '@shared/data/types/provider'
import { TranslateHistoryKindSchema, TranslateLanguageSchema } from '@shared/data/types/translate'

import { defineTable, type EntityAdapter, type Payload, type SyncChange } from './entity'
import { hasTranslationTarget } from './translationFiles'

type CollectionEndpoint = Extract<DataApiDataChangeEffect, { kind: 'projection' }>['endpoint']

const preferenceSchemas = {
  'agent.input.suggestions.enabled': z.boolean(),
  'agent.session.display_mode': z.enum(['time', 'agent', 'workdir']),
  'agent.session.position': z.enum(['left', 'right']),
  'app.language': z
    .enum([
      'zh-CN',
      'zh-TW',
      'cs-CZ',
      'de-DE',
      'el-GR',
      'en-US',
      'es-ES',
      'fr-FR',
      'it-IT',
      'ja-JP',
      'ko-KR',
      'pl-PL',
      'pt-PT',
      'ro-RO',
      'ru-RU',
      'th-TH',
      'tr-TR',
      'vi-VN'
    ])
    .nullable(),
  'app.zoom_factor': z.number().min(0.5).max(3),
  'chat.code.show_line_numbers': z.boolean(),
  'chat.code.wrappable': z.boolean(),
  'chat.message.font_size': z.number().finite(),
  'chat.message.style': z.enum(['plain', 'bubble']),
  'chat.narrow_mode': z.boolean(),
  'topic.tab.display_mode': z.enum(['time', 'assistant']),
  'topic.tab.position': z.enum(['left', 'right']),
  'topic.tab.show': z.boolean(),
  'ui.navbar.position': z.enum(['left', 'top']),
  'ui.theme_mode': z.enum(['light', 'dark', 'system'])
} satisfies Partial<Record<PreferenceKeyType, z.ZodType>>

const preferenceKeys = new Set(Object.keys(preferenceSchemas))
const portableDefaults = Object.fromEntries(
  [...preferenceKeys].map((key) => [key, DefaultPreferences.default[key as PreferenceKeyType]])
)

function notify(endpoint: CollectionEndpoint): void {
  notifyDataApiDataChange([{ endpoint, kind: 'membership' }])
}

function notifyAfterCommit(endpoint: CollectionEndpoint): (changes: SyncChange[]) => void {
  return (changes) => {
    if (changes.length) notify(endpoint)
  }
}

function syncTimes(change: SyncChange) {
  return {
    updatedAt: change.updatedAt,
    deletedAt: change.deleted ? change.deletedAt : undefined
  }
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
}

function isPortableUrl(value: string): boolean {
  try {
    const url = new URL(value)
    return ['http:', 'https:'].includes(url.protocol) && !url.username && !url.password && !url.search && !url.hash
  } catch {
    return false
  }
}

function hasRow<T extends SQLiteTable>(tx: DbOrTx, table: T, column: SQLiteColumn, value: string): boolean {
  return Boolean(
    tx
      .select({ one: sql<number>`1` })
      .from(table)
      .where(eq(column, value))
      .limit(1)
      .get()
  )
}

function hasPortableTarget(tx: DbOrTx, entityType: string, entityId: string): boolean {
  if (!EntityTypeSchema.safeParse(entityType).success || !EntityIdSchema.safeParse(entityId).success) return false
  switch (entityType) {
    case 'assistant':
      return Boolean(
        tx
          .select({ id: assistantTable.id })
          .from(assistantTable)
          .where(and(eq(assistantTable.id, entityId), isNull(assistantTable.deletedAt)))
          .limit(1)
          .get()
      )
    case 'topic':
      return Boolean(
        tx
          .select({ id: topicTable.id })
          .from(topicTable)
          .where(and(eq(topicTable.id, entityId), isNull(topicTable.deletedAt)))
          .limit(1)
          .get()
      )
    case 'model':
      return hasRow(tx, userModelTable, userModelTable.id, entityId)
    case 'agent':
      return hasPortableAgent(tx, entityId)
    case 'session':
      return Boolean(
        tx
          .select({ id: agentSessionTable.id })
          .from(agentSessionTable)
          .where(
            and(
              eq(agentSessionTable.id, entityId),
              eq(agentSessionTable.type, 'conversation'),
              isNull(agentSessionTable.ownerSessionId),
              isNull(agentSessionTable.taskId),
              eq(agentSessionTable.isTaskReuseTarget, false),
              isNull(agentSessionTable.deletedAt)
            )
          )
          .limit(1)
          .get()
      )
    case 'knowledge':
      return (
        hasRow(tx, knowledgeBaseTable, knowledgeBaseTable.id, entityId) ||
        hasRow(tx, knowledgeItemTable, knowledgeItemTable.id, entityId)
      )
    default:
      return false
  }
}

function hasPortableAgent(tx: DbOrTx, id: string): boolean {
  if (id === CHERRY_SUPPORT_AGENT_ID) return false
  const row = tx
    .select({ id: agentTable.id, configuration: agentTable.configuration })
    .from(agentTable)
    .where(and(eq(agentTable.id, id), isNull(agentTable.deletedAt)))
    .limit(1)
    .get()
  return Boolean(row && !(isRecord(row.configuration) && 'builtin_role' in row.configuration))
}

function safeProviderSettings(value: unknown): Record<string, unknown> | null | undefined {
  if (value == null) return value
  if (!isRecord(value)) return undefined
  const safeKeys = [
    'billing',
    'streamOptions',
    'apiVersion',
    'cacheControl',
    'keepAliveTime',
    'rateLimit',
    'timeout',
    'notes'
  ] as const
  const candidate = Object.fromEntries(safeKeys.filter((key) => key in value).map((key) => [key, value[key]]))
  const parsed = ProviderSettingsSchema.safeParse(candidate)
  return parsed.success ? parsed.data : undefined
}

function safeEndpointConfigs(value: unknown): Record<string, unknown> | null | undefined {
  if (value == null) return value
  if (!isRecord(value)) return undefined
  const result: Record<string, unknown> = {}
  for (const [type, entry] of Object.entries(value)) {
    const parsed = EndpointConfigOverrideSchema.safeParse(entry)
    if (!parsed.success || (parsed.data.baseUrl && !isPortableUrl(parsed.data.baseUrl))) continue
    result[type] = parsed.data
  }
  return result
}

const providerFields = [
  'providerId',
  'apiKeys',
  'presetProviderId',
  'name',
  'logoKey',
  'endpointConfigs',
  'defaultChatEndpoint',
  'providerSettings',
  'isEnabled',
  'orderKey',
  'createdAt'
] as const

const providerSchema = z.object({
  apiKeys: z.array(ApiKeyEntrySchema).nullable(),
  providerId: z.string().min(1),
  presetProviderId: z.string().nullable(),
  name: z.string().min(1),
  logoKey: z.string().nullable(),
  endpointConfigs: z.record(z.string(), z.unknown()).nullable(),
  defaultChatEndpoint: z.string().nullable(),
  providerSettings: z.record(z.string(), z.unknown()).nullable(),
  isEnabled: z.boolean(),
  orderKey: z.string().min(1),
  createdAt: z.number().int().nonnegative()
})

const modelFields = [
  'id',
  'providerId',
  'modelId',
  'presetModelId',
  'name',
  'description',
  'group',
  'capabilities',
  'inputModalities',
  'inputModalitiesExplicit',
  'outputModalities',
  'endpointTypes',
  'contextWindow',
  'maxInputTokens',
  'maxOutputTokens',
  'supportsStreaming',
  'reasoning',
  'parameters',
  'pricing',
  'isEnabled',
  'isHidden',
  'orderKey',
  'notes',
  'createdAt'
] as const

const modelSchema = z.object({
  id: UniqueModelIdSchema,
  providerId: z.string().min(1),
  modelId: z.string().min(1),
  presetModelId: z.string().nullable(),
  name: z.string().nullable(),
  description: z.string().nullable(),
  group: z.string().nullable(),
  capabilities: z.array(z.string()).nullable(),
  inputModalities: z.array(z.string()).nullable(),
  inputModalitiesExplicit: z.boolean(),
  outputModalities: z.array(z.string()).nullable(),
  endpointTypes: z.array(z.string()).nullable(),
  contextWindow: z.number().int().nullable(),
  maxInputTokens: z.number().int().nullable(),
  maxOutputTokens: z.number().int().nullable(),
  supportsStreaming: z.boolean().nullable(),
  reasoning: ReasoningConfigSchema.nullable(),
  parameters: ParameterSupportDbSchema.nullable(),
  pricing: RuntimeModelPricingSchema.nullable(),
  isEnabled: z.boolean(),
  isHidden: z.boolean(),
  orderKey: z.string().min(1),
  notes: z.string().nullable(),
  createdAt: z.number().int().nonnegative()
})

const groupFields = ['id', 'entityType', 'name', 'orderKey', 'createdAt'] as const
const tagFields = ['id', 'name', 'color', 'createdAt'] as const
const pinFields = ['id', 'entityType', 'entityId', 'orderKey', 'createdAt'] as const
const entityTagFields = ['entityType', 'entityId', 'tagId', 'createdAt'] as const

const mcpFields = [
  'id',
  'name',
  'type',
  'description',
  'baseUrl',
  'registryUrl',
  'provider',
  'providerUrl',
  'logoUrl',
  'tags',
  'longRunning',
  'timeout',
  'reference',
  'searchKey',
  'disabledTools',
  'disabledAutoApproveTools',
  'sortOrder',
  'createdAt'
] as const

const mcpSchema = z.object({
  id: z.string().uuid(),
  name: z.string().min(1),
  type: z.enum(['sse', 'streamableHttp']),
  description: z.string().nullable(),
  baseUrl: z.string().url(),
  registryUrl: z.string().nullable(),
  provider: z.string().nullable(),
  providerUrl: z.string().nullable(),
  logoUrl: z.string().nullable(),
  tags: z.array(z.string()).nullable(),
  longRunning: z.boolean().nullable(),
  timeout: z.number().int().nullable(),
  reference: z.string().nullable(),
  searchKey: z.string().nullable(),
  disabledTools: z.array(z.string()).nullable(),
  disabledAutoApproveTools: z.array(z.string()).nullable(),
  sortOrder: z.number().int().nullable(),
  createdAt: z.number().int().nonnegative()
})

const assistantFields = [
  'id',
  'name',
  'prompt',
  'emoji',
  'description',
  'modelId',
  'groupId',
  'settings',
  'orderKey',
  'createdAt'
] as const

const assistantSchema = z.object({
  id: z.string().uuid(),
  name: z.string().min(1),
  prompt: z.string(),
  emoji: z.string(),
  description: z.string(),
  modelId: z.string().nullable(),
  groupId: z.string().nullable(),
  settings: AssistantSettingsSchema,
  orderKey: z.string().min(1),
  createdAt: z.number().int().nonnegative()
})

const assistantRelationFields = ['assistantId', 'mcpServerId', 'createdAt'] as const
const assistantKnowledgeFields = ['assistantId', 'knowledgeBaseId', 'createdAt'] as const
const agentRelationFields = ['agentId', 'mcpServerId', 'createdAt'] as const
const agentKnowledgeFields = ['agentId', 'knowledgeBaseId', 'createdAt'] as const
const promptFields = ['id', 'title', 'content', 'visibility', 'orderKey', 'createdAt'] as const
const promptBindingFields = ['promptId', 'targetType', 'targetId', 'orderKey', 'createdAt'] as const

const miniAppFields = ['appId', 'kind', 'name', 'url', 'logoKey', 'status', 'orderKey', 'createdAt'] as const
const paintingFields = ['id', 'providerId', 'modelId', 'prompt', 'orderKey', 'createdAt'] as const
const translateLanguageFields = ['langCode', 'value', 'emoji', 'createdAt'] as const
const translateHistoryFields = [
  'id',
  'kind',
  'sourceText',
  'targetText',
  'sourceLanguage',
  'targetLanguage',
  'star',
  'createdAt'
] as const

const preferenceFields = ['scope', 'key', 'value'] as const

function validatePreference(payload: Payload): Payload {
  const key = payload.key
  if (payload.scope !== 'default' || typeof key !== 'string' || !preferenceKeys.has(key)) {
    throw new Error('Unsupported synced preference')
  }
  preferenceSchemas[key as keyof typeof preferenceSchemas].parse(payload.value)
  return payload
}

function preferenceEntityId(id: string): string {
  const value: unknown = JSON.parse(id)
  if (
    !Array.isArray(value) ||
    value.length !== 2 ||
    value[0] !== 'default' ||
    typeof value[1] !== 'string' ||
    !preferenceKeys.has(value[1])
  ) {
    throw new Error('Invalid preference identity')
  }
  return value[1]
}

function preferenceAfterCommit(changes: SyncChange[]): void {
  const updates = changes.map((change) => {
    const key = preferenceEntityId(change.entityId) as PreferenceKeyType
    return {
      key,
      value: change.deleted ? portableDefaults[key] : change.payload.value
    }
  })
  if (updates.length) application.get('PreferenceService').applySyncedValues(updates)
}

function isPortableFilename(value: unknown): boolean {
  return (
    typeof value === 'string' &&
    value.length > 0 &&
    value !== '.' &&
    value !== '..' &&
    !/[\\/:]/.test(value) &&
    !value.includes('\0')
  )
}

function partitionTopicLinks(base: EntityAdapter): EntityAdapter[] {
  const topicType = (
    { group: 'topic_group', pin: 'topic_pin', entity_tag: 'topic_tag_link' } as Record<string, string>
  )[base.type]
  if (!topicType) return [base]
  return [false, true].map((topic) => {
    const type = topic ? topicType : base.type
    const category = topic ? 'chats' : 'other'
    const belongs = (payload: Payload) => (payload.entityType === 'topic') === topic
    const project = (local: ReturnType<EntityAdapter['scan']>[number]) =>
      !local.payload || belongs(local.payload) ? local : { id: local.id, payload: null, withheld: true }
    return {
      ...base,
      type,
      category,
      scan: (db) => base.scan(db).map(project),
      get(db, id) {
        const local = base.get(db, id)
        return local && project(local)
      },
      apply(tx, change) {
        if (change.category !== category || change.entityType !== type || (!change.deleted && !belongs(change.payload)))
          throw new Error('Unexpected link category.')
        const local = base.get(tx, change.entityId)
        if (local?.payload && !belongs(local.payload)) return 'deferred'
        return base.apply(tx, { ...change, category: base.category, entityType: base.type })
      }
    }
  })
}

export function createLibraryEntities(
  options: { includeAgentBindings?: () => boolean; includeFiles?: () => boolean } = {}
): EntityAdapter[] {
  const agentBindingsEnabled = options.includeAgentBindings ?? (() => true)
  const includeFiles = options.includeFiles ?? (() => true)

  return [
    defineTable({
      table: groupTable,
      category: 'other',
      keys: ['id'],
      fields: groupFields,
      project: (row) =>
        row.entityType === 'workspace' ? null : Object.fromEntries(groupFields.map((key) => [key, row[key]])),
      validate(payload) {
        if (payload.entityType === 'workspace') throw new Error('Workspace groups are not portable')
        EntityTypeSchema.parse(payload.entityType)
        z.string().uuid().parse(payload.id)
        z.string().min(1).parse(payload.name)
        z.string().min(1).parse(payload.orderKey)
        return payload
      },
      write: (tx, key, row, change) => groupService.applySelfHostedSyncTx(tx, key.id, row, syncTimes(change)),
      afterCommit: notifyAfterCommit('/groups')
    }),
    defineTable({
      table: pinTable,
      category: 'other',
      keys: ['id'],
      fields: pinFields,
      project: (row, db) =>
        row.entityType === 'workspace' || !hasPortableTarget(db, row.entityType, row.entityId)
          ? null
          : Object.fromEntries(pinFields.map((key) => [key, row[key]])),
      validate(payload) {
        EntityTypeSchema.parse(payload.entityType)
        EntityIdSchema.parse(payload.entityId)
        z.string().uuid().parse(payload.id)
        z.string().min(1).parse(payload.orderKey)
        return payload
      },
      materialize(payload, _existing, tx) {
        return payload.entityType === 'workspace' ||
          !hasPortableTarget(tx, String(payload.entityType), String(payload.entityId))
          ? false
          : payload
      },
      write: (tx, key, row, change) => pinService.applySelfHostedSyncTx(tx, key.id, row, syncTimes(change)),
      afterCommit: notifyAfterCommit('/pins')
    }),
    defineTable({
      table: tagTable,
      category: 'other',
      keys: ['id'],
      fields: tagFields,
      validate(payload) {
        z.string().uuid().parse(payload.id)
        z.string().min(1).parse(payload.name)
        if (payload.color !== null && (typeof payload.color !== 'string' || !/^#[0-9a-f]{6}$/i.test(payload.color))) {
          throw new Error('Invalid tag color')
        }
        return payload
      },
      write: (tx, key, row, change) => tagService.applySelfHostedSyncTagTx(tx, key.id, row, syncTimes(change)),
      afterCommit: notifyAfterCommit('/tags')
    }),
    defineTable({
      table: entityTagTable,
      category: 'other',
      keys: ['entityType', 'entityId', 'tagId'],
      fields: entityTagFields,
      project: (row, db) =>
        row.entityType === 'workspace' || !hasPortableTarget(db, row.entityType, row.entityId)
          ? null
          : Object.fromEntries(entityTagFields.map((key) => [key, row[key]])),
      validate(payload) {
        EntityTypeSchema.parse(payload.entityType)
        EntityIdSchema.parse(payload.entityId)
        z.string().uuid().parse(payload.tagId)
        return payload
      },
      materialize(payload, _existing, tx) {
        return payload.entityType === 'workspace' ||
          !hasPortableTarget(tx, String(payload.entityType), String(payload.entityId))
          ? false
          : payload
      },
      write: (tx, key, row, change) =>
        tagService.applySelfHostedSyncEntityTagTx(
          tx,
          { entityType: key.entityType, entityId: key.entityId, tagId: key.tagId },
          row,
          syncTimes(change)
        ),
      afterCommit: notifyAfterCommit('/tags')
    }),
    defineTable({
      table: userProviderTable,
      category: 'providers',
      keys: ['providerId'],
      fields: providerFields,
      project(row) {
        if (isManagedCherryProviderId(row.providerId) || (row.authConfig && row.authConfig.type !== 'api-key'))
          return null
        const endpointConfigs = safeEndpointConfigs(row.endpointConfigs)
        const providerSettings = safeProviderSettings(row.providerSettings)
        if (endpointConfigs === undefined || providerSettings === undefined) return null
        return {
          providerId: row.providerId,
          apiKeys: row.apiKeys,
          presetProviderId: row.presetProviderId,
          name: row.name,
          logoKey: row.logoKey?.startsWith('icon:') ? row.logoKey : null,
          endpointConfigs,
          defaultChatEndpoint: row.defaultChatEndpoint,
          providerSettings,
          isEnabled: row.isEnabled,
          orderKey: row.orderKey,
          createdAt: row.createdAt
        }
      },
      validate(payload) {
        const parsed = providerSchema.parse(payload)
        if (isManagedCherryProviderId(parsed.providerId)) throw new Error('Managed provider accounts cannot be synced.')
        const endpointConfigs = safeEndpointConfigs(parsed.endpointConfigs)
        const providerSettings = safeProviderSettings(parsed.providerSettings)
        if (endpointConfigs === undefined || providerSettings === undefined)
          throw new Error('Invalid portable provider settings')
        if (parsed.logoKey !== null && !parsed.logoKey.startsWith('icon:')) throw new Error('Invalid provider logo key')
        return { ...parsed, endpointConfigs, providerSettings }
      },
      write: (tx, key, row, change) =>
        providerService.applySelfHostedSyncTx(tx, key.providerId, row, syncTimes(change)),
      afterCommit: notifyAfterCommit('/providers')
    }),
    defineTable({
      table: userModelTable,
      category: 'providers',
      keys: ['id'],
      fields: modelFields,
      validate(payload) {
        const parsed = modelSchema.parse(payload)
        if (parsed.id !== `${parsed.providerId}::${parsed.modelId}`) throw new Error('Invalid model identity')
        return parsed
      },
      write: (tx, key, row, change) => modelService.applySelfHostedSyncTx(tx, key.id, row, syncTimes(change)),
      afterCommit: notifyAfterCommit('/models')
    }),
    defineTable({
      table: mcpServerTable,
      category: 'mcp',
      keys: ['id'],
      fields: mcpFields,
      project(row) {
        if (
          !['sse', 'streamableHttp'].includes(row.type ?? '') ||
          !row.baseUrl ||
          !isPortableUrl(row.baseUrl) ||
          (row.registryUrl && !isPortableUrl(row.registryUrl)) ||
          (row.providerUrl && !isPortableUrl(row.providerUrl)) ||
          (row.logoUrl && !isPortableUrl(row.logoUrl))
        )
          return null
        return Object.fromEntries(mcpFields.map((key) => [key, row[key]]))
      },
      validate(payload) {
        const parsed = mcpSchema.parse(payload)
        if (!isPortableUrl(parsed.baseUrl)) throw new Error('Invalid MCP endpoint')
        for (const url of [parsed.registryUrl, parsed.providerUrl, parsed.logoUrl]) {
          if (url && !isPortableUrl(url)) throw new Error('Invalid MCP URL')
        }
        return parsed
      },
      materialize(payload) {
        return payload.type === 'sse' || payload.type === 'streamableHttp' ? payload : false
      },
      write: (tx, key, row, change) => mcpServerService.applySelfHostedSyncTx(tx, key.id, row, syncTimes(change)),
      afterCommit: notifyAfterCommit('/mcp-servers')
    }),
    defineTable({
      table: assistantTable,
      category: 'assistants',
      keys: ['id'],
      fields: assistantFields,
      validate: (payload) => assistantSchema.parse(payload),
      write: (tx, key, row, change) => assistantDataService.applySelfHostedSyncTx(tx, key.id, row, syncTimes(change)),
      afterCommit: notifyAfterCommit('/assistants')
    }),
    defineTable({
      table: assistantMcpServerTable,
      category: 'assistants',
      keys: ['assistantId', 'mcpServerId'],
      fields: assistantRelationFields,
      write: (tx, key, row, change) =>
        assistantDataService.applySelfHostedSyncMcpBindingTx(
          tx,
          key.assistantId,
          key.mcpServerId,
          row,
          syncTimes(change)
        ),
      afterCommit: notifyAfterCommit('/assistants')
    }),
    defineTable({
      table: assistantKnowledgeBaseTable,
      category: 'assistants',
      keys: ['assistantId', 'knowledgeBaseId'],
      fields: assistantKnowledgeFields,
      write: (tx, key, row, change) =>
        assistantDataService.applySelfHostedSyncKnowledgeBindingTx(
          tx,
          key.assistantId,
          key.knowledgeBaseId,
          row,
          syncTimes(change)
        ),
      afterCommit: notifyAfterCommit('/assistants')
    }),
    defineTable({
      table: agentMcpServerTable,
      category: 'agents',
      keys: ['agentId', 'mcpServerId'],
      fields: agentRelationFields,
      write: (tx, key, row, change) =>
        agentService.applySelfHostedSyncMcpBindingTx(tx, key.agentId, key.mcpServerId, row, syncTimes(change)),
      afterCommit: notifyAfterCommit('/agents')
    }),
    defineTable({
      table: agentKnowledgeBaseTable,
      category: 'agents',
      keys: ['agentId', 'knowledgeBaseId'],
      fields: agentKnowledgeFields,
      write: (tx, key, row, change) =>
        agentService.applySelfHostedSyncKnowledgeBindingTx(
          tx,
          key.agentId,
          key.knowledgeBaseId,
          row,
          syncTimes(change)
        ),
      afterCommit: notifyAfterCommit('/agents')
    }),
    defineTable({
      table: promptTable,
      category: 'assistants',
      keys: ['id'],
      fields: promptFields,
      validate(payload) {
        z.string().uuid().parse(payload.id)
        z.string().min(1).max(256).parse(payload.title)
        z.string().min(1).max(100_000).parse(payload.content)
        z.enum(['global', 'restricted']).parse(payload.visibility)
        z.string().min(1).parse(payload.orderKey)
        return payload
      },
      write: (tx, key, row, change) => promptService.applySelfHostedSyncTx(tx, key.id, row, syncTimes(change)),
      afterCommit: notifyAfterCommit('/prompts')
    }),
    defineTable({
      table: promptBindingTable,
      category: 'assistants',
      keys: ['promptId', 'targetType', 'targetId'],
      fields: promptBindingFields,
      project: (row, db) =>
        hasPromptTarget(db, row.targetType, row.targetId, agentBindingsEnabled())
          ? Object.fromEntries(promptBindingFields.map((key) => [key, row[key]]))
          : null,
      materialize: (payload, _existing, tx) =>
        hasPromptTarget(tx, String(payload.targetType), String(payload.targetId), agentBindingsEnabled())
          ? payload
          : false,
      validate(payload) {
        z.enum(['assistant', 'agent']).parse(payload.targetType)
        z.string().uuid().parse(payload.promptId)
        z.string().min(1).parse(payload.targetId)
        z.string().min(1).parse(payload.orderKey)
        return payload
      },
      write: (tx, key, row, change) =>
        promptService.applySelfHostedSyncBindingTx(
          tx,
          {
            promptId: key.promptId,
            targetType: key.targetType as 'assistant' | 'agent',
            targetId: key.targetId
          },
          row,
          syncTimes(change)
        ),
      afterCommit: notifyAfterCommit('/prompts')
    }),
    defineTable({
      table: preferenceTable,
      category: 'preferences',
      keys: ['scope', 'key'],
      fields: preferenceFields,
      absent: (row) =>
        row.scope === 'default' && preferenceKeys.has(row.key) && isEqual(row.value, portableDefaults[row.key]),
      project: (row) =>
        row.scope === 'default' && preferenceKeys.has(row.key)
          ? { scope: row.scope, key: row.key, value: row.value }
          : null,
      validate: validatePreference,
      write: (tx, key, row, change) =>
        application
          .get('PreferenceService')
          .applySelfHostedSyncTx(
            tx,
            key.scope,
            key.key,
            row ?? { scope: key.scope, key: key.key, value: portableDefaults[preferenceEntityId(change.entityId)] },
            syncTimes(change)
          ),
      afterCommit: preferenceAfterCommit
    }),
    defineTable({
      table: miniAppTable,
      category: 'other',
      keys: ['appId'],
      fields: miniAppFields,
      project(row) {
        if (row.kind !== 'site' || row.presetMiniAppId !== null || !isPortableUrl(row.url)) return null
        return {
          appId: row.appId,
          kind: 'site',
          name: row.name,
          url: row.url,
          logoKey: row.logoKey && /^[a-z0-9_-]+$/i.test(row.logoKey) ? row.logoKey : null,
          status: row.status,
          orderKey: row.orderKey,
          createdAt: row.createdAt
        }
      },
      validate(payload) {
        if (payload.kind !== 'site' || typeof payload.url !== 'string' || !isPortableUrl(payload.url)) {
          throw new Error('Only portable web mini apps can be synced')
        }
        z.string().min(1).parse(payload.appId)
        z.string().min(1).parse(payload.name)
        z.enum(['enabled', 'disabled', 'pinned']).parse(payload.status)
        z.string().min(1).parse(payload.orderKey)
        if (
          payload.logoKey !== null &&
          (typeof payload.logoKey !== 'string' || !/^[a-z0-9_-]+$/i.test(payload.logoKey))
        ) {
          throw new Error('Invalid mini app logo key')
        }
        return payload
      },
      materialize(payload, existing) {
        return existing?.presetMiniAppId != null || payload.kind !== 'site' ? false : payload
      },
      write: (tx, key, row, change) => miniAppService.applySelfHostedSyncTx(tx, key.appId, row, syncTimes(change)),
      afterCommit: notifyAfterCommit('/mini-apps')
    }),
    defineTable({
      table: paintingTable,
      category: 'other',
      keys: ['id'],
      fields: paintingFields,
      validate(payload) {
        z.string().uuid().parse(payload.id)
        z.string().min(1).parse(payload.providerId)
        if (payload.modelId !== null) z.string().min(1).parse(payload.modelId)
        z.string().min(1).parse(payload.prompt)
        z.string().min(1).parse(payload.orderKey)
        return payload
      },
      write: (tx, key, row, change) => paintingService.applySelfHostedSyncTx(tx, key.id, row, syncTimes(change)),
      afterCommit: notifyAfterCommit('/paintings')
    }),
    defineTable({
      table: translateLanguageTable,
      category: 'other',
      keys: ['langCode'],
      fields: translateLanguageFields,
      validate: (payload) => TranslateLanguageSchema.omit({ updatedAt: true }).parse(payload),
      write: (tx, key, row, change) =>
        translateLanguageService.applySelfHostedSyncTx(tx, key.langCode, row, syncTimes(change)),
      afterCommit: notifyAfterCommit('/translate/languages')
    }),
    defineTable({
      table: translateHistoryTable,
      category: 'other',
      keys: ['id'],
      fields: translateHistoryFields,
      project: (row, tx) =>
        row.kind === 'text' ||
        (row.kind === 'file' &&
          includeFiles() &&
          hasTranslationTarget(tx, row.id) &&
          [row.sourceText, row.targetText].every(isPortableFilename))
          ? Object.fromEntries(translateHistoryFields.map((key) => [key, row[key]]))
          : null,
      validate(payload) {
        if (
          TranslateHistoryKindSchema.parse(payload.kind) === 'file' &&
          ![payload.sourceText, payload.targetText].every(isPortableFilename)
        )
          throw new Error('File translation history must contain names, not paths.')
        z.string().uuid().parse(payload.id)
        z.string().min(1).parse(payload.sourceText)
        z.string().min(1).parse(payload.targetText)
        z.string().nullable().parse(payload.sourceLanguage)
        z.string().nullable().parse(payload.targetLanguage)
        z.boolean().parse(payload.star)
        return payload
      },
      materialize: (payload) => (payload.kind === 'text' || includeFiles() ? payload : false),
      write: (tx, key, row, change) =>
        translateHistoryService.applySelfHostedSyncTx(tx, key.id, row, syncTimes(change)),
      afterCommit: notifyAfterCommit('/translate/histories')
    })
  ].flatMap(partitionTopicLinks)
}

function hasPromptTarget(tx: DbOrTx, type: string, id: string, includeAgents: boolean): boolean {
  if (type === 'assistant') return hasPortableTarget(tx, type, id)
  if (type === 'agent') return includeAgents && hasPortableAgent(tx, id)
  return false
}

export const libraryEntities = createLibraryEntities()
