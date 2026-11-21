import { existsSync, lstatSync } from 'node:fs'
import path from 'node:path'

import { and, eq, inArray } from 'drizzle-orm'
import * as z from 'zod'

import { application } from '@application'
import { notifyDataApiDataChange } from '@data/dataApiDataChange'
import { knowledgeBaseTable, knowledgeItemTable } from '@data/db/schemas/knowledge'
import { noteTable } from '@data/db/schemas/note'
import { userModelTable } from '@data/db/schemas/userModel'
import { knowledgeBaseService } from '@data/services/KnowledgeBaseService'
import { knowledgeItemService } from '@data/services/KnowledgeItemService'
import { noteService } from '@data/services/NoteService'
import { getKnowledgeVectorStoreFilePathSync } from '@main/features/knowledge'
import {
  KNOWLEDGE_BASE_ERROR_MISSING_EMBEDDING_MODEL,
  KNOWLEDGE_BASE_ERROR_MISSING_VECTOR_STORE,
  KnowledgeBaseIdSchema,
  KnowledgeChunkOverlapSchema,
  KnowledgeChunkSeparatorSchema,
  KnowledgeChunkSizeSchema,
  KnowledgeChunkStrategySchema,
  KnowledgeThresholdSchema,
  NoteItemDataSchema,
  UrlItemDataSchema
} from '@shared/data/types/knowledge'
import { AbsoluteFilePathSchema } from '@shared/types/file'
import {
  PosixRelativeFilePathSchema,
  WindowsRelativeFilePathSchema,
  resolvePosixRelativeSegments,
  resolveWindowsRelativeSegments
} from '@shared/utils/file'

import { defineTable, type EntityAdapter } from './entity'

const CATEGORY = 'knowledge' as const
const MAX_NOTE_PATH_LENGTH = 500
const ACTIVE_ITEM_STATUSES = ['preparing', 'processing', 'reading', 'embedding', 'deleting'] as const

const PortableKnowledgeBaseSchema = z.strictObject({
  id: KnowledgeBaseIdSchema,
  name: z.string().trim().min(1),
  orderKey: z.string().min(1),
  groupId: z.uuidv4().nullable(),
  embeddingModelId: z.string().trim().min(1).nullable(),
  rerankModelId: z.string().nullable(),
  fileProcessorId: z.string().nullable(),
  chunkSize: KnowledgeChunkSizeSchema,
  chunkOverlap: KnowledgeChunkOverlapSchema,
  chunkStrategy: KnowledgeChunkStrategySchema,
  chunkSeparator: KnowledgeChunkSeparatorSchema,
  threshold: KnowledgeThresholdSchema.nullable(),
  createdAt: z.number().int().nonnegative(),
  updatedAt: z.number().int().nonnegative()
})

const PRIVATE_URL_QUERY_KEY =
  /(?:^|[-_])(?:access[_-]?token|api[_-]?key|auth(?:orization)?|client[_-]?secret|credential|jwt|key|password|passwd|secret|sig(?:nature)?|token)(?:$|[-_])|^(?:awsaccesskeyid|googleaccessid)$/i

function isPortableUrl(value: string): boolean {
  try {
    const url = new URL(value)
    const fragment = new URLSearchParams(url.hash.slice(1))
    return (
      (url.protocol === 'http:' || url.protocol === 'https:') &&
      !url.username &&
      !url.password &&
      ![...url.searchParams.keys(), ...fragment.keys()].some((key) => PRIVATE_URL_QUERY_KEY.test(key))
    )
  } catch {
    return false
  }
}

function isPortableSource(value: string): boolean {
  if (/^https?:/i.test(value)) return isPortableUrl(value)
  return (
    !AbsoluteFilePathSchema.safeParse(value).success &&
    !/^file:/i.test(value) &&
    !/^[a-z]:/i.test(value) &&
    !/[\\/]/.test(value)
  )
}

const PortableNoteItemDataSchema = NoteItemDataSchema.omit({ relativePath: true }).refine(
  (data) => isPortableSource(data.source),
  'Local paths are not portable'
)
const PortableUrlItemDataSchema = UrlItemDataSchema.omit({ relativePath: true }).refine(
  (data) => isPortableUrl(data.url) && isPortableSource(data.source),
  'URL credentials or local paths are not portable'
)

const PortableKnowledgeItemSchema = z.discriminatedUnion('type', [
  z.strictObject({
    id: z.uuidv7(),
    baseId: KnowledgeBaseIdSchema,
    groupId: z.uuidv7().nullable(),
    type: z.literal('note'),
    data: PortableNoteItemDataSchema,
    createdAt: z.number().int().nonnegative(),
    updatedAt: z.number().int().nonnegative()
  }),
  z.strictObject({
    id: z.uuidv7(),
    baseId: KnowledgeBaseIdSchema,
    groupId: z.uuidv7().nullable(),
    type: z.literal('url'),
    data: PortableUrlItemDataSchema,
    createdAt: z.number().int().nonnegative(),
    updatedAt: z.number().int().nonnegative()
  })
])

const PortableNoteMetadataSchema = z
  .strictObject({
    id: z.uuidv4(),
    path: z.string().max(MAX_NOTE_PATH_LENGTH),
    isStarred: z.boolean(),
    isExpanded: z.boolean(),
    createdAt: z.number().int().nonnegative(),
    updatedAt: z.number().int().nonnegative()
  })
  .refine((note) => note.isStarred || note.isExpanded, 'Empty note state is not portable')

function omitRelativePath(value: unknown): unknown {
  if (!value || typeof value !== 'object' || Array.isArray(value)) return value
  return Object.fromEntries(Object.entries(value).filter(([key]) => key !== 'relativePath'))
}

const knowledgeBaseTableAdapter = defineTable({
  table: knowledgeBaseTable,
  category: CATEGORY,
  keys: ['id'],
  fields: [
    'id',
    'name',
    'orderKey',
    'groupId',
    'embeddingModelId',
    'rerankModelId',
    'fileProcessorId',
    'chunkSize',
    'chunkOverlap',
    'chunkStrategy',
    'chunkSeparator',
    'threshold',
    'createdAt',
    'updatedAt'
  ],
  validate: (payload) => PortableKnowledgeBaseSchema.parse(payload),
  materialize: (payload, existing, tx) => {
    const portable = PortableKnowledgeBaseSchema.parse(payload)
    const hasModel =
      portable.embeddingModelId !== null &&
      Boolean(
        tx
          .select({ id: userModelTable.id })
          .from(userModelTable)
          .where(eq(userModelTable.id, portable.embeddingModelId))
          .limit(1)
          .get()
      )
    const embeddingModelId = hasModel ? portable.embeddingModelId : null

    if (existing) {
      const activeItem = tx
        .select({ id: knowledgeItemTable.id })
        .from(knowledgeItemTable)
        .where(
          and(eq(knowledgeItemTable.baseId, existing.id), inArray(knowledgeItemTable.status, ACTIVE_ITEM_STATUSES))
        )
        .limit(1)
        .get()
      if (activeItem) return false
      if (existsSync(getKnowledgeVectorStoreFilePathSync(existing.id))) {
        if (
          existing.embeddingModelId !== embeddingModelId ||
          existing.chunkSize !== portable.chunkSize ||
          existing.chunkOverlap !== portable.chunkOverlap ||
          existing.chunkStrategy !== portable.chunkStrategy ||
          existing.chunkSeparator !== portable.chunkSeparator ||
          existing.fileProcessorId !== portable.fileProcessorId
        ) {
          return false
        }
        return {
          ...portable,
          embeddingModelId,
          dimensions: existing.dimensions,
          documentCount: existing.documentCount,
          status: existing.status,
          error: existing.error
        }
      }
    }

    if (portable.embeddingModelId && !hasModel) {
      return {
        ...portable,
        embeddingModelId: null,
        dimensions: null,
        documentCount: null,
        status: 'failed',
        error: KNOWLEDGE_BASE_ERROR_MISSING_EMBEDDING_MODEL
      }
    }
    if (embeddingModelId) {
      return {
        ...portable,
        embeddingModelId,
        dimensions: null,
        documentCount: null,
        status: 'failed',
        error: KNOWLEDGE_BASE_ERROR_MISSING_VECTOR_STORE
      }
    }
    return {
      ...portable,
      embeddingModelId: null,
      dimensions: null,
      documentCount: null,
      status: 'completed',
      error: null
    }
  },
  write: (tx, key, row) => {
    if (!row) throw new Error('Knowledge base deletions are deferred to the owning service')
    knowledgeBaseService.applySync(tx, key.id, row)
  },
  afterCommit: (changes) => {
    const ids = [
      ...new Set(changes.filter((change) => change.entityType === 'knowledge_base').map((change) => change.entityId))
    ]
    if (ids.length === 0) return
    notifyDataApiDataChange([
      { endpoint: '/knowledge-bases', kind: 'projection', entityIds: ids },
      ...ids.map((id) => ({ endpoint: '/knowledge-bases/:id' as const, routeParams: { id } }))
    ])
  }
})

const knowledgeBaseEntity: EntityAdapter = {
  ...knowledgeBaseTableAdapter,
  apply(tx, change) {
    if (change.deleted) return knowledgeBaseTableAdapter.get(tx, change.entityId) ? 'deferred' : 'applied'
    return knowledgeBaseTableAdapter.apply(tx, change)
  }
}

const deletedItemBaseIds = new Map<string, string>()

const knowledgeItemTableAdapter = defineTable({
  table: knowledgeItemTable,
  category: CATEGORY,
  keys: ['id'],
  fields: ['id', 'baseId', 'groupId', 'type', 'data', 'createdAt', 'updatedAt'],
  project: (row) => {
    if (row.type !== 'note' && row.type !== 'url') return null
    const data =
      row.type === 'note'
        ? PortableNoteItemDataSchema.safeParse(omitRelativePath(row.data))
        : PortableUrlItemDataSchema.safeParse(omitRelativePath(row.data))
    if (!data.success) return null
    return {
      id: row.id,
      baseId: row.baseId,
      groupId: row.groupId,
      type: row.type,
      data: data.data,
      createdAt: row.createdAt,
      updatedAt: row.updatedAt
    }
  },
  validate: (payload) => PortableKnowledgeItemSchema.parse(payload),
  materialize: (payload, existing, tx) => {
    const portable = PortableKnowledgeItemSchema.parse(payload)
    if (existing && ACTIVE_ITEM_STATUSES.includes(existing.status as (typeof ACTIVE_ITEM_STATUSES)[number]))
      return false
    if (existing && existing.baseId !== portable.baseId) return false
    if (
      !tx
        .select({ id: knowledgeBaseTable.id })
        .from(knowledgeBaseTable)
        .where(eq(knowledgeBaseTable.id, portable.baseId))
        .limit(1)
        .get()
    ) {
      return false
    }

    if (existing && existsSync(getKnowledgeVectorStoreFilePathSync(existing.baseId))) {
      if (existing.type !== portable.type) return false
      if (portable.type === 'note') {
        if (existing.type !== 'note') return false
        const currentData = PortableNoteItemDataSchema.safeParse(omitRelativePath(existing.data))
        if (!currentData.success || currentData.data.content !== portable.data.content) return false
      } else {
        if (existing.type !== 'url') return false
        const currentData = PortableUrlItemDataSchema.safeParse(omitRelativePath(existing.data))
        if (!currentData.success || currentData.data.url !== portable.data.url) return false
      }
      const relativePath = (existing.data as { relativePath?: unknown }).relativePath
      const data = typeof relativePath === 'string' ? { ...portable.data, relativePath } : portable.data
      return { ...portable, data, status: existing.status, error: existing.error }
    }

    return { ...portable, status: 'idle', error: null }
  },
  write: (tx, key, row) => knowledgeItemService.applySync(tx, key.id, row),
  afterCommit: (changes) => {
    const applied = changes.filter((change) => change.entityType === 'knowledge_item')
    const baseIds = new Set<string>()
    for (const change of applied) {
      if (change.deleted) {
        const baseId = deletedItemBaseIds.get(change.entityId)
        if (baseId) baseIds.add(baseId)
        deletedItemBaseIds.delete(change.entityId)
      } else {
        baseIds.add(String(change.payload.baseId))
      }
    }
    const itemIds = [...new Set(applied.map((change) => change.entityId))]
    if (baseIds.size === 0 && itemIds.length === 0) return
    notifyDataApiDataChange([
      ...[...baseIds].flatMap((id) => [
        { endpoint: '/knowledge-bases/:id/items' as const, routeParams: { id }, kind: 'projection' as const },
        { endpoint: '/knowledge-bases/:id/items' as const, routeParams: { id }, kind: 'membership' as const }
      ]),
      ...itemIds.map((id) => ({ endpoint: '/knowledge-items/:id' as const, routeParams: { id } }))
    ])
  }
})

const knowledgeItemEntity: EntityAdapter = {
  ...knowledgeItemTableAdapter,
  apply(tx, change) {
    if (change.deleted) {
      deletedItemBaseIds.delete(change.entityId)
      const existing = knowledgeItemTableAdapter.get(tx, change.entityId)
      if (!existing) return 'applied'
      if (existing.payload === null) return 'deferred'
      const row = tx.select().from(knowledgeItemTable).where(eq(knowledgeItemTable.id, change.entityId)).limit(1).get()
      if (row && ACTIVE_ITEM_STATUSES.includes(row.status as (typeof ACTIVE_ITEM_STATUSES)[number])) return 'deferred'
      if (row && existsSync(getKnowledgeVectorStoreFilePathSync(row.baseId))) return 'deferred'
      const result = knowledgeItemTableAdapter.apply(tx, change)
      if (result === 'applied' && row) deletedItemBaseIds.set(change.entityId, row.baseId)
      return result
    }
    return knowledgeItemTableAdapter.apply(tx, change)
  }
}

function notesRoot(): string {
  return AbsoluteFilePathSchema.parse(path.resolve(application.getPath('feature.notes.data')))
}

function canonicalNotePath(value: string, allowRoot = false): string | null {
  if (allowRoot && value === '') return ''
  if (
    value.length === 0 ||
    value.length > MAX_NOTE_PATH_LENGTH ||
    !PosixRelativeFilePathSchema.safeParse(value).success ||
    !WindowsRelativeFilePathSchema.safeParse(value).success
  ) {
    return null
  }
  const posix = resolvePosixRelativeSegments(value)
  const windows = resolveWindowsRelativeSegments(value)
  if (!posix || !windows || posix.join('/') !== value || windows.join('/') !== value) return null
  return value
}

function maybeLstat(filePath: string): ReturnType<typeof lstatSync> | undefined {
  try {
    return lstatSync(filePath)
  } catch (error) {
    const code = (error as NodeJS.ErrnoException).code
    if (code === 'ENOENT' || code === 'ENOTDIR') return undefined
    throw error
  }
}

function safeNotesLocation(root: string, relativePath: string, allowRoot = false): string | null {
  const canonical = canonicalNotePath(relativePath, allowRoot)
  if (canonical === null) return null
  let current = root
  const rootStat = maybeLstat(root)
  if (rootStat && (!rootStat.isDirectory() || rootStat.isSymbolicLink())) return null
  const segments = canonical === '' ? [] : canonical.split('/')
  for (let index = 0; index < segments.length; index++) {
    current = path.join(current, segments[index])
    const info = maybeLstat(current)
    if (!info) return path.join(root, ...segments)
    if (info.isSymbolicLink()) return null
    if (index < segments.length - 1 && !info.isDirectory()) return null
    if (index === segments.length - 1 && !info.isFile() && !info.isDirectory()) return null
  }
  return path.join(root, ...segments)
}

function metadataRelativePath(row: typeof noteTable.$inferSelect): string | null {
  const root = notesRoot()
  if (!path.isAbsolute(row.rootPath) || path.resolve(row.rootPath) !== root || !path.isAbsolute(row.path)) return null
  const relative = path.relative(root, path.resolve(row.path))
  if (relative === '..' || relative.startsWith(`..${path.sep}`) || path.isAbsolute(relative)) return null
  const portable = relative.split(path.sep).join('/')
  if (safeNotesLocation(root, portable, true) === null) return null
  return canonicalNotePath(portable, true)
}

const noteMetadataTableAdapter = defineTable({
  table: noteTable,
  category: CATEGORY,
  keys: ['id'],
  fields: ['id', 'path', 'isStarred', 'isExpanded', 'createdAt', 'updatedAt'],
  project: (row) => {
    const relativePath = metadataRelativePath(row)
    return relativePath === null
      ? null
      : {
          id: row.id,
          path: relativePath,
          isStarred: row.isStarred,
          isExpanded: row.isExpanded,
          createdAt: row.createdAt,
          updatedAt: row.updatedAt
        }
  },
  validate: (payload) => PortableNoteMetadataSchema.parse(payload),
  materialize: (payload, _existing, tx) => {
    const portable = PortableNoteMetadataSchema.parse(payload)
    const root = notesRoot()
    const absolutePath = safeNotesLocation(root, portable.path, true)
    if (!absolutePath) return false
    const conflict = tx
      .select({ id: noteTable.id })
      .from(noteTable)
      .where(and(eq(noteTable.rootPath, root), eq(noteTable.path, absolutePath)))
      .limit(1)
      .get()
    if (conflict && conflict.id !== portable.id) return false
    return { ...portable, rootPath: root, path: absolutePath }
  },
  write: (tx, key, row) => noteService.applySync(tx, key.id, row as typeof noteTable.$inferSelect | null),
  afterCommit: (changes) => {
    if (!changes.some((change) => change.entityType === 'note')) return
    notifyDataApiDataChange([
      { endpoint: '/notes', kind: 'projection' },
      { endpoint: '/notes', kind: 'membership' }
    ])
  }
})

export const knowledgeEntities: EntityAdapter[] = [knowledgeBaseEntity, knowledgeItemEntity, noteMetadataTableAdapter]
