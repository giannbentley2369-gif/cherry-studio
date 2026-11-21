import * as z from 'zod'

import { defineRoute } from '../define'

export const SYNC_CATEGORIES = [
  'chats',
  'providers',
  'assistants',
  'mcp',
  'preferences',
  'knowledge',
  'agents',
  'files',
  'other'
] as const

export const syncCategorySchema = z.enum(SYNC_CATEGORIES)
const syncCategoriesSchema = z
  .array(syncCategorySchema)
  .max(SYNC_CATEGORIES.length)
  .refine((values) => {
    return new Set(values).size === values.length
  })

const serverUrlSchema = z
  .string()
  .max(2048)
  .refine((value) => {
    try {
      const url = new URL(value)
      const loopback =
        url.hostname === 'localhost' || /^127\.\d+\.\d+\.\d+$/.test(url.hostname) || url.hostname === '[::1]'
      return (
        !url.username &&
        !url.password &&
        !url.search &&
        !url.hash &&
        (url.protocol === 'https:' || (url.protocol === 'http:' && loopback))
      )
    } catch {
      return false
    }
  }, 'Use HTTPS, or HTTP on loopback.')
const tokenSchema = z.string().regex(/^[A-Za-z0-9._~+/-]{32,512}$/)

const syncConfigSchema = z.object({
  enabled: z.boolean(),
  serverUrl: z.string(),
  deviceId: z.string().min(1),
  categories: syncCategoriesSchema,
  intervalSeconds: z.number().int().min(15).max(3600),
  hasToken: z.boolean()
})

const syncStatusSchema = z.object({
  state: z.enum(['disabled', 'offline', 'syncing', 'connected', 'error']),
  deviceId: z.string().min(1),
  lastSyncedAt: z.number().nullable(),
  serverRevision: z.number().int().nonnegative(),
  lastError: z.string().nullable(),
  nextRetryAt: z.number().nullable()
})

const syncConfigUpdateSchema = z
  .object({
    enabled: z.boolean().optional(),
    serverUrl: z.union([serverUrlSchema, z.literal('')]).optional(),
    categories: syncCategoriesSchema.optional(),
    intervalSeconds: z.number().int().min(15).max(3600).optional(),
    token: z.union([tokenSchema, z.literal('')]).optional()
  })
  .strict()

const syncConnectionInputSchema = z.strictObject({
  serverUrl: serverUrlSchema,
  token: z.union([tokenSchema, z.literal('')]).optional()
})

const syncConnectionResultSchema = z.object({ ok: z.boolean(), error: z.string().optional() })

export type SyncCategory = z.infer<typeof syncCategorySchema>
export type SyncConfig = z.infer<typeof syncConfigSchema>
export type SyncConfigUpdate = z.infer<typeof syncConfigUpdateSchema>
export type SyncConnectionInput = z.infer<typeof syncConnectionInputSchema>
export type SyncStatus = z.infer<typeof syncStatusSchema>

export const selfHostedSyncRequestSchemas = {
  'self_hosted_sync.get_config': defineRoute({ input: z.void(), output: syncConfigSchema }),
  'self_hosted_sync.update_config': defineRoute({ input: syncConfigUpdateSchema, output: syncConfigSchema }),
  'self_hosted_sync.test_connection': defineRoute({
    input: syncConnectionInputSchema,
    output: syncConnectionResultSchema
  }),
  'self_hosted_sync.get_status': defineRoute({ input: z.void(), output: syncStatusSchema }),
  'self_hosted_sync.sync_now': defineRoute({ input: z.void(), output: syncStatusSchema }),
  'self_hosted_sync.reset_sync_state': defineRoute({ input: z.void(), output: syncStatusSchema })
}

export type SelfHostedSyncEventSchemas = {
  'self_hosted_sync.status_changed': SyncStatus
}
