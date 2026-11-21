export const CATEGORIES = [
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

export type Category = (typeof CATEGORIES)[number]
export type JsonObject = Record<string, unknown>

interface ChangeBase {
  category: Category
  entityType: string
  entityId: string
  deviceId: string
  updatedAt: number
}

export type IncomingChange = ChangeBase &
  ({ payload: JsonObject; deleted?: never; deletedAt?: never } | { deleted: true; deletedAt: number; payload?: never })

export type Winner = ChangeBase &
  (
    | { payload: JsonObject; deleted?: never; deletedAt?: never }
    | { deleted: true; deletedAt: number; payload?: never }
  ) & { serverRev: number }

export class ApiError extends Error {
  constructor(
    readonly status: number,
    readonly code: string,
    message: string
  ) {
    super(message)
    this.name = 'ApiError'
  }
}
