import { and, eq, getTableColumns, getTableName, sql, type SQL } from 'drizzle-orm'
import { getTableConfig, type SQLiteColumn, type SQLiteTable } from 'drizzle-orm/sqlite-core'

import type { DbOrTx } from '@data/db/types'
import type { SyncCategory } from '@shared/ipc/schemas/selfHostedSync'

export type Payload = Record<string, unknown>
export type SyncChange = {
  entityType: string
  entityId: string
  category: SyncCategory
  deviceId: string
  updatedAt: number
} & ({ deleted: true; deletedAt: number; payload?: never } | { deleted?: false; payload: Payload })
export type RemoteChange = SyncChange & { serverRev: number }

export interface LocalEntity {
  id: string
  payload: Payload | null
  updatedAt?: number
  deletedAt?: number
  withheld?: boolean
}

export interface EntityAdapter {
  type: string
  category: SyncCategory
  scan(db: DbOrTx): LocalEntity[]
  get(db: DbOrTx, id: string): LocalEntity | undefined
  apply(tx: DbOrTx, change: SyncChange): 'applied' | 'deferred' | 'partial'
  afterCommit?(changes: SyncChange[]): void
}

interface TableOptions<T extends SQLiteTable> {
  table: T
  category: SyncCategory
  keys: readonly (keyof T['$inferSelect'] & string)[]
  fields: readonly (keyof T['$inferSelect'] & string)[]
  absent?(row: T['$inferSelect']): boolean
  project?(row: T['$inferSelect'], db: DbOrTx): Payload | null
  validate?(payload: Payload): Payload
  materialize?(payload: Payload, existing: T['$inferSelect'] | undefined, tx: DbOrTx): Payload | false
  write(tx: DbOrTx, key: Record<string, string>, row: T['$inferInsert'] | null, change: SyncChange): void
  afterCommit?(changes: SyncChange[]): void
}

function readKey(id: string, keys: readonly string[]): Record<string, string> {
  const values: unknown = keys.length === 1 ? [id] : JSON.parse(id)
  if (
    !Array.isArray(values) ||
    keys.length !== values.length ||
    !values.every((v) => typeof v === 'string' && v.length)
  )
    throw new Error('Invalid sync record identity')
  if (keys.length > 1 && JSON.stringify(values) !== id) throw new Error('Noncanonical sync record identity')
  return Object.fromEntries(keys.map((key, i) => [key, values[i]]))
}

function checkValue(column: SQLiteColumn, value: unknown): boolean {
  if (value === null) return !column.notNull
  switch (column.dataType) {
    case 'string':
      return typeof value === 'string' && (!column.enumValues?.length || column.enumValues.includes(value))
    case 'number':
      return (
        typeof value === 'number' &&
        Number.isFinite(value) &&
        (column.columnType !== 'SQLiteInteger' || Number.isSafeInteger(value))
      )
    case 'boolean':
      return typeof value === 'boolean'
    case 'json':
      return value !== undefined
    default:
      return false
  }
}

// A remote delete must not let SQLite silently change a local or unselected child.
function hasDependants(tx: DbOrTx, table: SQLiteTable, where: SQL | undefined): boolean {
  const refs = tx.all<{ child: string; id: number; source: string; target: string }>(sql`
    SELECT s.name AS child, fk.id, fk."from" AS source, fk."to" AS target
    FROM sqlite_schema AS s, pragma_foreign_key_list(s.name) AS fk
    WHERE s.type = 'table' AND fk."table" = ${getTableName(table)} ORDER BY s.name, fk.id, fk.seq
  `)
  const groups = new Map<string, typeof refs>()
  for (const ref of refs) {
    const key = JSON.stringify([ref.child, ref.id])
    const group = groups.get(key) ?? []
    group.push(ref)
    groups.set(key, group)
  }
  for (const group of groups.values()) {
    const child = sql.identifier(group[0].child)
    const on = sql.join(
      group.map((r) => sql`child.${sql.identifier(r.source)} = ${table}.${sql.identifier(r.target)}`),
      sql` AND `
    )
    if (tx.all(sql`SELECT 1 FROM ${table} JOIN ${child} AS child ON ${on} WHERE ${where} LIMIT 1`).length) return true
  }
  return false
}

export function defineTable<T extends SQLiteTable>(options: TableOptions<T>): EntityAdapter {
  const { table, keys, fields, category } = options
  const type = getTableName(table)
  const columns: Record<string, SQLiteColumn> = getTableColumns(table)
  const foreignKeys = getTableConfig(table).foreignKeys.map((fk) => fk.reference())
  const keyOf = (row: Payload) =>
    keys.length === 1 ? String(row[keys[0]]) : JSON.stringify(keys.map((key) => row[key]))
  const predicate = (key: Record<string, string>) => and(...keys.map((k) => eq(columns[k], key[k])))
  const read = (db: DbOrTx, key: Record<string, string>) =>
    db
      .select()
      .from(table as SQLiteTable)
      .where(predicate(key))
      .limit(1)
      .get() as T['$inferSelect'] | undefined

  function checked(payload: Payload): Payload {
    if (
      Object.keys(payload).length !== fields.length ||
      !fields.every((f) => Object.hasOwn(payload, f) && checkValue(columns[f], payload[f]))
    ) {
      throw new Error(`Invalid portable fields for ${type}`)
    }
    return options.validate ? options.validate(payload) : payload
  }

  function describe(row: T['$inferSelect'], db: DbOrTx): LocalEntity {
    const id = keyOf(row)
    const value = options.project ? options.project(row, db) : Object.fromEntries(fields.map((f) => [f, row[f]]))
    if (value === null) return { id, payload: null, withheld: true }
    const updatedAt = typeof row.updatedAt === 'number' ? row.updatedAt : undefined
    if (typeof row.deletedAt === 'number') return { id, payload: null, deletedAt: row.deletedAt, updatedAt }
    return { id, payload: checked(value), updatedAt }
  }

  return {
    type,
    category,
    scan: (db) =>
      db
        .select()
        .from(table as SQLiteTable)
        .all()
        .flatMap((record) => {
          const row = record as T['$inferSelect']
          return options.absent?.(row) ? [] : [describe(row, db)]
        }),
    get(db, id) {
      const row = read(db, readKey(id, keys))
      return row && !options.absent?.(row) ? describe(row, db) : undefined
    },
    apply(tx, change) {
      if (change.entityType !== type || change.category !== category) throw new Error('Unexpected sync entity')
      const key = readKey(change.entityId, keys)
      const existing = read(tx, key)
      if (existing && options.project?.(existing, tx) === null) return 'deferred'
      if (change.deleted) {
        if (!existing) return 'applied'
        if (!columns.deletedAt && hasDependants(tx, table, predicate(key))) return 'deferred'
        options.write(tx, key, null, change)
        return 'applied'
      }
      const portable = checked(change.payload)
      if (keyOf(portable) !== change.entityId || portable.deletedAt != null)
        throw new Error('Invalid sync record identity or deletion state')
      const material = options.materialize ? options.materialize(portable, existing, tx) : portable
      if (material === false) return 'deferred'
      const row = { ...material, ...key }
      if (columns.updatedAt) row.updatedAt = change.updatedAt
      if (columns.deletedAt) row.deletedAt = null
      let partial = false
      for (const fk of foreignKeys) {
        const names = fk.columns.map((col) => Object.keys(columns).find((k) => columns[k] === col)!)
        if (names.some((name) => row[name] == null)) continue
        const condition = and(...fk.foreignColumns.map((col, i) => eq(col, row[names[i]])))
        if (
          tx
            .select({ one: sql<number>`1` })
            .from(fk.foreignTable)
            .where(condition)
            .limit(1)
            .get()
        )
          continue
        const nullable = names.filter((k) => !columns[k].notNull)
        if (!nullable.length || fk.foreignTable === table) return 'deferred'
        for (const k of nullable) row[k] = null
        partial = true
      }
      options.write(tx, key, row, change)
      return partial ? 'partial' : 'applied'
    },
    afterCommit: options.afterCommit
  }
}
