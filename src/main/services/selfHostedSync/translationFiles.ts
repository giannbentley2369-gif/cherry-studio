import { and, eq, isNull } from 'drizzle-orm'

import { fileEntryTable } from '@data/db/schemas/file'
import { translateHistoryFileRefTable } from '@data/db/schemas/fileRelations'
import type { DbOrTx } from '@data/db/types'

import type { SyncChange } from './entity'

export function hasTranslationTarget(tx: DbOrTx, historyId: string): boolean {
  return !!tx
    .select({ id: fileEntryTable.id })
    .from(translateHistoryFileRefTable)
    .innerJoin(fileEntryTable, eq(fileEntryTable.id, translateHistoryFileRefTable.fileEntryId))
    .where(
      and(
        eq(translateHistoryFileRefTable.sourceId, historyId),
        eq(translateHistoryFileRefTable.role, 'target'),
        eq(fileEntryTable.origin, 'internal'),
        isNull(fileEntryTable.deletedAt)
      )
    )
    .limit(1)
    .get()
}

export function plannedTranslationTargets(changes: SyncChange[]): Map<string, string> {
  const targets = new Map<string, string>()
  for (const c of changes) {
    if (
      c.category === 'files' &&
      c.entityType === 'translate_history_file_ref' &&
      c.payload?.role === 'target' &&
      typeof c.payload.sourceId === 'string' &&
      typeof c.payload.fileEntryId === 'string'
    ) {
      targets.set(c.payload.sourceId, c.payload.fileEntryId)
    }
  }
  return targets
}

export function translationReady(tx: DbOrTx, change: SyncChange, targets: ReadonlyMap<string, string>): boolean {
  if (
    change.entityType !== 'translate_history' ||
    change.payload?.kind !== 'file' ||
    hasTranslationTarget(tx, change.entityId)
  )
    return true
  const file = targets.get(change.entityId)
  return (
    !!file &&
    !!tx
      .select({ id: fileEntryTable.id })
      .from(fileEntryTable)
      .where(and(eq(fileEntryTable.id, file), eq(fileEntryTable.origin, 'internal'), isNull(fileEntryTable.deletedAt)))
      .limit(1)
      .get()
  )
}
