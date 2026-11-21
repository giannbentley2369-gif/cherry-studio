import type { DbOrTx } from '@data/db/types'

import type { RemoteChange, SyncChange } from './entity'
import type { SyncTransport } from './transport'

export interface FileTransferHooks {
  reset?(): void
  prepareScan(db: DbOrTx, signal: AbortSignal): Promise<void>
  upload(change: SyncChange, transport: Pick<SyncTransport, 'hasBlob' | 'putBlob'>, signal: AbortSignal): Promise<void>
  prepareApply(
    changes: RemoteChange[],
    transport: Pick<SyncTransport, 'getBlob'>,
    signal: AbortSignal
  ): Promise<{ commit(applied: RemoteChange[]): Promise<void>; abort(): Promise<void> }>
}
