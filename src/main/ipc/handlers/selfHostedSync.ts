import { application } from '@application'
import type { selfHostedSyncRequestSchemas } from '@shared/ipc/schemas/selfHostedSync'
import type { IpcHandlersFor } from '@shared/ipc/types'

export const selfHostedSyncHandlers: IpcHandlersFor<typeof selfHostedSyncRequestSchemas> = {
  'self_hosted_sync.get_config': async () => application.get('SelfHostedSyncService').getConfig(),
  'self_hosted_sync.update_config': (patch) => application.get('SelfHostedSyncService').updateConfig(patch),
  'self_hosted_sync.test_connection': (input) => application.get('SelfHostedSyncService').testConnection(input),
  'self_hosted_sync.get_status': async () => application.get('SelfHostedSyncService').getStatus(),
  'self_hosted_sync.sync_now': () => application.get('SelfHostedSyncService').syncNow(),
  'self_hosted_sync.reset_sync_state': () => application.get('SelfHostedSyncService').resetSyncState()
}
