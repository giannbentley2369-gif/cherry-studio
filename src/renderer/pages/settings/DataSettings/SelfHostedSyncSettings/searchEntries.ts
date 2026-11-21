import type { SettingsSearchEntry } from '../../settingsSearch/types'

export const selfHostedSyncSearchEntries: SettingsSearchEntry[] = [
  {
    anchorId: 'self-hosted-sync-server-url',
    titleKey: 'settings.data.self_hosted_sync.server_url',
    panel: 'self_hosted_sync',
    groupKey: 'settings.data.self_hosted_sync.title',
    aliases: ['self-hosted sync', 'self hosted sync', 'self-hosted server']
  },
  {
    anchorId: 'self-hosted-sync-token',
    titleKey: 'settings.data.self_hosted_sync.token',
    panel: 'self_hosted_sync',
    groupKey: 'settings.data.self_hosted_sync.title'
  },
  {
    anchorId: 'self-hosted-sync-category-chats',
    titleKey: 'settings.data.self_hosted_sync.category.chats',
    panel: 'self_hosted_sync',
    groupKey: 'settings.data.self_hosted_sync.categories'
  },
  {
    anchorId: 'self-hosted-sync-category-providers',
    titleKey: 'settings.data.self_hosted_sync.category.providers',
    panel: 'self_hosted_sync',
    groupKey: 'settings.data.self_hosted_sync.categories'
  },
  {
    anchorId: 'self-hosted-sync-category-assistants',
    titleKey: 'settings.data.self_hosted_sync.category.assistants',
    panel: 'self_hosted_sync',
    groupKey: 'settings.data.self_hosted_sync.categories'
  },
  {
    anchorId: 'self-hosted-sync-category-mcp',
    titleKey: 'settings.data.self_hosted_sync.category.mcp',
    panel: 'self_hosted_sync',
    groupKey: 'settings.data.self_hosted_sync.categories'
  },
  {
    anchorId: 'self-hosted-sync-category-preferences',
    titleKey: 'settings.data.self_hosted_sync.category.preferences',
    panel: 'self_hosted_sync',
    groupKey: 'settings.data.self_hosted_sync.categories'
  },
  {
    anchorId: 'self-hosted-sync-category-knowledge',
    titleKey: 'settings.data.self_hosted_sync.category.knowledge',
    panel: 'self_hosted_sync',
    groupKey: 'settings.data.self_hosted_sync.categories'
  },
  {
    anchorId: 'self-hosted-sync-category-agents',
    titleKey: 'settings.data.self_hosted_sync.category.agents',
    panel: 'self_hosted_sync',
    groupKey: 'settings.data.self_hosted_sync.categories'
  },
  {
    anchorId: 'self-hosted-sync-category-files',
    titleKey: 'settings.data.self_hosted_sync.category.files',
    panel: 'self_hosted_sync',
    groupKey: 'settings.data.self_hosted_sync.categories'
  },
  {
    anchorId: 'self-hosted-sync-category-other',
    titleKey: 'settings.data.self_hosted_sync.category.other',
    panel: 'self_hosted_sync',
    groupKey: 'settings.data.self_hosted_sync.categories'
  }
]
