import { Eye, EyeOff, RefreshCw, RotateCcw } from 'lucide-react'
import type { FC } from 'react'
import { useCallback, useEffect, useState } from 'react'
import { useTranslation } from 'react-i18next'

import { Button, Input, RowFlex, Switch } from '@cherrystudio/ui'
import Selector from '@renderer/components/Selector'
import {
  SettingDescription,
  SettingDivider,
  SettingGroup,
  SettingRow,
  SettingRowTitle,
  SettingTitle
} from '@renderer/components/SettingsPrimitives'
import { useTheme } from '@renderer/hooks/useTheme'
import { ipcApi, useIpcOn } from '@renderer/ipc'
import { settingsControlClasses } from '@renderer/pages/settings/settingsStyles'
import { popup } from '@renderer/services/popup'
import { toast } from '@renderer/services/toast'
import type { InputFor, OutputFor } from '@shared/ipc/types'

type SyncConfig = OutputFor<'self_hosted_sync.get_config'>
type SyncStatus = OutputFor<'self_hosted_sync.get_status'>
type SyncConfigUpdate = InputFor<'self_hosted_sync.update_config'>
type SyncCategory = SyncConfig['categories'][number]

const CATEGORIES: { id: SyncCategory; labelKey: string }[] = [
  { id: 'chats', labelKey: 'settings.data.self_hosted_sync.category.chats' },
  { id: 'providers', labelKey: 'settings.data.self_hosted_sync.category.providers' },
  { id: 'assistants', labelKey: 'settings.data.self_hosted_sync.category.assistants' },
  { id: 'mcp', labelKey: 'settings.data.self_hosted_sync.category.mcp' },
  { id: 'preferences', labelKey: 'settings.data.self_hosted_sync.category.preferences' },
  { id: 'knowledge', labelKey: 'settings.data.self_hosted_sync.category.knowledge' },
  { id: 'agents', labelKey: 'settings.data.self_hosted_sync.category.agents' },
  { id: 'files', labelKey: 'settings.data.self_hosted_sync.category.files' },
  { id: 'other', labelKey: 'settings.data.self_hosted_sync.category.other' }
]

const STATUS_LABEL_KEYS: Record<SyncStatus['state'], string> = {
  disabled: 'settings.data.self_hosted_sync.status.disabled',
  offline: 'settings.data.self_hosted_sync.status.offline',
  syncing: 'settings.data.self_hosted_sync.status.syncing',
  connected: 'settings.data.self_hosted_sync.status.connected',
  error: 'settings.data.self_hosted_sync.status.error'
}

const INTERVALS = [60, 300, 900, 1_800, 3_600]

const formatTimestamp = (timestamp: number) => new Date(timestamp).toLocaleString()

const intervalLabel = (seconds: number, t: ReturnType<typeof useTranslation>['t']) =>
  seconds % 3_600 === 0
    ? t('settings.data.self_hosted_sync.interval.hours', { value: seconds / 3_600 })
    : t('settings.data.self_hosted_sync.interval.minutes', { value: seconds / 60 })

const SelfHostedSyncSettings: FC = () => {
  const { t } = useTranslation()
  const { theme } = useTheme()
  const [config, setConfig] = useState<SyncConfig>()
  const [status, setStatus] = useState<SyncStatus>()
  const [serverUrl, setServerUrl] = useState('')
  const [token, setToken] = useState('')
  const [visible, setVisible] = useState(false)
  const [loading, setLoading] = useState(true)
  const [saving, setSaving] = useState(false)
  const [testing, setTesting] = useState(false)
  const [testResult, setTestResult] = useState<{ ok: boolean; error?: string }>()

  useIpcOn('self_hosted_sync.status_changed', setStatus)

  const loadSettings = useCallback(async () => {
    setLoading(true)
    try {
      const [nextConfig, nextStatus] = await Promise.all([
        ipcApi.request('self_hosted_sync.get_config'),
        ipcApi.request('self_hosted_sync.get_status')
      ])
      setConfig(nextConfig)
      setStatus(nextStatus)
      setServerUrl(nextConfig.serverUrl)
    } catch {
      toast.error(t('settings.data.self_hosted_sync.load_error'))
    } finally {
      setLoading(false)
    }
  }, [t])

  useEffect(() => {
    void loadSettings()
  }, [loadSettings])

  const updateConfig = async (patch: SyncConfigUpdate) => {
    if (!config || saving) return
    setSaving(true)
    try {
      const nextConfig = await ipcApi.request('self_hosted_sync.update_config', patch)
      setConfig(nextConfig)
      if (patch.serverUrl !== undefined) {
        setServerUrl(nextConfig.serverUrl)
        setToken('')
        setVisible(false)
      }
    } catch {
      toast.error(t('settings.data.self_hosted_sync.save_error'))
      void loadSettings()
    } finally {
      setSaving(false)
    }
  }

  const saveConnection = () => {
    const patch: SyncConfigUpdate = { serverUrl: serverUrl.trim() }
    if (token.length > 0) patch.token = token
    void updateConfig(patch)
  }

  const testConnection = async () => {
    setTesting(true)
    setTestResult(undefined)
    try {
      const result = await ipcApi.request('self_hosted_sync.test_connection', {
        serverUrl: serverUrl.trim(),
        ...(token.length > 0 ? { token } : {})
      })
      setTestResult(result)
    } catch (error) {
      setTestResult({ ok: false, error: error instanceof Error ? error.message : undefined })
    } finally {
      setTesting(false)
    }
  }

  const toggleCategory = (category: SyncCategory, enabled: boolean) => {
    if (!config) return
    const categories = enabled
      ? [...config.categories, category]
      : config.categories.filter((selected) => selected !== category)
    void updateConfig({ categories })
  }

  const syncNow = async () => {
    setSaving(true)
    try {
      setStatus(await ipcApi.request('self_hosted_sync.sync_now'))
    } catch {
      toast.error(t('settings.data.self_hosted_sync.sync_error'))
    } finally {
      setSaving(false)
    }
  }

  const resetSyncState = async () => {
    const confirmed = await popup.confirm({
      title: t('settings.data.self_hosted_sync.reset.confirm_title'),
      content: t('settings.data.self_hosted_sync.reset.confirm_content'),
      okText: t('common.confirm'),
      cancelText: t('common.cancel'),
      centered: true,
      okButtonProps: { danger: true }
    })
    if (!confirmed) return

    setSaving(true)
    try {
      setStatus(await ipcApi.request('self_hosted_sync.reset_sync_state'))
    } catch {
      toast.error(t('settings.data.self_hosted_sync.reset.error'))
    } finally {
      setSaving(false)
    }
  }

  if (loading && !config) {
    return <SettingGroup theme={theme}>{t('common.loading')}</SettingGroup>
  }

  if (!config) {
    return (
      <SettingGroup theme={theme}>
        <SettingTitle>{t('settings.data.self_hosted_sync.title')}</SettingTitle>
        <SettingDescription>{t('settings.data.self_hosted_sync.load_error')}</SettingDescription>
        <Button className="mt-4" variant="outline" onClick={() => void loadSettings()}>
          {t('common.retry')}
        </Button>
      </SettingGroup>
    )
  }

  const intervalOptions = [...new Set([...INTERVALS, config.intervalSeconds])].sort((a, b) => a - b)

  return (
    <div className="space-y-4">
      <SettingGroup theme={theme}>
        <SettingTitle>{t('settings.data.self_hosted_sync.title')}</SettingTitle>
        <SettingDescription>{t('settings.data.self_hosted_sync.description')}</SettingDescription>
        <SettingDivider />
        <SettingRow id="setting-data-self-hosted-sync-server-url" className="scroll-mt-6">
          <SettingRowTitle>{t('settings.data.self_hosted_sync.server_url')}</SettingRowTitle>
          <Input
            aria-label={t('settings.data.self_hosted_sync.server_url')}
            type="url"
            value={serverUrl}
            onChange={(event) => {
              setServerUrl(event.target.value)
              setTestResult(undefined)
            }}
            placeholder={t('settings.data.self_hosted_sync.server_url_placeholder')}
            className={settingsControlClasses.text}
            disabled={saving}
          />
        </SettingRow>
        <SettingDivider />
        <SettingRow id="setting-data-self-hosted-sync-token" className="scroll-mt-6">
          <SettingRowTitle>{t('settings.data.self_hosted_sync.token')}</SettingRowTitle>
          <div className="flex min-w-0 flex-col gap-1.5">
            <RowFlex className="items-center gap-1">
              <Input
                aria-label={t('settings.data.self_hosted_sync.token')}
                type={visible ? 'text' : 'password'}
                autoComplete="new-password"
                value={token}
                onChange={(event) => {
                  setToken(event.target.value)
                  setTestResult(undefined)
                }}
                placeholder={t('settings.data.self_hosted_sync.token_placeholder')}
                className={settingsControlClasses.text}
                disabled={saving}
              />
              <Button
                type="button"
                size="icon"
                variant="ghost"
                aria-label={t(visible ? 'settings.provider.api_key.hide_key' : 'settings.provider.api_key.show_key')}
                aria-pressed={visible}
                onClick={() => setVisible(!visible)}>
                {visible ? <EyeOff size={16} /> : <Eye size={16} />}
              </Button>
            </RowFlex>
            {config.hasToken && (
              <span className="text-xs text-muted-foreground">{t('settings.data.self_hosted_sync.token_saved')}</span>
            )}
          </div>
        </SettingRow>
        <SettingDivider />
        <SettingRow>
          <SettingRowTitle>{t('settings.data.self_hosted_sync.interval')}</SettingRowTitle>
          <Selector
            aria-label={t('settings.data.self_hosted_sync.interval')}
            value={config.intervalSeconds}
            disabled={saving}
            options={intervalOptions.map((seconds) => ({ label: intervalLabel(seconds, t), value: seconds }))}
            onChange={(intervalSeconds) => void updateConfig({ intervalSeconds })}
          />
        </SettingRow>
        <SettingDivider />
        <SettingRow>
          <SettingRowTitle>{t('settings.data.self_hosted_sync.enabled')}</SettingRowTitle>
          <Switch
            aria-label={t('settings.data.self_hosted_sync.enabled')}
            checked={config.enabled}
            disabled={saving || !config.hasToken}
            onCheckedChange={(enabled) => void updateConfig({ enabled })}
          />
        </SettingRow>
        <SettingDivider />
        <SettingRow>
          <RowFlex className="flex-wrap justify-end gap-2">
            <Button variant="outline" disabled={saving || loading} onClick={saveConnection}>
              {t('settings.data.self_hosted_sync.save')}
            </Button>
            <Button
              variant="outline"
              disabled={testing || saving || !serverUrl.trim()}
              onClick={() => void testConnection()}>
              <RefreshCw size={14} className={testing ? 'animate-spin' : undefined} />
              {t('settings.data.self_hosted_sync.test_connection')}
            </Button>
          </RowFlex>
        </SettingRow>
        {testResult && (
          <p className={testResult.ok ? 'mt-3 text-sm text-success' : 'mt-3 text-sm text-destructive'} role="status">
            {testResult.ok
              ? t('settings.data.self_hosted_sync.test_success')
              : `${t('settings.data.self_hosted_sync.test_failure')}${testResult.error ? `: ${testResult.error}` : ''}`}
          </p>
        )}
      </SettingGroup>

      <SettingGroup theme={theme}>
        <SettingTitle>{t('settings.data.self_hosted_sync.categories')}</SettingTitle>
        {CATEGORIES.map(({ id, labelKey }, index) => (
          <div key={id}>
            {index > 0 && <SettingDivider />}
            <SettingRow id={`setting-data-self-hosted-sync-category-${id}`} className="scroll-mt-6">
              <div>
                <SettingRowTitle>{t(labelKey)}</SettingRowTitle>
                {id === 'providers' && (
                  <SettingDescription>{t('settings.data.self_hosted_sync.provider_api_keys_help')}</SettingDescription>
                )}
              </div>
              <Switch
                aria-label={t(labelKey)}
                checked={config.categories.includes(id)}
                disabled={saving}
                onCheckedChange={(enabled) => toggleCategory(id, enabled)}
              />
            </SettingRow>
          </div>
        ))}
      </SettingGroup>

      <SettingGroup theme={theme}>
        <SettingTitle>{t('settings.data.self_hosted_sync.status.title')}</SettingTitle>
        <SettingDescription>{t(STATUS_LABEL_KEYS[status?.state ?? 'offline'])}</SettingDescription>
        <p className="mt-2 break-all text-xs text-muted-foreground">
          {t('settings.data.self_hosted_sync.status.device_id', { id: config.deviceId })}
        </p>
        {status?.lastSyncedAt != null && (
          <p className="mt-2 text-xs text-muted-foreground">
            {t('settings.data.self_hosted_sync.status.last_synced', { date: formatTimestamp(status.lastSyncedAt) })}
          </p>
        )}
        {status && (
          <p className="mt-1 text-xs text-muted-foreground">
            {t('settings.data.self_hosted_sync.status.server_revision', { revision: status.serverRevision })}
          </p>
        )}
        {status?.nextRetryAt != null && (
          <p className="mt-1 text-xs text-muted-foreground">
            {t('settings.data.self_hosted_sync.status.next_retry', { date: formatTimestamp(status.nextRetryAt) })}
          </p>
        )}
        {status?.lastError && <p className="mt-2 text-sm text-destructive">{status.lastError}</p>}
        <SettingDivider />
        <SettingRow>
          <SettingRowTitle>{t('settings.data.self_hosted_sync.status.actions')}</SettingRowTitle>
          <RowFlex className="flex-wrap justify-end gap-2">
            <Button variant="outline" disabled={saving || !config.enabled} onClick={() => void syncNow()}>
              <RefreshCw size={14} className={status?.state === 'syncing' ? 'animate-spin' : undefined} />
              {t('settings.data.self_hosted_sync.sync_now')}
            </Button>
            <Button variant="destructive" disabled={saving} onClick={() => void resetSyncState()}>
              <RotateCcw size={14} />
              {t('settings.data.self_hosted_sync.reset.button')}
            </Button>
          </RowFlex>
        </SettingRow>
      </SettingGroup>
    </div>
  )
}

export default SelfHostedSyncSettings
