import '@testing-library/jest-dom/vitest'
import { render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { beforeEach, describe, expect, it, vi } from 'vitest'

import i18n from '@renderer/i18n/resolver'
import { popup } from '@renderer/services/popup'

import SelfHostedSyncSettings from '../SelfHostedSyncSettings'

const { requestMock } = vi.hoisted(() => ({ requestMock: vi.fn() }))

vi.mock('@renderer/ipc', () => ({ ipcApi: { request: requestMock }, useIpcOn: vi.fn() }))
vi.mock('@renderer/hooks/useTheme', () => ({ useTheme: () => ({ theme: 'light' }) }))

const config = {
  enabled: true,
  serverUrl: 'https://sync.example',
  deviceId: 'device-1',
  categories: ['chats', 'assistants'],
  intervalSeconds: 60,
  hasToken: true
}

const status = {
  state: 'connected',
  deviceId: 'device-1',
  lastSyncedAt: 1_800_000_000_000,
  serverRevision: 7,
  lastError: null,
  nextRetryAt: null
}

const categoryLabels: Record<string, string> = {
  chats: 'Chats and messages',
  providers: 'AI providers and API keys',
  assistants: 'Assistants and prompts',
  mcp: 'MCP servers',
  preferences: 'Portable preferences',
  knowledge: 'Notes and knowledge bases',
  agents: 'Agents and sessions',
  files: 'Files and attachments',
  other: 'Other data'
}

async function renderSettings() {
  render(<SelfHostedSyncSettings />)
  await screen.findByRole('switch', { name: 'Enable sync' })
  requestMock.mockClear()
}

describe('SelfHostedSyncSettings', () => {
  beforeEach(async () => {
    await i18n.changeLanguage('en-US')
    vi.clearAllMocks()
    requestMock.mockImplementation((route: string, input?: Record<string, unknown>) => {
      if (route === 'self_hosted_sync.get_config') return Promise.resolve(config)
      if (route === 'self_hosted_sync.get_status') return Promise.resolve(status)
      if (route === 'self_hosted_sync.update_config') return Promise.resolve({ ...config, ...input })
      if (route === 'self_hosted_sync.test_connection') return Promise.resolve({ ok: true })
      if (route === 'self_hosted_sync.sync_now') return Promise.resolve({ ...status, state: 'connected' })
      if (route === 'self_hosted_sync.reset_sync_state') return Promise.resolve({ ...status, state: 'offline' })
      throw new Error(`Unexpected request: ${route}`)
    })
  })

  it('shows all nine data categories and keeps provider-key sharing opt-in', async () => {
    const user = userEvent.setup()
    await renderSettings()

    expect(screen.getByText('Device ID: device-1')).toBeInTheDocument()
    for (const category of [
      'chats',
      'providers',
      'assistants',
      'mcp',
      'preferences',
      'knowledge',
      'agents',
      'files',
      'other'
    ]) {
      expect(screen.getByRole('switch', { name: categoryLabels[category] })).toBeInTheDocument()
    }

    expect(screen.getByRole('switch', { name: 'AI providers and API keys' })).not.toBeChecked()
    await user.click(screen.getByRole('switch', { name: 'AI providers and API keys' }))
    await waitFor(() =>
      expect(requestMock).toHaveBeenCalledWith('self_hosted_sync.update_config', {
        categories: ['chats', 'assistants', 'providers']
      })
    )
  })

  it('reveals only the token being entered, never the saved credential', async () => {
    const user = userEvent.setup()
    await renderSettings()
    const input = screen.getByLabelText('Bearer token')
    expect(input).toHaveValue('')
    expect(input).toHaveAttribute('type', 'password')
    await user.type(input, 'example')
    await user.click(screen.getByRole('button', { name: 'Show key' }))
    expect(input).toHaveAttribute('type', 'text')
    await user.click(screen.getByRole('button', { name: 'Hide key' }))
    expect(input).toHaveAttribute('type', 'password')
  })

  it('tests a connection using the unsaved URL and token', async () => {
    const user = userEvent.setup()
    await renderSettings()

    await user.clear(screen.getByRole('textbox', { name: 'Server URL' }))
    await user.type(screen.getByRole('textbox', { name: 'Server URL' }), 'https://new-sync.example')
    await user.type(screen.getByLabelText('Bearer token'), 'test-token')
    await user.click(screen.getByRole('button', { name: 'Test connection' }))

    await waitFor(() =>
      expect(requestMock).toHaveBeenCalledWith('self_hosted_sync.test_connection', {
        serverUrl: 'https://new-sync.example',
        token: 'test-token'
      })
    )
    expect(await screen.findByText('Connection test succeeded.')).toBeInTheDocument()
  })

  it('saves the server URL and token together', async () => {
    const user = userEvent.setup()
    await renderSettings()

    await user.clear(screen.getByRole('textbox', { name: 'Server URL' }))
    await user.type(screen.getByRole('textbox', { name: 'Server URL' }), 'https://new-sync.example')
    await user.type(screen.getByLabelText('Bearer token'), 'saved-token')
    await user.click(screen.getByRole('button', { name: 'Save connection' }))

    await waitFor(() =>
      expect(requestMock).toHaveBeenCalledWith('self_hosted_sync.update_config', {
        serverUrl: 'https://new-sync.example',
        token: 'saved-token'
      })
    )
  })

  it('does not reset sync state when the confirmation is cancelled', async () => {
    const user = userEvent.setup()
    vi.mocked(popup.confirm).mockResolvedValueOnce(false)
    await renderSettings()

    await user.click(screen.getByRole('button', { name: 'Reset sync state' }))

    await waitFor(() => expect(popup.confirm).toHaveBeenCalledOnce())
    expect(requestMock).not.toHaveBeenCalledWith('self_hosted_sync.reset_sync_state')
  })
})
