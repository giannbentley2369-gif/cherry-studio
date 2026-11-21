import { closeSync, constants, existsSync, fchmodSync, fstatSync, openSync } from 'node:fs'

import Database from 'better-sqlite3'
import { app } from 'electron'

import { application } from '@application'
import { loggerService } from '@logger'
import { BaseService, DependsOn, Injectable, Phase, ServicePhase } from '@main/core/lifecycle'
import type {
  SyncCategory,
  SyncConfig,
  SyncConfigUpdate,
  SyncConnectionInput,
  SyncStatus
} from '@shared/ipc/schemas/selfHostedSync'

import { guardLiveHistory } from './activeHistory'
import { conversationEntities } from './conversationEntities'
import { SyncEngine } from './engine'
import { createFileRelations } from './fileRelations'
import { knowledgeEntities } from './knowledgeEntities'
import { createLibraryEntities } from './libraryEntities'
import { ManagedFiles } from './managedFiles'
import { SettingsStore } from './settingsStore'
import { normalizeServerUrl, SyncTransport, SyncTransportError } from './transport'

const logger = loggerService.withContext('SelfHostedSync')
const entityOrder = [
  'group',
  'tag',
  'user_provider',
  'user_provider_api_keys',
  'user_model',
  'file_entry',
  'mcp_server',
  'knowledge_base',
  'knowledge_item',
  'assistant',
  'agent',
  'agent_skill',
  'prompt',
  'topic',
  'agent_session',
  'message',
  'agent_session_message',
  'translate_language',
  'translate_history',
  'mini_app',
  'painting'
]

function rank(type: string): number {
  const index = entityOrder.indexOf(type)
  return index < 0 ? entityOrder.length : index
}

@Injectable('SelfHostedSyncService')
@ServicePhase(Phase.WhenReady)
@DependsOn(['AiStreamManager', 'AgentSessionRuntimeService', 'FileManager'])
export class SelfHostedSyncService extends BaseService {
  private sqlite?: Database.Database
  private settings?: SettingsStore
  private engine?: SyncEngine
  private transport?: SyncTransport
  private controller?: AbortController
  private active?: Promise<void>
  private mutations: Promise<void> = Promise.resolve()
  private connectionState: SyncStatus['state'] = 'disabled'
  private activeCategories: SyncCategory[] = []
  private nextRun = Infinity
  private again = false
  private stopping = false

  protected onReady(): void {
    this.stopping = false
    const onFocus = () => this.trigger()
    app.on('browser-window-focus', onFocus)
    this.registerDisposable(() => app.off('browser-window-focus', onFocus))
    this.registerInterval(() => {
      if (Date.now() >= this.nextRun) this.trigger()
    }, 1000)
    if (existsSync(this.path)) void this.change(() => this.connect()).catch(() => logger.warn('Sync startup failed'))
  }

  protected async onStop(): Promise<void> {
    this.stopping = true
    this.controller?.abort()
    await this.mutations
    await this.disconnect()
    this.sqlite?.close()
    this.sqlite = undefined
    this.settings = undefined
    this.engine = undefined
  }

  getConfig(): SyncConfig {
    return this.store.config()
  }

  updateConfig(patch: SyncConfigUpdate): Promise<SyncConfig> {
    return this.change(async () => {
      const old = this.getConfig()
      await this.disconnect()
      try {
        const engine = old.serverUrl && patch.serverUrl !== undefined ? this.syncEngine : undefined
        return await this.store.update(patch, () => engine?.reset())
      } finally {
        await this.connect()
      }
    })
  }

  async testConnection(input: SyncConnectionInput): Promise<{ ok: boolean; error?: string }> {
    await this.mutations
    try {
      const url = normalizeServerUrl(input.serverUrl)
      const token = input.token ?? (url === this.getConfig().serverUrl ? await this.store.token() : '')
      if (!token) return { ok: false, error: 'Enter a token for this server.' }
      const controller = new AbortController()
      try {
        await new SyncTransport(url, token, controller.signal).status()
        return { ok: true }
      } finally {
        controller.abort()
      }
    } catch (error) {
      return { ok: false, error: error instanceof SyncTransportError ? error.message : 'Connection test failed.' }
    }
  }

  async syncNow(): Promise<SyncStatus> {
    await this.mutations
    if (!this.getConfig().enabled || this.stopping) return this.getStatus()
    if (!this.transport) await this.change(() => this.connect())
    this.trigger(true)
    await this.active
    return this.getStatus()
  }

  resetSyncState(): Promise<SyncStatus> {
    return this.change(async () => {
      await this.disconnect()
      this.syncEngine.reset()
      this.store.clearStats()
      await this.connect()
      return this.getStatus()
    })
  }

  getStatus(): SyncStatus {
    const { lastSyncedAt, serverRevision, lastError, nextRetryAt } = this.store.stats()
    return {
      state: this.connectionState,
      deviceId: this.getConfig().deviceId,
      lastSyncedAt,
      serverRevision,
      lastError,
      nextRetryAt
    }
  }

  private get path(): string {
    return application.getPath('feature.self_hosted_sync.database_file')
  }

  private get store(): SettingsStore {
    if (!this.settings) {
      const path = this.path
      const fd = openSync(path, constants.O_CREAT | constants.O_RDWR | (constants.O_NOFOLLOW ?? 0), 0o600)
      try {
        const stat = fstatSync(fd)
        if (!stat.isFile() || stat.nlink !== 1) throw new Error('Invalid sync database file.')
        if (process.platform !== 'win32') fchmodSync(fd, 0o600)
      } finally {
        closeSync(fd)
      }
      const sqlite = new Database(path)
      try {
        sqlite.pragma('secure_delete = ON')
        this.settings = new SettingsStore(sqlite)
        this.sqlite = sqlite
      } catch (error) {
        sqlite.close()
        throw error
      }
    }
    return this.settings
  }

  private get syncEngine(): SyncEngine {
    if (!this.engine) {
      this.getConfig()
      const files = new ManagedFiles(this.sqlite)
      const entities = [
        ...createLibraryEntities({
          includeFiles: () => this.activeCategories.includes('files'),
          includeAgentBindings: () => this.activeCategories.includes('agents')
        }),
        ...conversationEntities.map(guardLiveHistory),
        ...knowledgeEntities,
        ...createFileRelations(() => this.activeCategories),
        ...files.entities
      ]
      entities.sort((a, b) => rank(a.type) - rank(b.type))
      this.engine = new SyncEngine(this.sqlite!, application.get('DbService'), entities, files)
    }
    return this.engine
  }

  private change<T>(action: () => Promise<T>): Promise<T> {
    const next = this.mutations.then(action)
    this.mutations = next.then(
      () => {},
      () => {}
    )
    return next
  }

  private publish(state: SyncStatus['state']): void {
    this.connectionState = state
    if (!this.stopping) application.get('IpcApiService').broadcast('self_hosted_sync.status_changed', this.getStatus())
  }

  private async disconnect(): Promise<void> {
    this.nextRun = Infinity
    this.again = false
    this.controller?.abort()
    this.controller = undefined
    this.transport = undefined
    await this.active
  }

  private async connect(): Promise<void> {
    if (this.stopping) return
    const config = this.getConfig()
    this.activeCategories = config.categories
    if (!config.enabled || !config.categories.length) {
      this.publish('disabled')
      return
    }
    const controller = new AbortController()
    this.controller = controller
    let token: string
    try {
      token = await this.store.token()
      if (!token) throw new Error('Missing token')
    } catch {
      this.store.failed('Re-enter the sync token or unlock the operating system key store.', Date.now() + 60_000)
      this.publish('error')
      return
    }
    if (controller.signal.aborted || this.stopping) return
    const transport = new SyncTransport(config.serverUrl, token, controller.signal)
    this.transport = transport
    void transport
      .events(({ sourceDeviceId }) => {
        if (sourceDeviceId !== config.deviceId && !controller.signal.aborted) this.trigger(true)
      })
      .catch(() => {
        if (!controller.signal.aborted) logger.warn('Sync notifications unavailable; polling is still enabled')
      })
    this.nextRun = this.store.stats().nextRetryAt ?? 0
    this.publish('offline')
    this.trigger()
  }

  private trigger(force = false): void {
    const transport = this.transport
    const signal = this.controller?.signal
    if (!transport || !signal || signal.aborted || this.stopping) return
    if (this.active) {
      if (force) this.again = true
      return
    }
    const { nextRetryAt } = this.store.stats()
    if (!force && nextRetryAt && nextRetryAt > Date.now()) return
    this.nextRun = Infinity
    this.publish('syncing')
    const config = this.getConfig()
    this.active = Promise.resolve().then(async () => {
      try {
        const revision = await this.syncEngine.run(config.deviceId, config.categories, transport, signal)
        if (signal.aborted) return
        this.store.succeeded(revision)
        this.nextRun = Date.now() + config.intervalSeconds * 1000
        this.publish('connected')
      } catch (error) {
        if (signal.aborted) return
        const delay = Math.min(300_000, 2000 * 2 ** Math.min(8, this.store.stats().attempts))
        this.nextRun = Date.now() + delay
        const message =
          error instanceof SyncTransportError ? error.message : 'Could not apply sync changes. Local data is retained.'
        this.store.failed(message, this.nextRun)
        this.publish(error instanceof SyncTransportError && error.offline ? 'offline' : 'error')
      } finally {
        this.active = undefined
        if (this.again && !signal.aborted) {
          this.again = false
          this.trigger()
        }
      }
    })
  }
}
