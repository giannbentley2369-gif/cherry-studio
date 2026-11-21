import Database from 'better-sqlite3'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'

import { SettingsStore } from '../settingsStore'

describe('private sync settings', () => {
  let db: Database.Database
  const token = 'test-only-private-session-credential'
  const unavailable = {
    available: async () => false,
    encrypt: async (): Promise<Buffer> => {
      throw new Error('No key store')
    },
    decrypt: async (): Promise<string> => {
      throw new Error('No key store')
    }
  }
  const secure = {
    available: async () => true,
    encrypt: async () => Buffer.from([1, 2, 3]),
    decrypt: async (value: Buffer) => {
      expect([...value]).toEqual([1, 2, 3])
      return token
    }
  }
  beforeEach(() => {
    db = new Database(':memory:')
  })
  afterEach(() => db.close())

  it('retains only an encrypted token on restart and never returns it to settings', async () => {
    const settings = new SettingsStore(db, secure)
    const original = settings.config()
    expect(original.enabled).toBe(false)
    const result = await settings.update({ enabled: true, serverUrl: 'https://example.org', token })
    expect(result).toMatchObject({ hasToken: true, deviceId: original.deviceId, enabled: true })
    expect(JSON.stringify(result)).not.toContain(token)
    expect(db.serialize().includes(Buffer.from(token))).toBe(false)
    expect(await new SettingsStore(db, secure).token()).toBe(token)
    const locked = new SettingsStore(db, unavailable)
    await expect(locked.token()).rejects.toThrow(/key store/)
    expect(locked.config().hasToken).toBe(true)
  })

  it('keeps an unsealable credential in memory only and never transfers it to another URL', async () => {
    const settings = new SettingsStore(db, unavailable)
    await settings.update({ serverUrl: 'https://one.example/sync', token, enabled: true })
    expect(await settings.token()).toBe(token)
    expect(db.serialize().includes(Buffer.from(token))).toBe(false)
    expect(await new SettingsStore(db, unavailable).token()).toBe('')
    await expect(settings.update({ serverUrl: 'https://two.example' })).rejects.toThrow(/token/)
    expect(await settings.token()).toBe(token)
    const changed = await settings.update({ enabled: false, serverUrl: 'https://two.example' })
    expect(changed.hasToken).toBe(false)
    expect(await settings.token()).toBe('')
  })

  it('keeps the old endpoint and cursor together if clearing its replication history fails', async () => {
    const settings = new SettingsStore(db, secure)
    await settings.update({ serverUrl: 'https://old.example', token })
    settings.succeeded(17)
    const before = settings.config()
    await expect(
      settings.update({ serverUrl: 'https://new.example', token: 'test-only-new-private-session-credential' }, () => {
        settings.clearStats()
        throw new Error('Reset failed')
      })
    ).rejects.toThrow('Reset failed')
    expect(settings.config()).toEqual(before)
    expect(settings.stats().serverRevision).toBe(17)
  })

  it('remembers offline backoff until a successful sync without rotating the device identity', () => {
    const settings = new SettingsStore(db, secure)
    const { deviceId } = settings.config()
    settings.failed('Offline', 100)
    const reopened = new SettingsStore(db, secure)
    expect(reopened.stats()).toMatchObject({ nextRetryAt: 100, attempts: 1 })
    reopened.succeeded(12)
    expect(reopened.stats()).toMatchObject({ serverRevision: 12, nextRetryAt: null, attempts: 0, lastError: null })
    reopened.clearStats()
    expect(reopened.stats()).toMatchObject({ serverRevision: 0, lastSyncedAt: null })
    expect(reopened.config().deviceId).toBe(deviceId)
  })
})
