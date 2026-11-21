import { setupTestDatabase } from '@test-helpers/db'
import { MockApplicationUtils } from '@test-mocks/main/application'
import { eq } from 'drizzle-orm'
import { expect, it, vi } from 'vitest'

import { preferenceTable } from '@data/db/schemas/preference'
import { userProviderTable } from '@data/db/schemas/userProvider'
import { PreferenceService } from '@data/PreferenceService'
import { BaseService } from '@main/core/lifecycle'
import { DefaultPreferences } from '@shared/data/preference/preferenceSchemas'
import { ThemeMode } from '@shared/data/preference/preferenceTypes'

import type { SyncChange } from '../entity'
import { libraryEntities } from '../libraryEntities'

vi.unmock('@main/data/PreferenceService')

const dbh = setupTestDatabase()

it('exports opted-in manual provider keys, never an OAuth session or the route for its credentials', () => {
  dbh.db
    .insert(userProviderTable)
    .values([
      {
        providerId: 'custom',
        name: 'Manual',
        orderKey: 'a0',
        apiKeys: [{ id: 'key', key: 'manual-test-key', isEnabled: true }],
        authConfig: { type: 'api-key' }
      },
      {
        providerId: 'oauth',
        name: 'Bound',
        orderKey: 'a1',
        authConfig: { type: 'oauth', clientId: 'local', accessToken: 'secret-from-sign-in' }
      }
    ])
    .run()
  const adapter = libraryEntities.find((a) => a.type === 'user_provider')!
  const manual = adapter.get(dbh.db, 'custom')!
  expect(manual.payload?.apiKeys).toEqual([{ id: 'key', key: 'manual-test-key', isEnabled: true }])
  expect(adapter.get(dbh.db, 'oauth')).toMatchObject({ withheld: true, payload: null })
  expect(JSON.stringify(adapter.scan(dbh.db))).not.toContain('secret-from-sign-in')
  const record: SyncChange = {
    entityId: 'oauth',
    entityType: 'user_provider',
    category: 'providers',
    deviceId: 'peer',
    updatedAt: 99,
    payload: { ...manual.payload, providerId: 'oauth' }
  }
  expect(adapter.apply(dbh.db, record)).toBe('deferred')
  expect(dbh.db.select().from(userProviderTable).where(eq(userProviderTable.providerId, 'oauth')).get()?.name).toBe(
    'Bound'
  )
})

it('refreshes preference subscribers after commit, not on rollback, and leaves reset preferences writable', async () => {
  const key = 'ui.theme_mode'
  const initial = DefaultPreferences.default[key]
  dbh.db.insert(preferenceTable).values({ key, scope: 'default', value: initial }).run()
  BaseService.resetInstances()
  const preferences = new PreferenceService()
  MockApplicationUtils.setService('PreferenceService', preferences)
  try {
    await preferences._doInit()
    const observed: unknown[] = []
    preferences.subscribeChange(key, (value) => observed.push(value))
    const adapter = libraryEntities.find((a) => a.type === 'preference')!
    const change: SyncChange = {
      entityId: JSON.stringify(['default', key]),
      entityType: 'preference',
      category: 'preferences',
      deviceId: 'elsewhere',
      updatedAt: 50,
      payload: { scope: 'default', key, value: ThemeMode.dark }
    }
    expect(() =>
      dbh.db.transaction((tx) => {
        adapter.apply(tx, change)
        throw new Error('rollback')
      })
    ).toThrow('rollback')
    expect(preferences.get(key)).toBe(initial)
    expect(observed).toEqual([])
    dbh.db.transaction((tx) => adapter.apply(tx, change))
    adapter.afterCommit!([change])
    expect(preferences.get(key)).toBe(ThemeMode.dark)
    expect(observed).toEqual([ThemeMode.dark])
    const removal: SyncChange = {
      entityId: change.entityId,
      entityType: 'preference',
      category: 'preferences',
      deviceId: 'elsewhere',
      updatedAt: 60,
      deleted: true,
      deletedAt: 60
    }
    dbh.db.transaction((tx) => adapter.apply(tx, removal))
    adapter.afterCommit!([removal])
    expect(preferences.get(key)).toBe(initial)
    expect(adapter.get(dbh.db, change.entityId)).toBeUndefined()
    await preferences.set(key, ThemeMode.light)
    expect(dbh.db.select().from(preferenceTable).where(eq(preferenceTable.key, key)).get()?.value).toBe(ThemeMode.light)
  } finally {
    DefaultPreferences.default[key] = initial
    await preferences._doStop()
    MockApplicationUtils.resetServices()
  }
})
