import { once } from 'node:events'
import { createServer } from 'node:http'
import type { AddressInfo } from 'node:net'
import { dirname, join } from 'node:path'

import { setupTestDatabase } from '@test-helpers/db'
import Database from 'better-sqlite3'
import { expect, it, vi } from 'vitest'

import { application } from '@application'
import { BaseService } from '@main/core/lifecycle'

import { SelfHostedSyncService } from '../SelfHostedSyncService'
import { SettingsStore } from '../settingsStore'

const dbh = setupTestDatabase()

it('never tests an edited endpoint with the saved credential, even while sync is disabled', async () => {
  const path = join(dirname(dbh.sqlite.name), 'self-hosted-sync.sqlite')
  const db = new Database(path)
  const store = new SettingsStore(db, {
    available: async () => true,
    encrypt: async () => Buffer.from([42]),
    decrypt: async () => 'old-test-server-credential-never-forwarded'
  })
  await store.update({ serverUrl: 'https://previous.example', token: 'old-test-server-credential-never-forwarded' })
  db.close()
  const getPath = vi.spyOn(application, 'getPath').mockReturnValue(path)
  const received: string[] = []
  const server = createServer((req, res) => {
    received.push(req.headers.authorization ?? '')
    res.writeHead(200, { 'Content-Type': 'application/json' }).end(JSON.stringify({ version: '1.0', revision: 0 }))
  })
  BaseService.resetInstances()
  const service = new SelfHostedSyncService()
  try {
    server.listen(0, '127.0.0.1')
    await once(server, 'listening')
    const serverUrl = `http://127.0.0.1:${(server.address() as AddressInfo).port}`
    expect((await service.syncNow()).state).toBe('disabled')
    expect(await service.testConnection({ serverUrl })).toMatchObject({ ok: false })
    expect(received).toEqual([])
    expect(
      await service.testConnection({ serverUrl, token: 'new-test-server-credential-just-for-this-origin' })
    ).toEqual({ ok: true })
    expect(received).toEqual(['Bearer new-test-server-credential-just-for-this-origin'])
    expect(service.getConfig().serverUrl).toContain('previous.example')
  } finally {
    await service._doStop()
    getPath.mockRestore()
    server.closeAllConnections()
    await new Promise<void>((resolve) => server.close(() => resolve()))
  }
})
