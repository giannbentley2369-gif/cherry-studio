import assert from 'node:assert/strict'
import { execFile } from 'node:child_process'
import { createHash } from 'node:crypto'
import { mkdtemp, readFile, readdir, rm, stat } from 'node:fs/promises'
import { request as httpRequest } from 'node:http'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import test from 'node:test'
import { fileURLToPath } from 'node:url'
import { promisify } from 'node:util'

import { readServerOptions, startSyncServer, type RunningServer, type ServerOptions } from './server.js'
import type { Category, IncomingChange, Winner } from './types.js'

const TEST_TOKEN = 'a'.repeat(64)
const AUTH_HEADERS = { authorization: 'Bearer ' + TEST_TOKEN }
const execFileAsync = promisify(execFile)

interface JsonResponse {
  response: Response
  body: Record<string, unknown>
}

async function withServer(
  run: (server: RunningServer, dataDir: string) => Promise<void>,
  options: Partial<Omit<ServerOptions, 'dataDir'>> = {}
): Promise<void> {
  const dataDir = await mkdtemp(join(tmpdir(), 'sync-server-test-'))
  let running: RunningServer | undefined
  try {
    running = await startSyncServer({
      host: '127.0.0.1',
      port: 0,
      dataDir,
      token: TEST_TOKEN,
      ...options
    })
    await run(running, dataDir)
  } finally {
    if (running) await running.close()
    await rm(dataDir, { recursive: true, force: true })
  }
}

async function apiRequest(
  server: RunningServer,
  path: string,
  init: RequestInit = {},
  token: string | null = TEST_TOKEN
): Promise<JsonResponse> {
  const headers = new Headers(init.headers)
  if (token !== null) headers.set('authorization', 'Bearer ' + token)
  const response = await fetch(server.url + path, { ...init, headers })
  return { response, body: (await response.json()) as Record<string, unknown> }
}

function upsert(
  deviceId: string,
  entityId: string,
  updatedAt: number,
  payload: Record<string, unknown> = { value: entityId },
  category: Category = 'chats',
  entityType = 'chat'
): IncomingChange {
  return { category, entityType, entityId, deviceId, updatedAt, payload }
}

async function push(
  server: RunningServer,
  deviceId: string,
  changes: IncomingChange[],
  token: string | null = TEST_TOKEN
): Promise<JsonResponse> {
  return apiRequest(
    server,
    '/api/v1/push',
    {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ deviceId, changes })
    },
    token
  )
}

async function chunkedJsonPost(server: RunningServer, chunks: Buffer[]): Promise<{ status: number; body: string }> {
  const address = new URL(server.url)
  return new Promise((resolveRequest, rejectRequest) => {
    const request = httpRequest(
      {
        hostname: address.hostname,
        port: Number(address.port),
        path: '/api/v1/push',
        method: 'POST',
        headers: {
          authorization: AUTH_HEADERS.authorization,
          'content-type': 'application/json',
          'transfer-encoding': 'chunked'
        }
      },
      (response) => {
        const parts: Buffer[] = []
        response.on('data', (chunk: Buffer | string) => parts.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk)))
        response.on('end', () => {
          resolveRequest({
            status: response.statusCode ?? 0,
            body: Buffer.concat(parts).toString('utf8')
          })
        })
      }
    )
    request.on('error', rejectRequest)
    for (const chunk of chunks) request.write(chunk)
    request.end()
  })
}

void test('only health is public; protected routes require a bearer token', async () => {
  await withServer(async (server) => {
    const health = await fetch(server.url + '/api/v1/health')
    assert.equal(health.status, 200)
    assert.deepEqual(await health.json(), { status: 'ok' })

    for (const path of ['/api/v1/status', '/api/v1/pull', '/api/v1/push', '/api/v1/events']) {
      const result = await apiRequest(server, path, {}, null)
      assert.equal(result.response.status, 401, path)
      assert.equal((result.body.error as Record<string, unknown>).code, 'unauthorized')
    }

    const healthMethod = await apiRequest(server, '/api/v1/health', { method: 'POST' }, null)
    assert.equal(healthMethod.response.status, 401)
  })
})

void test('generated bearer token is private, shown once, and stable across restart', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'sync-server-token-'))
  let first: RunningServer | undefined
  let second: RunningServer | undefined
  try {
    first = await startSyncServer({ host: '127.0.0.1', port: 0, dataDir })
    assert.match(first.initialToken ?? '', /^[a-f0-9]{64}$/)
    const tokenPath = join(dataDir, 'token.txt')
    assert.equal((await stat(tokenPath)).mode & 0o777, 0o600)
    assert.equal(await readFile(tokenPath, 'utf8'), first.initialToken)

    const healthBody = JSON.stringify(await (await fetch(first.url + '/api/v1/health')).json())
    assert.equal(healthBody.includes(first.initialToken!), false)
    const firstToken = first.initialToken!
    const saved = await push(first, 'device-a', [upsert('device-a', 'saved-entity', 10)], firstToken)
    assert.equal(saved.response.status, 200)
    await first.close()
    first = undefined

    second = await startSyncServer({ host: '127.0.0.1', port: 0, dataDir })
    assert.equal(second.initialToken, undefined)
    const authorized = await apiRequest(second, '/api/v1/status', {}, firstToken)
    assert.equal(authorized.response.status, 200)
    assert.equal(authorized.body.revision, 1)
  } finally {
    if (first) await first.close()
    if (second) await second.close()
    await rm(dataDir, { recursive: true, force: true })
  }
})

void test('environment configuration accepts an explicit bearer secret and numeric limits', () => {
  const options = readServerOptions({
    SYNC_TOKEN: TEST_TOKEN,
    SYNC_BIND: '127.0.0.1',
    SYNC_PORT: '9001',
    SYNC_MAX_JSON_BYTES: '4096',
    SYNC_MAX_BLOB_MB: '2',
    SYNC_MAX_STORAGE_BYTES: '65536'
  })
  assert.equal(options.token, TEST_TOKEN)
  assert.equal(options.port, 9001)
  assert.equal(options.maxJsonBytes, 4096)
  assert.equal(options.maxBlobBytes, 2 * 1024 * 1024)
  assert.equal(options.maxStorageBytes, 65_536)
  assert.equal(readServerOptions({ SYNC_TOKEN: '' }).token, undefined)
  assert.throws(() => readServerOptions({ SYNC_PORT: 'port' }), /SYNC_PORT/)
})

void test('push returns the authoritative winner and does not revise equal or stale changes', async () => {
  await withServer(async (server) => {
    const initial = await push(server, 'device-10', [upsert('device-10', 'entity-a', 100, { value: 'winner' })])
    assert.equal(initial.response.status, 200)
    const initialBody = initial.body as unknown as {
      revision: number
      changes: Winner[]
    }
    assert.equal(initialBody.revision, 1)
    assert.equal(initialBody.changes[0]?.serverRev, 1)

    const stale = await push(server, 'device-z', [upsert('device-z', 'entity-a', 99, { value: 'stale' })])
    const staleBody = stale.body as unknown as {
      revision: number
      changes: Winner[]
    }
    assert.equal(staleBody.revision, 1)
    assert.deepEqual(staleBody.changes[0], initialBody.changes[0])

    const equal = await push(server, 'device-10', [upsert('device-10', 'entity-a', 100, { value: 'equal-loser' })])
    const equalBody = equal.body as unknown as {
      revision: number
      changes: Winner[]
    }
    assert.equal(equalBody.revision, 1)
    assert.deepEqual(equalBody.changes[0], initialBody.changes[0])

    const tie = await push(server, 'device-9', [upsert('device-9', 'entity-a', 100, { value: 'ascii-tie-winner' })])
    const tieBody = tie.body as unknown as {
      revision: number
      changes: Winner[]
    }
    assert.equal(tieBody.revision, 2)
    assert.equal(tieBody.changes[0]?.deviceId, 'device-9')
    assert.equal(tieBody.changes[0]?.serverRev, 2)

    const finalStatus = await apiRequest(server, '/api/v1/status')
    assert.equal(finalStatus.body.revision, 2)
  })
})

void test('push validates categories and per-change device identity before applying a batch', async () => {
  await withServer(async (server) => {
    const invalidDevice = await push(server, 'device-a', [upsert('device-b', 'entity-a', 1)])
    assert.equal(invalidDevice.response.status, 400)

    const unsupportedCategory = await apiRequest(server, '/api/v1/push', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        deviceId: 'device-a',
        changes: [{ ...upsert('device-a', 'entity-a', 1), category: 'unknown' }]
      })
    })
    assert.equal(unsupportedCategory.response.status, 400)

    const first = await push(server, 'device-a', [upsert('device-a', 'entity-a', 1)])
    assert.equal(first.response.status, 200)
    const categoryConflict = await push(server, 'device-a', [
      upsert('device-a', 'entity-new', 2),
      upsert('device-a', 'entity-a', 2, {}, 'files')
    ])
    assert.equal(categoryConflict.response.status, 409)
    const status = await apiRequest(server, '/api/v1/status')
    assert.equal(status.body.revision, 1)
    const afterRollback = await apiRequest(server, '/api/v1/pull?since=1')
    assert.deepEqual(afterRollback.body.changes, [])

    const tooMany = await push(
      server,
      'device-a',
      Array.from({ length: 101 }, (_, index) => upsert('device-a', 'batch-' + index, 1))
    )
    assert.equal(tooMany.response.status, 400)
  })
})

void test('pull advances through filtered revisions and paginates with a stable global cursor', async () => {
  await withServer(async (server) => {
    await push(server, 'device-a', [upsert('device-a', 'chat-1', 1)])
    await push(server, 'device-b', [upsert('device-b', 'provider-1', 2, {}, 'providers', 'provider')])
    await push(server, 'device-c', [upsert('device-c', 'agent-1', 3, {}, 'agents', 'agent')])

    const first = await apiRequest(server, '/api/v1/pull?since=0&categories=chats&limit=1')
    const firstBody = first.body as unknown as {
      changes: Winner[]
      cursor: number
      hasMore: boolean
    }
    assert.deepEqual(
      firstBody.changes.map((change) => change.entityId),
      ['chat-1']
    )
    assert.equal(firstBody.cursor, 1)
    assert.equal(firstBody.hasMore, true)

    const second = await apiRequest(server, '/api/v1/pull?since=' + firstBody.cursor + '&categories=chats&limit=1')
    const secondBody = second.body as unknown as {
      changes: Winner[]
      cursor: number
      hasMore: boolean
    }
    assert.deepEqual(secondBody.changes, [])
    assert.equal(secondBody.cursor, 2)
    assert.equal(secondBody.hasMore, true)

    const third = await apiRequest(server, '/api/v1/pull?since=' + secondBody.cursor + '&categories=chats&limit=1')
    const thirdBody = third.body as unknown as {
      changes: Winner[]
      cursor: number
      hasMore: boolean
    }
    assert.deepEqual(thirdBody.changes, [])
    assert.equal(thirdBody.cursor, 3)
    assert.equal(thirdBody.hasMore, false)

    await push(server, 'device-d', [upsert('device-d', 'chat-2', 4)])
    const fourth = await apiRequest(server, '/api/v1/pull?since=' + thirdBody.cursor + '&categories=chats')
    const fourthBody = fourth.body as unknown as {
      changes: Winner[]
      cursor: number
      hasMore: boolean
    }
    assert.deepEqual(
      fourthBody.changes.map((change) => change.entityId),
      ['chat-2']
    )
    assert.equal(fourthBody.changes[0]?.serverRev, 4)
    assert.equal(fourthBody.cursor, 4)
  })
})

void test('tombstones are versioned winners and remain in pull history', async () => {
  await withServer(async (server) => {
    await push(server, 'device-a', [upsert('device-a', 'entity-a', 10)])
    const deletedAt = 12
    const result = await push(server, 'device-b', [
      {
        category: 'chats',
        entityType: 'chat',
        entityId: 'entity-a',
        deviceId: 'device-b',
        updatedAt: 12,
        deleted: true,
        deletedAt
      }
    ])
    const body = result.body as unknown as {
      revision: number
      changes: Winner[]
    }
    assert.equal(body.revision, 2)
    assert.equal(body.changes[0]?.deleted, true)
    assert.equal(body.changes[0]?.deletedAt, deletedAt)
    assert.equal('payload' in (body.changes[0] ?? {}), false)

    const pull = await apiRequest(server, '/api/v1/pull?since=1')
    const changes = pull.body.changes as Winner[]
    assert.equal(changes[0]?.deleted, true)
    assert.equal(changes[0]?.serverRev, 2)
  })
})

void test('JSON request limits apply to declared and chunked bodies', async () => {
  await withServer(
    async (server) => {
      const declared = await apiRequest(server, '/api/v1/push', {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({
          deviceId: 'a',
          changes: [],
          padding: 'x'.repeat(200)
        })
      })
      assert.equal(declared.response.status, 413)

      const chunked = await chunkedJsonPost(server, [
        Buffer.from('{"deviceId":"a","changes":[],"padding":"'),
        Buffer.alloc(200, 'x'),
        Buffer.from('"}')
      ])
      assert.equal(chunked.status, 413)
      assert.match(chunked.body, /request_too_large/)
    },
    { maxJsonBytes: 128 }
  )
})

void test('pull reduces a page when winners would exceed the bounded response size', async () => {
  await withServer(
    async (server) => {
      for (let index = 0; index < 4; index += 1) {
        const result = await push(server, 'device-a', [
          upsert('device-a', 'large-' + index, index + 1, {
            value: 'x'.repeat(250)
          })
        ])
        assert.equal(result.response.status, 200)
      }

      const result = await apiRequest(server, '/api/v1/pull?since=0&limit=4')
      assert.equal(result.response.status, 200)
      assert.ok(Buffer.byteLength(JSON.stringify(result.body)) <= 1024)
      assert.equal(result.body.hasMore, true)
      assert.ok((result.body.changes as Winner[]).length < 4)
    },
    { maxJsonBytes: 512 }
  )
})

void test('blob PUT streams, verifies, deduplicates, and serves content-addressed bytes', async () => {
  await withServer(async (server, dataDir) => {
    const bytes = Buffer.from('attachment bytes for a local sync test')
    const sha256 = createHash('sha256').update(bytes).digest('hex')
    const path = '/api/v1/blobs/' + sha256
    const headers = {
      ...AUTH_HEADERS,
      'content-type': 'application/octet-stream'
    }

    const missing = await fetch(server.url + path, { method: 'HEAD', headers })
    assert.equal(missing.status, 404)
    assert.equal(await missing.text(), '')

    const first = await fetch(server.url + path, {
      method: 'PUT',
      headers,
      body: bytes
    })
    assert.equal(first.status, 201)
    assert.deepEqual(await first.json(), {
      sha256,
      size: bytes.length,
      duplicate: false
    })

    const duplicate = await fetch(server.url + path, {
      method: 'PUT',
      headers,
      body: bytes
    })
    assert.equal(duplicate.status, 200)
    assert.deepEqual(await duplicate.json(), {
      sha256,
      size: bytes.length,
      duplicate: true
    })

    const found = await fetch(server.url + path, { method: 'HEAD', headers })
    assert.equal(found.status, 200)
    assert.equal(found.headers.get('content-length'), String(bytes.length))
    assert.equal(await found.text(), '')

    const download = await fetch(server.url + path, { headers: AUTH_HEADERS })
    assert.equal(download.status, 200)
    assert.deepEqual(Buffer.from(await download.arrayBuffer()), bytes)
    assert.equal(download.headers.get('etag'), '"' + sha256 + '"')

    const wrongHash = '0'.repeat(64)
    const rejected = await fetch(server.url + '/api/v1/blobs/' + wrongHash, {
      method: 'PUT',
      headers,
      body: bytes
    })
    assert.equal(rejected.status, 422)
    assert.match(await rejected.text(), /blob_hash_mismatch/)

    const status = await apiRequest(server, '/api/v1/status')
    const stats = status.body.stats as Record<string, number>
    assert.equal(stats.blobs, 1)
    assert.equal(stats.blobBytes, bytes.length)
    assert.deepEqual(await readdir(join(dataDir, 'blobs')), [sha256])
  })
})

void test('blob size and storage limits reject uploads without leaving partial files', async () => {
  const bytes = Buffer.from('five!')
  const sha256 = createHash('sha256').update(bytes).digest('hex')

  await withServer(
    async (server, dataDir) => {
      const response = await fetch(server.url + '/api/v1/blobs/' + sha256, {
        method: 'PUT',
        headers: {
          ...AUTH_HEADERS,
          'content-type': 'application/octet-stream'
        },
        body: bytes
      })
      assert.equal(response.status, 413)
      assert.deepEqual(await readdir(join(dataDir, 'blobs')), [])
    },
    { maxBlobBytes: 4 }
  )

  await withServer(
    async (server, dataDir) => {
      const large = Buffer.alloc(4096, 7)
      const hash = createHash('sha256').update(large).digest('hex')
      const response = await fetch(server.url + '/api/v1/blobs/' + hash, {
        method: 'PUT',
        headers: {
          ...AUTH_HEADERS,
          'content-type': 'application/octet-stream'
        },
        body: large
      })
      assert.equal(response.status, 507)
      assert.deepEqual(await readdir(join(dataDir, 'blobs')), [])

      const pushResponse = await push(server, 'device-a', [upsert('device-a', 'quota-check', 1)])
      assert.equal(pushResponse.response.status, 507)
      assert.equal((await apiRequest(server, '/api/v1/status')).body.revision, 0)
    },
    { maxStorageBytes: 65_536 }
  )
})

void test('revision events contain only committed winners and require authentication', async () => {
  await withServer(async (server) => {
    const controller = new AbortController()
    const response = await fetch(server.url + '/api/v1/events', {
      headers: AUTH_HEADERS,
      signal: controller.signal
    })
    assert.equal(response.status, 200)
    assert.match(response.headers.get('content-type') ?? '', /^text\/event-stream/)
    const reader = response.body!.getReader()
    const decoder = new TextDecoder()
    const connected = decoder.decode((await reader.read()).value)
    assert.match(connected, /: connected/)

    const pushed = await push(server, 'device-a', [upsert('device-a', 'event-entity', 5)])
    assert.equal(pushed.response.status, 200)
    const event = decoder.decode((await reader.read()).value)
    assert.match(event, /event: revision/)
    assert.match(event, /"revision":1/)
    assert.match(event, /"sourceDeviceId":"device-a"/)
    controller.abort()
    await reader.cancel().catch(() => undefined)
  })
})

void test('two-device smoke check pulls each peer winner and tombstones its test entity', async () => {
  await withServer(async (server) => {
    const scriptPath = fileURLToPath(new URL('../scripts/two-device-check.mjs', import.meta.url))
    const result = await execFileAsync(process.execPath, [scriptPath], {
      env: { SYNC_TEST_URL: server.url, SYNC_TEST_TOKEN: TEST_TOKEN },
      timeout: 5_000
    })
    assert.match(result.stdout, /Two-device sync check passed/)
    const status = await apiRequest(server, '/api/v1/status')
    assert.equal(status.body.revision, 3)
    assert.equal((status.body.stats as Record<string, number>).tombstones, 1)
  })
})
