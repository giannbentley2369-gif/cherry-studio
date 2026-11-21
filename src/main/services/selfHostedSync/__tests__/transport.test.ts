import { createServer, type IncomingMessage, type Server, type ServerResponse } from 'node:http'
import type { AddressInfo } from 'node:net'
import { Readable } from 'node:stream'

import { afterEach, describe, expect, it, vi } from 'vitest'

import type { SyncChange } from '../entity'
import { normalizeServerUrl, SyncTransport, SyncTransportError } from '../transport'

const servers: Server[] = []
const token = 'transport-test-token-0123456789abcdef'

async function listen(handler: (request: IncomingMessage, response: ServerResponse) => void): Promise<string> {
  const server = createServer(handler)
  servers.push(server)
  await new Promise<void>((resolve, reject) => {
    server.once('error', reject)
    server.listen(0, '127.0.0.1', () => {
      server.off('error', reject)
      resolve()
    })
  })
  const { port } = server.address() as AddressInfo
  return `http://127.0.0.1:${port}`
}

async function readBody(request: IncomingMessage): Promise<Buffer> {
  const chunks: Buffer[] = []
  for await (const chunk of request) chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk))
  return Buffer.concat(chunks)
}

function json(response: ServerResponse, value: unknown, status = 200): void {
  response.writeHead(status, { 'content-type': 'application/json' })
  response.end(JSON.stringify(value))
}

function transport(url: string, signal = new AbortController().signal): SyncTransport {
  return new SyncTransport(url, token, signal)
}

afterEach(async () => {
  await Promise.all(
    servers.splice(0).map(
      (server) =>
        new Promise<void>((resolve) => {
          server.close(() => resolve())
          server.closeAllConnections()
        })
    )
  )
  vi.useRealTimers()
  vi.unstubAllGlobals()
})

describe('SyncTransport', () => {
  it('allows HTTPS and loopback HTTP, rejects unsafe server URLs without echoing them', () => {
    expect(normalizeServerUrl('http://127.0.0.1:8787/sync/')).toBe('http://127.0.0.1:8787/sync')
    expect(() => new SyncTransport('https://sync.example/team/', token, new AbortController().signal)).not.toThrow()
    expect(() => new SyncTransport('http://127.0.0.1:8787/sync', token, new AbortController().signal)).not.toThrow()
    expect(() => new SyncTransport('http://[::1]:8787/sync', token, new AbortController().signal)).not.toThrow()
    expect(() => new SyncTransport('https://sync.example', 'too-short', new AbortController().signal)).toThrow(
      SyncTransportError
    )

    for (const serverUrl of [
      'http://sync.example/',
      'https://alice:private-pass@sync.example/path?secret=query#fragment',
      'https://sync.example/path?secret=query',
      'https://sync.example/path#fragment'
    ]) {
      try {
        new SyncTransport(serverUrl, token, new AbortController().signal)
        throw new Error('Expected server URL validation to fail')
      } catch (error) {
        expect(error).toBeInstanceOf(SyncTransportError)
        expect((error as Error).message).not.toContain('private-pass')
        expect((error as Error).message).not.toContain('secret=query')
      }
    }
  })

  it('preserves HTTPS subpaths and sends authenticated status, push, and pull requests', async () => {
    const received: Array<{ method: string; url: string; authorization?: string }> = []
    let pushed: unknown
    const baseUrl = await listen((request, response) => {
      received.push({
        method: request.method ?? '',
        url: request.url ?? '',
        authorization: request.headers.authorization
      })
      if (request.url === '/workspace/api/v1/status')
        return json(response, { version: '1.0.0', revision: 19, stats: { changes: 0, blobs: 0 } })
      if (request.url === '/workspace/api/v1/push') {
        void readBody(request).then((body) => {
          pushed = JSON.parse(body.toString()) as unknown
          json(response, {
            changes: [
              {
                entityType: 'topic',
                entityId: 'topic-1',
                category: 'chats',
                deviceId: 'device-a',
                updatedAt: 42,
                payload: { name: 'Updated topic' },
                serverRev: 20
              }
            ],
            revision: 20
          })
        })
        return
      }
      if (request.url?.startsWith('/workspace/api/v1/pull?'))
        return json(response, {
          changes: [
            {
              entityType: 'topic',
              entityId: 'topic-1',
              category: 'chats',
              deviceId: 'device-a',
              updatedAt: 42,
              payload: { name: 'Updated topic' },
              serverRev: 20
            }
          ],
          cursor: 20,
          hasMore: false
        })
      response.writeHead(404).end()
    })
    const client = transport(`${baseUrl}/workspace`)
    const change: SyncChange = {
      entityType: 'topic',
      entityId: 'topic-1',
      category: 'chats',
      deviceId: 'device-a',
      updatedAt: 42,
      payload: { name: 'Updated topic' }
    }

    await expect(client.status()).resolves.toEqual({ version: '1.0.0', revision: 19 })
    await expect(client.push('device-a', [change])).resolves.toMatchObject({
      revision: 20,
      changes: [{ serverRev: 20 }]
    })
    await expect(client.pull(18, ['chats'], 5)).resolves.toMatchObject({ cursor: 20, hasMore: false })

    expect(pushed).toEqual({ deviceId: 'device-a', changes: [change] })
    expect(received.map(({ method, url }) => [method, url])).toEqual([
      ['GET', '/workspace/api/v1/status'],
      ['POST', '/workspace/api/v1/push'],
      ['GET', '/workspace/api/v1/pull?cursor=18&categories=chats&limit=5']
    ])
    expect(received.every(({ authorization }) => authorization === `Bearer ${token}`)).toBe(true)
  })

  it('rejects oversized batches and malformed server responses before returning data', async () => {
    let requestCount = 0
    const baseUrl = await listen((_request, response) => {
      requestCount++
      json(response, { version: '1.0.0' })
    })
    const client = transport(baseUrl)
    const changes = Array.from(
      { length: 101 },
      (_, index): SyncChange => ({
        entityType: 'topic',
        entityId: `topic-${index}`,
        category: 'chats',
        deviceId: 'device-a',
        updatedAt: 42,
        payload: { name: 'Topic' }
      })
    )

    await expect(client.push('device-a', changes)).rejects.toMatchObject({ code: 'invalid-input' })
    await expect(client.pull(0, ['chats'], 101)).rejects.toMatchObject({ code: 'invalid-input' })
    await expect(
      client.push('device-a', [
        {
          entityType: 'topic',
          entityId: 'large-topic',
          category: 'chats',
          deviceId: 'device-a',
          updatedAt: 42,
          payload: { content: 'x'.repeat(8 * 1024 * 1024) }
        }
      ])
    ).rejects.toMatchObject({ code: 'invalid-input' })
    expect(requestCount).toBe(0)
    await expect(client.status()).rejects.toMatchObject({ code: 'invalid-response' })
    expect(requestCount).toBe(1)
  })

  it('does not follow redirects or expose response bodies in errors', async () => {
    let redirectTargetRequests = 0
    const baseUrl = await listen((request, response) => {
      if (request.url === '/api/v1/redirect-target') redirectTargetRequests++
      response.writeHead(302, { location: '/api/v1/redirect-target?private=location-secret' })
      response.end('response-body-secret')
    })

    await expect(transport(baseUrl).status()).rejects.toSatisfy((error: SyncTransportError) => {
      expect(error.code).toBe('redirect')
      expect(error.message).not.toContain('location-secret')
      expect(error.message).not.toContain('response-body-secret')
      expect(error.message).not.toContain(token)
      return true
    })
    expect(redirectTargetRequests).toBe(0)
  })

  it('redacts server error payloads and bounds JSON response bodies', async () => {
    let responseMode: 'error' | 'oversized' = 'error'
    const baseUrl = await listen((_request, response) => {
      if (responseMode === 'error') {
        response.writeHead(500, { 'content-type': 'application/json' })
        response.end(JSON.stringify({ message: 'private-server-body' }))
        return
      }
      response.writeHead(200, { 'content-type': 'application/json', 'content-length': String(16 * 1024 * 1024 + 1) })
      response.end('{}')
    })
    const client = transport(baseUrl)

    await expect(client.status()).rejects.toSatisfy((error: SyncTransportError) => {
      expect(error.code).toBe('http')
      expect(error.offline).toBe(true)
      expect(error.message).not.toContain('private-server-body')
      expect(error.message).not.toContain(token)
      return true
    })
    responseMode = 'oversized'
    await expect(client.status()).rejects.toMatchObject({ code: 'invalid-response' })
  })

  it('streams blobs in both directions and treats only 404 as absent', async () => {
    let uploaded: Buffer = Buffer.alloc(0)
    let uploadSize = ''
    const missingHash = '1'.repeat(64)
    const baseUrl = await listen(async (request, response) => {
      const hash = decodeURIComponent((request.url ?? '').split('/').at(-1) ?? '')
      if (request.method === 'HEAD') {
        response.writeHead(hash === '0'.repeat(64) ? 200 : 404).end()
        return
      }
      if (request.method === 'PUT') {
        uploadSize = request.headers['content-length'] ?? ''
        uploaded = await readBody(request)
        response.writeHead(204).end()
        return
      }
      if (request.method === 'GET') {
        response.writeHead(200, { 'content-type': 'application/octet-stream' })
        response.write('stream-')
        response.end('contents')
        return
      }
      response.writeHead(405).end()
    })
    const client = transport(baseUrl)
    const hash = '0'.repeat(64)

    await expect(client.hasBlob(hash)).resolves.toBe(true)
    await expect(client.hasBlob(missingHash)).resolves.toBe(false)
    await client.putBlob(hash, Readable.from([Buffer.from('upload-'), Buffer.from('contents')]), 15)
    expect(uploadSize).toBe('15')
    expect(uploaded.toString()).toBe('upload-contents')

    const chunks: Buffer[] = []
    for await (const chunk of await client.getBlob(hash)) chunks.push(Buffer.from(chunk))
    expect(Buffer.concat(chunks).toString()).toBe('stream-contents')
  })

  it('sanitizes failures while a returned blob stream is being read', async () => {
    const baseUrl = await listen((_request, response) => {
      response.writeHead(200, { 'content-length': '20', connection: 'close' })
      response.end('truncated')
    })
    const stream = await transport(baseUrl).getBlob('2'.repeat(64))
    await expect(async () => {
      for await (const chunk of stream) {
        // Consume the response to observe the truncated transfer.
        void chunk
      }
    }).rejects.toSatisfy((error: SyncTransportError) => {
      expect(error).toBeInstanceOf(SyncTransportError)
      expect(error.message).not.toContain(baseUrl)
      expect(error.message).not.toContain(token)
      return true
    })
  })

  it('honors caller cancellation and request deadlines', async () => {
    let requestReceived!: () => void
    const received = new Promise<void>((resolve) => {
      requestReceived = resolve
    })
    const baseUrl = await listen(() => requestReceived())
    const controller = new AbortController()
    const pending = transport(baseUrl, controller.signal).status()
    await received
    controller.abort('abort-reason-secret')
    await expect(pending).rejects.toSatisfy((error: SyncTransportError) => {
      expect(error.code).toBe('aborted')
      expect(error.message).not.toContain('abort-reason-secret')
      return true
    })

    vi.useFakeTimers()
    vi.stubGlobal(
      'fetch',
      (_url: URL | RequestInfo, init?: RequestInit) =>
        new Promise<Response>((_resolve, reject) => {
          init?.signal?.addEventListener('abort', () => reject(init.signal?.reason), { once: true })
        })
    )
    const timeoutClient = transport('https://sync.example')
    const timeoutAssertion = expect(timeoutClient.status()).rejects.toMatchObject({ code: 'timeout' })
    await vi.advanceTimersByTimeAsync(30_000)
    await timeoutAssertion
  })

  it('reconnects named revision events with the last event id and ignores keepalive comments', async () => {
    const controller = new AbortController()
    let requestCount = 0
    let resumedFrom: string | undefined
    const baseUrl = await listen((request, response) => {
      requestCount++
      response.writeHead(200, { 'content-type': 'text/event-stream' })
      if (requestCount === 1) {
        response.write('id: 7\revent: revision\rdata: {"revision":7,"sourceDeviceId":"device-a"}\r\r')
        response.end()
        return
      }
      const eventId = request.headers['last-event-id']
      resumedFrom = Array.isArray(eventId) ? eventId[0] : eventId
      response.write(': keepalive\n\n')
      response.write('event: revision\ndata: {"revision":8,"sourceDeviceId":"device-b"}\n\n')
    })
    const events: Array<{ revision: number; sourceDeviceId: string }> = []

    await transport(baseUrl, controller.signal).events((event) => {
      events.push(event)
      if (event.revision === 8) controller.abort()
    })

    expect(events).toEqual([
      { revision: 7, sourceDeviceId: 'device-a' },
      { revision: 8, sourceDeviceId: 'device-b' }
    ])
    expect(requestCount).toBe(2)
    expect(resumedFrom).toBe('7')
  })

  it('rejects oversized SSE lines instead of buffering them indefinitely', async () => {
    const baseUrl = await listen((_request, response) => {
      response.writeHead(200, { 'content-type': 'text/event-stream' })
      response.end(`${'x'.repeat(16 * 1024 + 1)}\n`)
    })

    await expect(transport(baseUrl).events(() => {})).rejects.toMatchObject({ code: 'invalid-response' })
  })
})
