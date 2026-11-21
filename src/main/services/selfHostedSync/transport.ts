import { isIP } from 'node:net'
import { Readable } from 'node:stream'

import { SYNC_CATEGORIES, type SyncCategory } from '@shared/ipc/schemas/selfHostedSync'

import type { Payload, RemoteChange, SyncChange } from './entity'

const MAX_CHANGES = 100
const MAX_JSON_BYTES = 8 * 1024 * 1024
const MAX_SSE_LINE_BYTES = 16 * 1024
const MAX_SSE_EVENT_BYTES = 64 * 1024
const MAX_EVENT_ID_LENGTH = 256
const BLOB_HASH_PATTERN = /^[a-f0-9]{64}$/
export function assertSyncToken(token: string): void {
  if (!/^[A-Za-z0-9._~+/-]{32,512}$/.test(token)) throw new Error('Enter a valid 32–512 character sync server token.')
}
const REQUEST_TIMEOUT_MS = 30_000
const BLOB_UPLOAD_TIMEOUT_MS = 5 * 60_000
const STREAM_IDLE_TIMEOUT_MS = 60_000
const RECONNECT_BASE_DELAY_MS = 250
const RECONNECT_MAX_DELAY_MS = 10_000

export type SyncTransportErrorCode =
  | 'aborted'
  | 'consumer'
  | 'http'
  | 'invalid-input'
  | 'invalid-response'
  | 'network'
  | 'redirect'
  | 'timeout'

const ERROR_MESSAGES: Record<Exclude<SyncTransportErrorCode, 'http'>, string> = {
  aborted: 'Self-hosted sync request was aborted',
  consumer: 'Self-hosted sync event consumer failed',
  'invalid-input': 'Invalid self-hosted sync request',
  'invalid-response': 'Invalid self-hosted sync response',
  network: 'Self-hosted sync network request failed',
  redirect: 'Self-hosted sync server redirected the request',
  timeout: 'Self-hosted sync request timed out'
}

export class SyncTransportError extends Error {
  readonly offline: boolean

  constructor(
    readonly code: SyncTransportErrorCode,
    readonly status?: number
  ) {
    super(code === 'http' ? `Self-hosted sync server returned HTTP ${status}` : ERROR_MESSAGES[code])
    this.name = 'SyncTransportError'
    this.offline =
      code === 'network' ||
      code === 'timeout' ||
      (code === 'http' && (status === 408 || (status !== undefined && status >= 500)))
  }
}

interface RequestScope {
  signal: AbortSignal
  timedOut(): boolean
  clearTimeout(): void
  timeoutNow(): void
  dispose(): void
}

function requestScope(parent: AbortSignal, timeoutMs: number): RequestScope {
  const controller = new AbortController()
  let didTimeout = false
  let timer: ReturnType<typeof setTimeout> | undefined
  const onAbort = () => controller.abort()

  if (parent.aborted) controller.abort()
  else parent.addEventListener('abort', onAbort, { once: true })

  timer = setTimeout(() => {
    didTimeout = true
    controller.abort()
  }, timeoutMs)

  return {
    signal: controller.signal,
    timedOut: () => didTimeout,
    clearTimeout() {
      if (timer !== undefined) clearTimeout(timer)
      timer = undefined
    },
    timeoutNow() {
      didTimeout = true
      controller.abort()
    },
    dispose() {
      if (timer !== undefined) clearTimeout(timer)
      timer = undefined
      parent.removeEventListener('abort', onAbort)
    }
  }
}

function transportError(error: unknown, scope: RequestScope, parent: AbortSignal): SyncTransportError {
  if (error instanceof SyncTransportError) return error
  if (parent.aborted) return new SyncTransportError('aborted')
  if (scope.timedOut()) return new SyncTransportError('timeout')
  return new SyncTransportError('network')
}

function invalidInput(): never {
  throw new SyncTransportError('invalid-input')
}

function invalidResponse(): never {
  throw new SyncTransportError('invalid-response')
}

function isRecord(value: unknown): value is Record<string, unknown> {
  if (value === null || typeof value !== 'object' || Array.isArray(value)) return false
  const prototype = Object.getPrototypeOf(value)
  return prototype === Object.prototype || prototype === null
}

function hasKeys(value: Record<string, unknown>, allowed: readonly string[], required: readonly string[]): boolean {
  return Object.keys(value).every((key) => allowed.includes(key)) && required.every((key) => Object.hasOwn(value, key))
}

function isNonemptyString(value: unknown, maxLength = 512): value is string {
  return typeof value === 'string' && value.length > 0 && value.length <= maxLength
}

function isPayload(value: unknown): value is Payload {
  return isRecord(value)
}

function isRevision(value: unknown): value is number {
  return typeof value === 'number' && Number.isSafeInteger(value) && value >= 0
}

function isSyncCategory(value: unknown): value is SyncCategory {
  return typeof value === 'string' && (SYNC_CATEGORIES as readonly string[]).includes(value)
}

function isBlobHash(value: unknown): value is string {
  return typeof value === 'string' && BLOB_HASH_PATTERN.test(value)
}

function isSyncChange(value: unknown, remote: false): value is SyncChange
function isSyncChange(value: unknown, remote: true): value is RemoteChange
function isSyncChange(value: unknown, remote: boolean): value is SyncChange | RemoteChange {
  if (!isRecord(value)) return false

  const common = ['entityType', 'entityId', 'category', 'deviceId', 'updatedAt']
  const revision = remote ? ['serverRev'] : []
  if (
    !isNonemptyString(value.entityType) ||
    !isNonemptyString(value.entityId) ||
    !isSyncCategory(value.category) ||
    !isNonemptyString(value.deviceId) ||
    !isRevision(value.updatedAt) ||
    (remote && !isRevision(value.serverRev))
  )
    return false

  if (value.deleted === true) {
    const fields = [...common, ...revision, 'deleted', 'deletedAt']
    return hasKeys(value, fields, fields) && isRevision(value.deletedAt)
  }

  const hasDeletedFlag = Object.hasOwn(value, 'deleted')
  const fields = [...common, ...revision, 'payload', ...(hasDeletedFlag ? ['deleted'] : [])]
  return (
    hasKeys(value, fields, [...common, ...revision, 'payload']) &&
    (!hasDeletedFlag || value.deleted === false) &&
    isPayload(value.payload)
  )
}

function isLoopbackHostname(hostname: string): boolean {
  const host = hostname.replace(/^\[|\]$/g, '').toLowerCase()
  if (host === 'localhost' || host === 'localhost.' || host.endsWith('.localhost') || host.endsWith('.localhost.'))
    return true

  const family = isIP(host)
  if (family === 4) return Number(host.split('.')[0]) === 127
  return family === 6 && host === '::1'
}

function normalizeServerBase(serverUrl: string): URL {
  let url: URL
  try {
    url = new URL(serverUrl)
  } catch {
    return invalidInput()
  }

  if (
    (url.protocol !== 'https:' && url.protocol !== 'http:') ||
    (url.protocol === 'http:' && !isLoopbackHostname(url.hostname)) ||
    url.username ||
    url.password ||
    url.search ||
    url.hash
  )
    return invalidInput()

  const path = url.pathname.replace(/\/+$/, '')
  url.pathname = path ? `${path}/` : '/'
  return url
}

export function normalizeServerUrl(serverUrl: string): string {
  return normalizeServerBase(serverUrl).href.replace(/\/$/, '')
}

async function discardBody(response: Response): Promise<void> {
  try {
    await response.body?.cancel()
  } catch {
    // Response cleanup must not replace the transport result.
  }
}

async function requireSuccessfulResponse(response: Response): Promise<void> {
  if (response.status >= 300 && response.status < 400) {
    await discardBody(response)
    throw new SyncTransportError('redirect', response.status)
  }
  if (!response.ok) {
    await discardBody(response)
    throw new SyncTransportError('http', response.status)
  }
}

async function readJson(response: Response): Promise<unknown> {
  const contentLength = response.headers.get('content-length')
  if (contentLength !== null) {
    if (!/^\d+$/.test(contentLength) || Number(contentLength) > MAX_JSON_BYTES) {
      await discardBody(response)
      return invalidResponse()
    }
  }

  if (!response.body) return invalidResponse()
  const reader = response.body.getReader()
  const chunks: Uint8Array[] = []
  let totalBytes = 0

  try {
    while (true) {
      const { done, value } = await reader.read()
      if (done) break
      totalBytes += value.byteLength
      if (totalBytes > MAX_JSON_BYTES) {
        await reader.cancel()
        return invalidResponse()
      }
      chunks.push(value)
    }
  } finally {
    reader.releaseLock()
  }

  let text: string
  try {
    text = new TextDecoder('utf-8', { fatal: true }).decode(Buffer.concat(chunks, totalBytes))
  } catch {
    return invalidResponse()
  }

  try {
    return JSON.parse(text) as unknown
  } catch {
    return invalidResponse()
  }
}

function serializeJson(value: unknown): string {
  let body: string | undefined
  try {
    body = JSON.stringify(value)
  } catch {
    return invalidInput()
  }
  if (body === undefined || Buffer.byteLength(body) > MAX_JSON_BYTES) return invalidInput()
  return body
}

function parseStatus(value: unknown): { version: string; revision: number } {
  if (
    !isRecord(value) ||
    !hasKeys(value, ['version', 'revision', 'stats'], ['version', 'revision']) ||
    (Object.hasOwn(value, 'stats') && !isRecord(value.stats)) ||
    !isNonemptyString(value.version, 128) ||
    !isRevision(value.revision)
  )
    return invalidResponse()
  return { version: value.version, revision: value.revision }
}

function parsePush(value: unknown): { changes: RemoteChange[]; revision: number } {
  if (
    !isRecord(value) ||
    !hasKeys(value, ['changes', 'revision'], ['changes', 'revision']) ||
    !Array.isArray(value.changes) ||
    value.changes.length > MAX_CHANGES ||
    !value.changes.every((change) => isSyncChange(change, true)) ||
    !isRevision(value.revision)
  )
    return invalidResponse()
  return { changes: value.changes, revision: value.revision }
}

function parsePull(value: unknown, limit: number): { changes: RemoteChange[]; cursor: number; hasMore: boolean } {
  if (
    !isRecord(value) ||
    !hasKeys(value, ['changes', 'cursor', 'hasMore'], ['changes', 'cursor', 'hasMore']) ||
    !Array.isArray(value.changes) ||
    value.changes.length > limit ||
    !value.changes.every((change) => isSyncChange(change, true)) ||
    !isRevision(value.cursor) ||
    typeof value.hasMore !== 'boolean'
  )
    return invalidResponse()
  return { changes: value.changes, cursor: value.cursor, hasMore: value.hasMore }
}

async function readWithIdleTimeout<T>(
  reader: ReadableStreamDefaultReader<T>,
  scope: RequestScope
): Promise<ReadableStreamReadResult<T>> {
  let timer: ReturnType<typeof setTimeout> | undefined
  try {
    return await Promise.race([
      reader.read(),
      new Promise<never>((_resolve, reject) => {
        timer = setTimeout(() => {
          scope.timeoutNow()
          reject(new SyncTransportError('timeout'))
        }, STREAM_IDLE_TIMEOUT_MS)
      })
    ])
  } finally {
    if (timer !== undefined) clearTimeout(timer)
  }
}

function readableFromWeb(body: ReadableStream<Uint8Array>, scope: RequestScope, parent: AbortSignal): Readable {
  let finished = false

  const stream = Readable.from(
    (async function* () {
      const reader = body.getReader()
      try {
        while (true) {
          const { done, value } = await readWithIdleTimeout(reader, scope)
          if (done) {
            finished = true
            return
          }
          yield Buffer.from(value)
        }
      } catch (error) {
        throw transportError(error, scope, parent)
      } finally {
        if (!finished) {
          try {
            await reader.cancel()
          } catch {
            // The source can already be aborted or closed.
          }
        }
        reader.releaseLock()
        scope.dispose()
      }
    })()
  )

  stream.once('close', () => {
    if (!finished) void body.cancel().catch(() => {})
    scope.dispose()
  })
  return stream
}

function retryDelay(attempt: number): number {
  return Math.min(RECONNECT_BASE_DELAY_MS * 2 ** Math.min(attempt, 6), RECONNECT_MAX_DELAY_MS)
}

function waitForReconnect(delayMs: number, signal: AbortSignal): Promise<void> {
  if (signal.aborted) return Promise.resolve()
  return new Promise((resolve) => {
    const finish = () => {
      clearTimeout(timer)
      signal.removeEventListener('abort', finish)
      resolve()
    }
    const timer = setTimeout(finish, delayMs)
    signal.addEventListener('abort', finish, { once: true })
  })
}

function isRetryable(error: SyncTransportError): boolean {
  return (
    error.code === 'network' ||
    error.code === 'timeout' ||
    (error.code === 'http' &&
      (error.status === 408 || error.status === 429 || (error.status !== undefined && error.status >= 500)))
  )
}

function nextSseLine(buffer: string, endOfStream = false): { line: string; length: number } | undefined {
  for (let index = 0; index < buffer.length; index++) {
    if (buffer[index] === '\n') return { line: buffer.slice(0, index), length: index + 1 }
    if (buffer[index] === '\r') {
      if (index === buffer.length - 1 && !endOfStream) return undefined
      return { line: buffer.slice(0, index), length: index + (buffer[index + 1] === '\n' ? 2 : 1) }
    }
  }
  return undefined
}

async function readRevisionEvents(
  body: ReadableStream<Uint8Array>,
  onRevision: (event: { revision: number; sourceDeviceId: string }) => void,
  onLastEventId: (id: string) => void,
  scope: RequestScope,
  parent: AbortSignal
): Promise<void> {
  const reader = body.getReader()
  const decoder = new TextDecoder('utf-8', { fatal: true })
  let buffer = ''
  let eventName = ''
  let eventData: string[] = []
  let eventBytes = 0
  let eventHasId = false

  const dispatch = () => {
    if (eventName === 'revision' && eventData.length) {
      let payload: unknown
      try {
        payload = JSON.parse(eventData.join('\n')) as unknown
      } catch {
        return invalidResponse()
      }
      if (
        !isRecord(payload) ||
        !hasKeys(payload, ['revision', 'sourceDeviceId'], ['revision', 'sourceDeviceId']) ||
        !isRevision(payload.revision) ||
        !isNonemptyString(payload.sourceDeviceId, MAX_EVENT_ID_LENGTH)
      )
        return invalidResponse()

      if (!eventHasId) onLastEventId(String(payload.revision))
      try {
        onRevision({ revision: payload.revision, sourceDeviceId: payload.sourceDeviceId })
      } catch {
        throw new SyncTransportError('consumer')
      }
    }
    eventName = ''
    eventData = []
    eventBytes = 0
    eventHasId = false
  }

  const processLine = (line: string) => {
    const lineBytes = Buffer.byteLength(line)
    if (lineBytes > MAX_SSE_LINE_BYTES) return invalidResponse()
    if (line === '') return dispatch()
    if (line.startsWith(':')) return

    eventBytes += lineBytes + 1
    if (eventBytes > MAX_SSE_EVENT_BYTES) return invalidResponse()

    const separator = line.indexOf(':')
    const field = separator < 0 ? line : line.slice(0, separator)
    const rawValue = separator < 0 ? '' : line.slice(separator + 1)
    const value = rawValue.startsWith(' ') ? rawValue.slice(1) : rawValue

    if (field === 'event') eventName = value
    else if (field === 'data') eventData.push(value)
    else if (field === 'id') {
      if (value.length > MAX_EVENT_ID_LENGTH || /[\0\r\n]/.test(value)) return invalidResponse()
      eventHasId = true
      onLastEventId(value)
    }
  }

  try {
    while (true) {
      const { done, value } = await readWithIdleTimeout(reader, scope)
      if (done) {
        try {
          buffer += decoder.decode()
        } catch {
          return invalidResponse()
        }
        let line: ReturnType<typeof nextSseLine>
        while ((line = nextSseLine(buffer, true))) {
          processLine(line.line)
          buffer = buffer.slice(line.length)
        }
        if (buffer) processLine(buffer)
        dispatch()
        return
      }

      try {
        buffer += decoder.decode(value, { stream: true })
      } catch {
        return invalidResponse()
      }
      let line: ReturnType<typeof nextSseLine>
      while ((line = nextSseLine(buffer))) {
        processLine(line.line)
        buffer = buffer.slice(line.length)
      }
      if (Buffer.byteLength(buffer) > MAX_SSE_LINE_BYTES) return invalidResponse()
    }
  } catch (error) {
    throw transportError(error, scope, parent)
  } finally {
    try {
      await reader.cancel()
    } catch {
      // The stream may already be closed.
    }
    reader.releaseLock()
  }
}

export class SyncTransport {
  private readonly baseUrl: URL

  constructor(
    serverUrl: string,
    private readonly token: string,
    private readonly signal: AbortSignal
  ) {
    this.baseUrl = normalizeServerBase(serverUrl)
    try {
      assertSyncToken(token)
    } catch {
      invalidInput()
    }
  }

  private endpoint(path: string): URL {
    return new URL(`api/v1/${path}`, this.baseUrl)
  }

  private headers(extra?: HeadersInit): Headers {
    const headers = new Headers(extra)
    headers.set('authorization', `Bearer ${this.token}`)
    return headers
  }

  private async requestJson<T>(url: URL, init: RequestInit, parse: (value: unknown) => T): Promise<T> {
    if (this.signal.aborted) throw new SyncTransportError('aborted')
    const scope = requestScope(this.signal, REQUEST_TIMEOUT_MS)
    try {
      const response = await fetch(url, {
        ...init,
        headers: this.headers(init.headers),
        redirect: 'manual',
        signal: scope.signal
      })
      await requireSuccessfulResponse(response)
      const value = await readJson(response)
      return parse(value)
    } catch (error) {
      throw transportError(error, scope, this.signal)
    } finally {
      scope.dispose()
    }
  }

  async status(): Promise<{ version: string; revision: number }> {
    return this.requestJson(
      this.endpoint('status'),
      { method: 'GET', headers: { accept: 'application/json' } },
      parseStatus
    )
  }

  async push(deviceId: string, changes: SyncChange[]): Promise<{ changes: RemoteChange[]; revision: number }> {
    if (!isNonemptyString(deviceId) || !Array.isArray(changes) || changes.length > MAX_CHANGES) invalidInput()
    if (!changes.every((change) => isSyncChange(change, false))) invalidInput()

    const body = serializeJson({ deviceId, changes })
    return this.requestJson(
      this.endpoint('push'),
      {
        method: 'POST',
        headers: { accept: 'application/json', 'content-type': 'application/json' },
        body
      },
      parsePush
    )
  }

  async pull(
    since: number,
    categories: SyncCategory[],
    limit: number
  ): Promise<{ changes: RemoteChange[]; cursor: number; hasMore: boolean }> {
    if (
      !isRevision(since) ||
      !Array.isArray(categories) ||
      !categories.every(isSyncCategory) ||
      !Number.isSafeInteger(limit) ||
      limit < 1 ||
      limit > MAX_CHANGES
    )
      invalidInput()

    const url = this.endpoint('pull')
    url.searchParams.set('cursor', String(since))
    url.searchParams.set('categories', categories.join(','))
    url.searchParams.set('limit', String(limit))
    return this.requestJson(url, { method: 'GET', headers: { accept: 'application/json' } }, (value) =>
      parsePull(value, limit)
    )
  }

  async hasBlob(hash: string): Promise<boolean> {
    if (!isBlobHash(hash)) invalidInput()
    if (this.signal.aborted) throw new SyncTransportError('aborted')
    const scope = requestScope(this.signal, REQUEST_TIMEOUT_MS)
    try {
      const response = await fetch(this.endpoint(`blobs/${encodeURIComponent(hash)}`), {
        method: 'HEAD',
        headers: this.headers(),
        redirect: 'manual',
        signal: scope.signal
      })
      if (response.status === 404) {
        await discardBody(response)
        return false
      }
      await requireSuccessfulResponse(response)
      await discardBody(response)
      return true
    } catch (error) {
      throw transportError(error, scope, this.signal)
    } finally {
      scope.dispose()
    }
  }

  async putBlob(hash: string, source: Readable, size: number): Promise<void> {
    if (!isBlobHash(hash) || !(source instanceof Readable) || !Number.isSafeInteger(size) || size < 0) invalidInput()
    if (this.signal.aborted) throw new SyncTransportError('aborted')

    const scope = requestScope(this.signal, BLOB_UPLOAD_TIMEOUT_MS)
    try {
      const init: RequestInit & { duplex: 'half' } = {
        method: 'PUT',
        headers: this.headers({
          accept: 'application/json',
          'content-length': String(size),
          'content-type': 'application/octet-stream'
        }),
        body: Readable.toWeb(source) as ReadableStream<Uint8Array>,
        duplex: 'half',
        redirect: 'manual',
        signal: scope.signal
      }
      const response = await fetch(this.endpoint(`blobs/${encodeURIComponent(hash)}`), init)
      await requireSuccessfulResponse(response)
      await discardBody(response)
    } catch (error) {
      throw transportError(error, scope, this.signal)
    } finally {
      scope.dispose()
    }
  }

  async getBlob(hash: string): Promise<Readable> {
    if (!isBlobHash(hash)) invalidInput()
    if (this.signal.aborted) throw new SyncTransportError('aborted')

    const scope = requestScope(this.signal, REQUEST_TIMEOUT_MS)
    let handedOff = false
    try {
      const response = await fetch(this.endpoint(`blobs/${encodeURIComponent(hash)}`), {
        method: 'GET',
        headers: this.headers({ accept: 'application/octet-stream' }),
        redirect: 'manual',
        signal: scope.signal
      })
      if (response.status !== 200) {
        await requireSuccessfulResponse(response)
        await discardBody(response)
        return invalidResponse()
      }
      if (!response.body) return invalidResponse()
      scope.clearTimeout()
      handedOff = true
      return readableFromWeb(response.body, scope, this.signal)
    } catch (error) {
      throw transportError(error, scope, this.signal)
    } finally {
      if (!handedOff) scope.dispose()
    }
  }

  async events(onRevision: (event: { revision: number; sourceDeviceId: string }) => void): Promise<void> {
    let lastEventId: string | undefined
    let attempt = 0

    while (!this.signal.aborted) {
      const scope = requestScope(this.signal, REQUEST_TIMEOUT_MS)
      try {
        const headers = this.headers({ accept: 'text/event-stream' })
        if (lastEventId !== undefined) headers.set('last-event-id', lastEventId)
        const response = await fetch(this.endpoint('events'), {
          method: 'GET',
          headers,
          redirect: 'manual',
          signal: scope.signal
        })
        scope.clearTimeout()
        await requireSuccessfulResponse(response)
        if (response.headers.get('content-type')?.split(';', 1)[0].trim().toLowerCase() !== 'text/event-stream') {
          await discardBody(response)
          invalidResponse()
        }
        if (!response.body) invalidResponse()
        await readRevisionEvents(
          response.body,
          onRevision,
          (id) => {
            lastEventId = id
          },
          scope,
          this.signal
        )
      } catch (error) {
        if (this.signal.aborted) break
        const safeError = transportError(error, scope, this.signal)
        if (!isRetryable(safeError)) throw safeError
      } finally {
        scope.dispose()
      }

      if (this.signal.aborted) break
      await waitForReconnect(retryDelay(attempt), this.signal)
      attempt = Math.min(attempt + 1, 6)
    }
  }
}
