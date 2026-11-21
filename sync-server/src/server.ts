import { createHash, randomBytes, randomUUID, timingSafeEqual } from 'node:crypto'
import {
  chmodSync,
  closeSync,
  createReadStream,
  fsyncSync,
  lstatSync,
  mkdirSync,
  openSync,
  readFileSync,
  unlinkSync,
  writeSync
} from 'node:fs'
import { open, rm, unlink } from 'node:fs/promises'
import { createServer, type IncomingMessage, type Server, type ServerResponse } from 'node:http'
import { join, resolve } from 'node:path'
import { fileURLToPath } from 'node:url'

import { SyncStore } from './store.js'
import { ApiError, CATEGORIES, type Category, type IncomingChange, type JsonObject } from './types.js'

const API_ROOT = '/api/v1'
const HEALTH_PATH = API_ROOT + '/health'
const MAX_BATCH_SIZE = 100
const DEFAULT_PULL_LIMIT = 100
const MAX_PULL_LIMIT = 500
const MAX_SSE_BUFFER_BYTES = 65_536
const DEFAULT_JSON_BYTES = 8 * 1024 * 1024
const DEFAULT_BLOB_BYTES = 100 * 1024 * 1024
const DEFAULT_STORAGE_BYTES = 10 * 1024 * 1024 * 1024
const REQUEST_TIMEOUT_MS = 300_000
const TOKEN_PATTERN = /^[A-Za-z0-9._~+/-]+$/
const CATEGORY_SET = new Set<string>(CATEGORIES)

const packageData = JSON.parse(readFileSync(new URL('../package.json', import.meta.url), 'utf8')) as { version: string }

export interface ServerOptions {
  host?: string
  port?: number
  dataDir: string
  token?: string
  maxJsonBytes?: number
  maxBlobBytes?: number
  maxStorageBytes?: number
  heartbeatMs?: number
}

export interface RunningServer {
  server: Server
  url: string
  initialToken?: string
  close(): Promise<void>
}

interface NormalizedOptions {
  host: string
  port: number
  dataDir: string
  token?: string
  maxJsonBytes: number
  maxBlobBytes: number
  maxStorageBytes: number
  heartbeatMs: number
}

interface EventClient {
  response: ServerResponse
  heartbeat: NodeJS.Timeout
}

interface PushBody {
  deviceId: string
  changes: IncomingChange[]
}

type RequestHandler = ((request: IncomingMessage, response: ServerResponse) => void) & {
  closeClients(): void
}

function configInteger(
  value: string | undefined,
  name: string,
  fallback: number,
  minimum: number,
  maximum = Number.MAX_SAFE_INTEGER
): number {
  if (value === undefined || value === '') return fallback
  if (!/^\d+$/.test(value)) throw new Error(name + ' must be a positive integer')
  const parsed = Number(value)
  if (!Number.isSafeInteger(parsed) || parsed < minimum || parsed > maximum) {
    throw new Error(name + ' must be a positive integer')
  }
  return parsed
}

export function readServerOptions(env: NodeJS.ProcessEnv = process.env): ServerOptions {
  const configuredToken = env.SYNC_TOKEN
  return {
    host: env.SYNC_BIND || '127.0.0.1',
    port: configInteger(env.SYNC_PORT, 'SYNC_PORT', 8787, 0, 65_535),
    dataDir: env.SYNC_DATA_DIR || './data',
    ...(configuredToken ? { token: configuredToken } : {}),
    maxJsonBytes: configInteger(env.SYNC_MAX_JSON_BYTES, 'SYNC_MAX_JSON_BYTES', DEFAULT_JSON_BYTES, 1),
    maxBlobBytes:
      configInteger(env.SYNC_MAX_BLOB_MB, 'SYNC_MAX_BLOB_MB', DEFAULT_BLOB_BYTES / (1024 * 1024), 1, 1048576) *
      1024 *
      1024,
    maxStorageBytes: configInteger(env.SYNC_MAX_STORAGE_BYTES, 'SYNC_MAX_STORAGE_BYTES', DEFAULT_STORAGE_BYTES, 1),
    heartbeatMs: configInteger(env.SYNC_HEARTBEAT_MS, 'SYNC_HEARTBEAT_MS', 20_000, 1_000)
  }
}

function normalizeOptions(options: ServerOptions): NormalizedOptions {
  const result: NormalizedOptions = {
    host: options.host ?? '127.0.0.1',
    port: options.port ?? 8787,
    dataDir: resolve(options.dataDir),
    maxJsonBytes: options.maxJsonBytes ?? DEFAULT_JSON_BYTES,
    maxBlobBytes: options.maxBlobBytes ?? DEFAULT_BLOB_BYTES,
    maxStorageBytes: options.maxStorageBytes ?? DEFAULT_STORAGE_BYTES,
    heartbeatMs: options.heartbeatMs ?? 20_000
  }

  if (options.token !== undefined) result.token = options.token
  if (!Number.isSafeInteger(result.port) || result.port < 0 || result.port > 65_535)
    throw new Error('port is out of range')
  if (!Number.isSafeInteger(result.maxJsonBytes) || result.maxJsonBytes < 1)
    throw new Error('maxJsonBytes is out of range')
  if (!Number.isSafeInteger(result.maxBlobBytes) || result.maxBlobBytes < 1)
    throw new Error('maxBlobBytes is out of range')
  if (!Number.isSafeInteger(result.maxStorageBytes) || result.maxStorageBytes < 1)
    throw new Error('maxStorageBytes is out of range')
  if (!Number.isSafeInteger(result.heartbeatMs) || result.heartbeatMs < 1_000)
    throw new Error('heartbeatMs is out of range')

  return result
}

function validateToken(token: string): void {
  if (token.length < 32 || token.length > 512 || !TOKEN_PATTERN.test(token)) {
    throw new Error('SYNC_TOKEN must be 32-512 URL-safe characters')
  }
}

function loadOrCreateToken(
  dataDir: string,
  configuredToken: string | undefined
): { token: string; initialToken?: string } {
  if (configuredToken !== undefined) {
    validateToken(configuredToken)
    return { token: configuredToken }
  }

  mkdirSync(dataDir, { recursive: true, mode: 0o700 })
  const tokenPath = join(dataDir, 'token.txt')
  const generated = randomBytes(32).toString('hex')
  let descriptor: number

  try {
    descriptor = openSync(tokenPath, 'wx', 0o600)
  } catch (error) {
    if (!(typeof error === 'object' && error !== null && 'code' in error && error.code === 'EEXIST')) throw error
    const stat = lstatSync(tokenPath)
    if (!stat.isFile()) throw new Error('The token path must be a regular file')
    chmodSync(tokenPath, 0o600)
    const token = readFileSync(tokenPath, 'utf8')
    validateToken(token)
    return { token }
  }

  try {
    writeSync(descriptor, generated, undefined, 'utf8')
    fsyncSync(descriptor)
  } catch (error) {
    closeSync(descriptor)
    unlinkSync(tokenPath)
    throw error
  }
  closeSync(descriptor)
  return { token: generated, initialToken: generated }
}

function jsonObject(value: unknown): value is JsonObject {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
}

function exactKeys(value: JsonObject, keys: string[]): boolean {
  const actual = Object.keys(value)
  return actual.length === keys.length && actual.every((key) => keys.includes(key))
}

function identifier(value: unknown, field: string): string {
  if (typeof value !== 'string' || value.length < 1 || value.length > 128 || !/^[\x21-\x7e]+$/.test(value)) {
    throw new ApiError(400, 'invalid_change', field + ' must be 1-128 visible ASCII characters')
  }
  return value
}

function timestamp(value: unknown, field: string): number {
  if (typeof value !== 'number' || !Number.isSafeInteger(value) || value < 0) {
    throw new ApiError(400, 'invalid_change', field + ' must be a non-negative safe integer')
  }
  return value
}

function validateChange(value: unknown, requestDeviceId: string): IncomingChange {
  if (!jsonObject(value)) throw new ApiError(400, 'invalid_change', 'Each change must be a JSON object')

  const category = value.category
  if (typeof category !== 'string' || !CATEGORY_SET.has(category)) {
    throw new ApiError(400, 'invalid_change', 'category is not supported')
  }

  const common = {
    category: category as Category,
    entityType: identifier(value.entityType, 'entityType'),
    entityId: identifier(value.entityId, 'entityId'),
    deviceId: identifier(value.deviceId, 'deviceId'),
    updatedAt: timestamp(value.updatedAt, 'updatedAt')
  }

  if (common.deviceId !== requestDeviceId) {
    throw new ApiError(400, 'invalid_change', 'Each change deviceId must match the push deviceId')
  }

  if (Object.hasOwn(value, 'payload')) {
    if (
      !exactKeys(value, ['category', 'entityType', 'entityId', 'deviceId', 'updatedAt', 'payload']) ||
      !jsonObject(value.payload)
    ) {
      throw new ApiError(400, 'invalid_change', 'An upsert requires only a JSON object payload')
    }
    try {
      JSON.stringify(value.payload)
    } catch {
      throw new ApiError(400, 'invalid_change', 'payload cannot be serialized')
    }
    return { ...common, payload: value.payload }
  }

  if (
    !exactKeys(value, ['category', 'entityType', 'entityId', 'deviceId', 'updatedAt', 'deleted', 'deletedAt']) ||
    value.deleted !== true
  ) {
    throw new ApiError(400, 'invalid_change', 'A deletion requires deleted:true and deletedAt')
  }

  return {
    ...common,
    deleted: true,
    deletedAt: timestamp(value.deletedAt, 'deletedAt')
  }
}

function validatePushBody(value: unknown): PushBody {
  if (!jsonObject(value) || !exactKeys(value, ['deviceId', 'changes']) || !Array.isArray(value.changes)) {
    throw new ApiError(400, 'invalid_push', 'A push requires deviceId and changes')
  }
  const deviceId = identifier(value.deviceId, 'deviceId')
  if (value.changes.length > MAX_BATCH_SIZE) {
    throw new ApiError(400, 'batch_too_large', 'A push may contain at most ' + MAX_BATCH_SIZE + ' changes')
  }
  return {
    deviceId,
    changes: value.changes.map((change) => validateChange(change, deviceId))
  }
}

function sendJson(response: ServerResponse, status: number, body: unknown): void {
  const data = Buffer.from(JSON.stringify(body))
  response.writeHead(status, {
    'Content-Type': 'application/json; charset=utf-8',
    'Content-Length': data.length,
    'Cache-Control': 'no-store',
    'X-Content-Type-Options': 'nosniff'
  })
  response.end(data)
}

function apiError(error: unknown): ApiError {
  if (error instanceof ApiError) return error
  if (typeof error === 'object' && error !== null && 'code' in error && error.code === 'SQLITE_FULL') {
    return new ApiError(507, 'storage_limit', 'The server storage limit would be exceeded')
  }
  return new ApiError(500, 'internal_error', 'Internal server error')
}

function contentLength(request: IncomingMessage): number | undefined {
  const raw = request.headers['content-length']
  if (raw === undefined) return undefined
  if (Array.isArray(raw) || !/^\d+$/.test(raw)) {
    throw new ApiError(400, 'invalid_content_length', 'Content-Length is invalid')
  }
  const parsed = Number(raw)
  if (!Number.isSafeInteger(parsed)) throw new ApiError(400, 'invalid_content_length', 'Content-Length is invalid')
  return parsed
}

function drainAndThrow(request: IncomingMessage, error: ApiError): never {
  request.resume()
  throw error
}

async function readJson(request: IncomingMessage, maximumBytes: number): Promise<{ value: unknown; size: number }> {
  const declaredLength = contentLength(request)
  if (declaredLength !== undefined && declaredLength > maximumBytes) {
    drainAndThrow(request, new ApiError(413, 'request_too_large', 'The JSON request exceeds the configured size limit'))
  }

  const body = await new Promise<Buffer>((resolveBody, rejectBody) => {
    const chunks: Buffer[] = []
    let size = 0
    let settled = false

    const cleanup = (): void => {
      request.removeListener('data', onData)
      request.removeListener('end', onEnd)
      request.removeListener('aborted', onAborted)
      request.removeListener('error', onError)
    }
    const fail = (error: Error): void => {
      if (settled) return
      settled = true
      chunks.length = 0
      cleanup()
      request.resume()
      rejectBody(error)
    }
    const onData = (value: Buffer | string): void => {
      const chunk = Buffer.isBuffer(value) ? value : Buffer.from(value)
      if (size + chunk.length > maximumBytes) {
        fail(new ApiError(413, 'request_too_large', 'The JSON request exceeds the configured size limit'))
        return
      }
      size += chunk.length
      chunks.push(chunk)
    }
    const onEnd = (): void => {
      if (settled) return
      settled = true
      cleanup()
      resolveBody(Buffer.concat(chunks, size))
    }
    const onAborted = (): void => fail(new ApiError(400, 'request_aborted', 'The request body was interrupted'))
    const onError = (error: Error): void => fail(error)

    request.on('data', onData)
    request.on('end', onEnd)
    request.on('aborted', onAborted)
    request.on('error', onError)
  })

  let decoded: string
  try {
    decoded = new TextDecoder('utf-8', { fatal: true }).decode(body)
  } catch {
    throw new ApiError(400, 'invalid_json', 'The request body must be valid UTF-8 JSON')
  }

  try {
    return { value: JSON.parse(decoded) as unknown, size: body.length }
  } catch {
    throw new ApiError(400, 'invalid_json', 'The request body must be valid JSON')
  }
}

function requireJson(request: IncomingMessage): void {
  const mediaType = request.headers['content-type']?.split(';', 1)[0]?.trim().toLowerCase()
  if (mediaType !== 'application/json') {
    drainAndThrow(request, new ApiError(415, 'unsupported_media_type', 'Content-Type must be application/json'))
  }
}

function parseCursor(raw: string | null): number {
  if (raw === null) return 0
  if (!/^\d+$/.test(raw)) throw new ApiError(400, 'invalid_cursor', 'cursor must be a non-negative integer')
  const value = Number(raw)
  if (!Number.isSafeInteger(value))
    throw new ApiError(400, 'invalid_cursor', 'cursor must be a non-negative safe integer')
  return value
}

function parseLimit(raw: string | null): number {
  if (raw === null) return DEFAULT_PULL_LIMIT
  if (!/^\d+$/.test(raw)) throw new ApiError(400, 'invalid_limit', 'limit must be an integer')
  const value = Number(raw)
  if (!Number.isSafeInteger(value) || value < 1 || value > MAX_PULL_LIMIT) {
    throw new ApiError(400, 'invalid_limit', 'limit must be between 1 and ' + MAX_PULL_LIMIT)
  }
  return value
}

function parseCategories(raw: string | null): Set<Category> | undefined {
  if (raw === null) return undefined
  const values = raw.split(',')
  if (values.some((value) => !CATEGORY_SET.has(value))) {
    throw new ApiError(400, 'invalid_categories', 'categories contains an unsupported category')
  }
  return new Set(values as Category[])
}

function bearerAuthorized(request: IncomingMessage, expectedHash: Buffer): boolean {
  const header = request.headers.authorization
  const token = typeof header === 'string' && header.slice(0, 7).toLowerCase() === 'bearer ' ? header.slice(7) : ''
  const suppliedHash = createHash('sha256').update(token).digest()
  return timingSafeEqual(suppliedHash, expectedHash) && token.length > 0
}

function eventData(response: ServerResponse, event: { revision: number; sourceDeviceId: string }): void {
  response.write('event: revision\ndata: ' + JSON.stringify(event) + '\n\n')
}

async function streamBlobUpload(
  request: IncomingMessage,
  response: ServerResponse,
  sha256: string,
  store: SyncStore,
  maxBlobBytes: number
): Promise<void> {
  const mediaType = request.headers['content-type']?.split(';', 1)[0]?.trim().toLowerCase()
  if (mediaType !== 'application/octet-stream') {
    drainAndThrow(request, new ApiError(415, 'unsupported_media_type', 'Content-Type must be application/octet-stream'))
  }

  const declaredLength = contentLength(request)
  if (declaredLength !== undefined && declaredLength > maxBlobBytes) {
    drainAndThrow(request, new ApiError(413, 'blob_too_large', 'The blob exceeds the configured size limit'))
  }

  const existing = store.findBlob(sha256)
  const temporaryPath = existing ? undefined : join(store.blobsDir, '.upload-' + randomUUID() + '.tmp')
  let file = temporaryPath ? await open(temporaryPath, 'wx', 0o600) : undefined
  let reserved = 0
  let size = 0
  let tooLarge = false
  let overQuota = false
  const hash = createHash('sha256')

  const removeTemporaryFile = async (): Promise<void> => {
    if (file) {
      await file.close()
      file = undefined
    }
    if (temporaryPath) {
      try {
        await unlink(temporaryPath)
      } catch (error) {
        if (!(typeof error === 'object' && error !== null && 'code' in error && error.code === 'ENOENT')) throw error
      }
    }
  }

  try {
    for await (const value of request) {
      const chunk = Buffer.isBuffer(value) ? value : Buffer.from(value)
      if (tooLarge || overQuota) continue
      if (size + chunk.length > maxBlobBytes) {
        tooLarge = true
        await removeTemporaryFile()
        continue
      }

      if (file) {
        try {
          store.reserveStorage(chunk.length)
          reserved += chunk.length
        } catch (error) {
          if (!(error instanceof ApiError) || error.code !== 'storage_limit') throw error
          overQuota = true
          await removeTemporaryFile()
          continue
        }
      }

      size += chunk.length
      hash.update(chunk)
      if (file) await file.writeFile(chunk)
    }

    if (tooLarge) throw new ApiError(413, 'blob_too_large', 'The blob exceeds the configured size limit')
    if (overQuota) throw new ApiError(507, 'storage_limit', 'The server storage limit would be exceeded')
    if (declaredLength !== undefined && declaredLength !== size) {
      throw new ApiError(400, 'content_length_mismatch', 'The request body does not match Content-Length')
    }

    if (hash.digest('hex') !== sha256) {
      throw new ApiError(422, 'blob_hash_mismatch', 'The uploaded content does not match its SHA-256 path')
    }

    if (existing) {
      if (existing.size !== size) throw new ApiError(500, 'blob_corrupt', 'The stored blob metadata is inconsistent')
      sendJson(response, 200, { sha256, size, duplicate: true })
      return
    }

    if (!file || !temporaryPath) throw new Error('Temporary blob file was not created')
    await file.sync()
    await file.close()
    file = undefined

    const inserted = store.commitBlob(temporaryPath, sha256, size)
    sendJson(response, inserted ? 201 : 200, {
      sha256,
      size,
      duplicate: !inserted
    })
  } finally {
    try {
      if (file) await file.close().catch(() => undefined)
      if (temporaryPath) await rm(temporaryPath, { force: true })
    } finally {
      if (reserved > 0) store.releaseStorage(reserved)
    }
  }
}

function requestHandler(options: NormalizedOptions, store: SyncStore, token: string): RequestHandler {
  const expectedHash = createHash('sha256').update(token).digest()
  const maxResponseBytes = Math.min(Number.MAX_SAFE_INTEGER, options.maxJsonBytes * 2)
  const clients = new Set<EventClient>()

  const publish = (revision: number, sourceDeviceId: string): void => {
    const event = { revision, sourceDeviceId }
    for (const client of clients) {
      if (client.response.writableLength > MAX_SSE_BUFFER_BYTES) {
        client.response.end()
        continue
      }
      eventData(client.response, event)
    }
  }

  const closeClients = (): void => {
    for (const client of clients) {
      clearInterval(client.heartbeat)
      client.response.end()
    }
    clients.clear()
  }

  const handle = async (request: IncomingMessage, response: ServerResponse): Promise<void> => {
    const url = new URL(request.url ?? '/', 'http://sync.local')
    const pathname = url.pathname

    if (pathname === HEALTH_PATH && request.method === 'GET') {
      sendJson(response, 200, { status: 'ok' })
      return
    }

    if (pathname !== API_ROOT && !pathname.startsWith(API_ROOT + '/')) {
      throw new ApiError(404, 'not_found', 'Route not found')
    }

    if (!bearerAuthorized(request, expectedHash)) {
      response.setHeader('WWW-Authenticate', 'Bearer')
      drainAndThrow(request, new ApiError(401, 'unauthorized', 'A valid bearer token is required'))
    }

    if (pathname === HEALTH_PATH) {
      if (request.method !== 'GET') {
        response.setHeader('Allow', 'GET')
        throw new ApiError(405, 'method_not_allowed', 'Method not allowed')
      }
      sendJson(response, 200, { status: 'ok' })
      return
    }

    if (pathname === API_ROOT + '/status') {
      if (request.method !== 'GET') {
        response.setHeader('Allow', 'GET')
        throw new ApiError(405, 'method_not_allowed', 'Method not allowed')
      }
      sendJson(response, 200, {
        version: packageData.version,
        revision: store.getRevision(),
        stats: store.stats()
      })
      return
    }

    if (pathname === API_ROOT + '/push') {
      if (request.method !== 'POST') {
        response.setHeader('Allow', 'POST')
        throw new ApiError(405, 'method_not_allowed', 'Method not allowed')
      }
      requireJson(request)
      const body = await readJson(request, options.maxJsonBytes)
      const push = validatePushBody(body.value)
      const result = store.push(push.changes, body.size)
      sendJson(response, 200, {
        revision: result.revision,
        changes: result.changes
      })
      for (const revision of result.acceptedRevisions) publish(revision, push.deviceId)
      return
    }

    if (pathname === API_ROOT + '/pull') {
      if (request.method !== 'GET') {
        response.setHeader('Allow', 'GET')
        throw new ApiError(405, 'method_not_allowed', 'Method not allowed')
      }
      const cursor = url.searchParams.get('since')
      const categories = parseCategories(url.searchParams.get('categories'))
      let limit = parseLimit(url.searchParams.get('limit'))
      let result = store.pull(parseCursor(cursor), categories, limit)
      while (Buffer.byteLength(JSON.stringify(result)) > maxResponseBytes && limit > 1) {
        limit = Math.max(1, Math.floor(limit / 2))
        result = store.pull(parseCursor(cursor), categories, limit)
      }
      if (Buffer.byteLength(JSON.stringify(result)) > maxResponseBytes) {
        throw new ApiError(413, 'response_too_large', 'A change exceeds the configured response size limit')
      }
      sendJson(response, 200, result)
      return
    }

    if (pathname === API_ROOT + '/events') {
      if (request.method !== 'GET') {
        response.setHeader('Allow', 'GET')
        throw new ApiError(405, 'method_not_allowed', 'Method not allowed')
      }

      response.writeHead(200, {
        'Content-Type': 'text/event-stream; charset=utf-8',
        'Cache-Control': 'no-cache, no-transform',
        Connection: 'keep-alive',
        'X-Accel-Buffering': 'no',
        'X-Content-Type-Options': 'nosniff'
      })
      response.write(': connected\n\n')
      const client: EventClient = {
        response,
        heartbeat: setInterval(() => {
          if (response.writableLength > MAX_SSE_BUFFER_BYTES) {
            response.end()
            return
          }
          response.write(': heartbeat\n\n')
        }, options.heartbeatMs)
      }
      client.heartbeat.unref()
      clients.add(client)
      response.on('close', () => {
        clearInterval(client.heartbeat)
        clients.delete(client)
      })
      return
    }

    const blobPrefix = API_ROOT + '/blobs/'
    if (pathname.startsWith(blobPrefix)) {
      const sha256 = pathname.slice(blobPrefix.length)
      if (!/^[a-f0-9]{64}$/.test(sha256))
        throw new ApiError(400, 'invalid_blob_hash', 'Blob paths must be lowercase SHA-256 hex')

      if (request.method === 'PUT') {
        await streamBlobUpload(request, response, sha256, store, options.maxBlobBytes)
        return
      }

      if (request.method === 'GET' || request.method === 'HEAD') {
        const blob = store.findBlob(sha256)
        if (!blob) throw new ApiError(404, 'blob_not_found', 'Blob not found')
        response.writeHead(200, {
          'Content-Type': 'application/octet-stream',
          'Content-Length': blob.size,
          ETag: '"' + sha256 + '"',
          'Cache-Control': 'private, no-store',
          'X-Content-Type-Options': 'nosniff'
        })
        if (request.method === 'HEAD') {
          response.end()
          return
        }
        const stream = createReadStream(blob.filePath, {
          highWaterMark: 64 * 1024
        })
        stream.on('error', () => {
          if (!response.headersSent) {
            sendJson(response, 404, {
              error: { code: 'blob_not_found', message: 'Blob not found' }
            })
          } else {
            response.destroy()
          }
        })
        stream.pipe(response)
        return
      }

      response.setHeader('Allow', 'HEAD, GET, PUT')
      throw new ApiError(405, 'method_not_allowed', 'Method not allowed')
    }

    throw new ApiError(404, 'not_found', 'Route not found')
  }

  const handler: RequestHandler = (request, response): void => {
    void handle(request, response).catch((error: unknown) => {
      if (response.headersSent) {
        response.destroy()
        return
      }
      request.resume()
      const normalized = apiError(error)
      if (normalized.status === 500) {
        process.stderr.write('sync-server internal request error\n')
      }
      sendJson(response, normalized.status, {
        error: { code: normalized.code, message: normalized.message }
      })
    })
  }

  handler.closeClients = closeClients
  return handler
}

export async function startSyncServer(rawOptions: ServerOptions): Promise<RunningServer> {
  const options = normalizeOptions(rawOptions)
  const tokenPath = join(options.dataDir, 'token.txt')
  const tokenState = loadOrCreateToken(options.dataDir, options.token)
  let store: SyncStore | undefined
  let server: Server | undefined

  try {
    store = new SyncStore({
      dataDir: options.dataDir,
      tokenPath,
      maxStorageBytes: options.maxStorageBytes,
      maxBlobBytes: options.maxBlobBytes
    })
    await store.initialize()

    const handler = requestHandler(options, store, tokenState.token)
    server = createServer(handler)
    server.requestTimeout = REQUEST_TIMEOUT_MS
    server.headersTimeout = 60_000
    server.keepAliveTimeout = 5_000

    await new Promise<void>((resolveListen, rejectListen) => {
      server!.once('error', rejectListen)
      server!.listen(options.port, options.host, () => {
        server!.removeListener('error', rejectListen)
        resolveListen()
      })
    })

    const address = server.address()
    if (!address || typeof address === 'string') throw new Error('The server did not bind to a TCP port')
    const host = options.host === '0.0.0.0' || options.host === '::' ? '127.0.0.1' : options.host
    let closed = false

    const result: RunningServer = {
      server,
      url: 'http://' + (host.includes(':') ? '[' + host + ']' : host) + ':' + address.port,
      close: async () => {
        if (closed) return
        closed = true
        handler.closeClients()
        try {
          await new Promise<void>((resolveClose, rejectClose) => {
            server!.close((error) => {
              if (error) rejectClose(error)
              else resolveClose()
            })
          })
        } finally {
          store!.close()
        }
      }
    }
    if (tokenState.initialToken !== undefined) result.initialToken = tokenState.initialToken
    return result
  } catch (error) {
    if (server?.listening) {
      await new Promise<void>((resolveClose) => server!.close(() => resolveClose()))
    }
    store?.close()
    if (tokenState.initialToken !== undefined) {
      try {
        unlinkSync(tokenPath)
      } catch {
        // Preserve the startup error if cleanup is not possible.
      }
    }
    throw error
  }
}

function isMainModule(): boolean {
  return process.argv[1] !== undefined && resolve(process.argv[1]) === fileURLToPath(import.meta.url)
}

if (isMainModule()) {
  const options = readServerOptions()
  void startSyncServer(options)
    .then((running) => {
      if (running.initialToken) {
        process.stdout.write('Initial bearer token (shown once): ' + running.initialToken + '\n')
      }
      process.stdout.write('Sync server listening on ' + options.host + ':' + options.port + '\n')

      let stopping = false
      const stop = (): void => {
        if (stopping) return
        stopping = true
        void running.close().then(
          () => process.exit(0),
          () => process.exit(1)
        )
      }
      process.once('SIGINT', stop)
      process.once('SIGTERM', stop)
    })
    .catch((error: unknown) => {
      process.stderr.write(
        'Could not start sync server: ' + (error instanceof Error ? error.message : 'unknown error') + '\n'
      )
      process.exitCode = 1
    })
}
