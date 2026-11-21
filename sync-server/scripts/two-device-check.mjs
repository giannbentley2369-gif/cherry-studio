import { randomUUID } from 'node:crypto'

const endpointValue = process.env.SYNC_TEST_URL
const token = process.env.SYNC_TEST_TOKEN

if (!endpointValue || !token) {
  throw new Error('Set SYNC_TEST_URL and SYNC_TEST_TOKEN for a disposable development server')
}

const endpoint = new URL(endpointValue)
const localHosts = new Set(['localhost', '127.0.0.1', '[::1]'])
if (
  endpoint.username ||
  endpoint.password ||
  endpoint.search ||
  endpoint.hash ||
  endpoint.pathname !== '/' ||
  (endpoint.protocol !== 'https:' && !(endpoint.protocol === 'http:' && localHosts.has(endpoint.hostname)))
) {
  throw new Error('Use an HTTPS endpoint or a localhost URL without credentials, query, or path')
}

async function api(path, init = {}) {
  const response = await fetch(new URL('/api/v1' + path, endpoint), {
    ...init,
    redirect: 'error',
    headers: {
      authorization: 'Bearer ' + token,
      ...init.headers
    }
  })
  const text = await response.text()
  let body
  try {
    body = JSON.parse(text)
  } catch {
    throw new Error('The sync server returned a non-JSON response')
  }
  if (!response.ok) throw new Error('The sync server returned HTTP ' + response.status)
  return body
}

const suffix = randomUUID()
const entityId = 'smoke-' + suffix
const deviceA = 'smoke-a-' + suffix
const deviceB = 'smoke-b-' + suffix
const timestamp = Date.now()
const before = await api('/status')
const pushChange = async (deviceId, change) =>
  api('/push', {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ deviceId, changes: [change] })
  })

const first = await pushChange(deviceA, {
  category: 'chats',
  entityType: 'smoke-check',
  entityId,
  deviceId: deviceA,
  updatedAt: timestamp,
  payload: { testOnly: true, value: 'device-a' }
})
const pulled = await api('/pull?since=' + before.revision + '&categories=chats&limit=500')
if (!pulled.changes.some((change) => change.entityId === entityId && change.payload?.value === 'device-a')) {
  throw new Error('Device B did not pull Device A data')
}

const second = await pushChange(deviceB, {
  category: 'chats',
  entityType: 'smoke-check',
  entityId,
  deviceId: deviceB,
  updatedAt: timestamp + 1,
  payload: { testOnly: true, value: 'device-b' }
})
if (second.changes[0]?.deviceId !== deviceB || second.revision <= first.revision) {
  throw new Error('Server did not return Device B as the authoritative winner')
}

const receivedByA = await api('/pull?since=' + pulled.cursor + '&categories=chats&limit=500')
if (
  !receivedByA.changes.some(
    (change) => change.entityId === entityId && change.deviceId === deviceB && change.payload?.value === 'device-b'
  )
) {
  throw new Error('Device A did not pull Device B data')
}

const deletedAt = timestamp + 2
const cleanup = await pushChange(deviceA, {
  category: 'chats',
  entityType: 'smoke-check',
  entityId,
  deviceId: deviceA,
  updatedAt: deletedAt,
  deleted: true,
  deletedAt
})
if (cleanup.changes[0]?.deleted !== true) throw new Error('The smoke record was not tombstoned')

process.stdout.write('Two-device sync check passed; its temporary entity is tombstoned.\n')
