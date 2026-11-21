import { describe, expect, it } from 'vitest'

import { selfHostedSyncRequestSchemas } from '../selfHostedSync'

describe('self-hosted sync IPC schema', () => {
  it('accepts independent selection of credential and content categories', () => {
    const input = {
      categories: ['chats', 'files', 'providers']
    }

    expect(selfHostedSyncRequestSchemas['self_hosted_sync.update_config'].input.parse(input)).toEqual(input)
  })

  it('rejects duplicate or unsupported categories', () => {
    const schema = selfHostedSyncRequestSchemas['self_hosted_sync.update_config'].input

    expect(() => schema.parse({ categories: ['chats', 'chats'] })).toThrow()
    expect(() => schema.parse({ categories: ['messages'] })).toThrow()
  })

  it('does not accept bearer tokens over cleartext networks or URLs with embedded credentials', () => {
    const schema = selfHostedSyncRequestSchemas['self_hosted_sync.test_connection'].input
    expect(schema.safeParse({ serverUrl: 'http://127.0.0.1:8787' }).success).toBe(true)
    for (const serverUrl of [
      'http://192.0.2.1',
      'https://user:pass@example.org',
      'file:///tmp/data',
      'https://example.org/?secret=x'
    ]) {
      expect(schema.safeParse({ serverUrl }).success).toBe(false)
    }
  })

  it('does not include a token in the configuration response shape', () => {
    const schema = selfHostedSyncRequestSchemas['self_hosted_sync.get_config'].output
    const parsed = schema.parse({
      enabled: false,
      serverUrl: '',
      deviceId: 'device-1',
      categories: [],
      intervalSeconds: 60,
      hasToken: false,
      token: 'must-not-be-returned'
    })

    expect(parsed).not.toHaveProperty('token')
  })
})
