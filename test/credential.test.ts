/*
 *  Copyright 2016-2025. Couchbase, Inc.
 *  All Rights Reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

import { assert } from 'chai'
import * as http from 'node:http'
import { AddressInfo } from 'node:net'
import {
  Credential,
  JwtCredential,
  createInstance,
  type ClusterCredential,
} from '../lib/analytics.js'
import { InvalidArgumentError } from '../lib/errors.js'

const SAMPLE_JWT = 'header.payload.signature'

function authHeaderOf(cred: ClusterCredential): string | undefined {
  const cluster = createInstance('http://localhost:8095', cred)
  try {
    const opts = cluster.httpClient.genericRequestOptions()
    return (opts.headers as Record<string, string> | undefined)?.Authorization
  } finally {
    cluster.close()
  }
}

describe('#Credential', function () {
  describe('Password', function () {
    it('constructor builds a Basic header', function () {
      const cred = new Credential('Administrator', 'password')
      assert.strictEqual(cred.username, 'Administrator')
      assert.strictEqual(cred.password, 'password')
      assert.strictEqual(
        authHeaderOf(cred),
        'Basic ' + Buffer.from('Administrator:password').toString('base64')
      )
    })

    it('supports validated username/password mutation after construction', function () {
      const cred = new Credential('Administrator', 'password')

      cred.username = 'alice'
      cred.password = 'changed'

      assert.strictEqual(
        authHeaderOf(cred),
        'Basic ' + Buffer.from('alice:changed').toString('base64')
      )
    })

    it('rejects invalid username/password mutation', function () {
      const cred = new Credential('alice', 'pw')

      assert.throws(() => {
        const mutableCred = cred as unknown as { password: string }
        mutableCred.password = undefined as unknown as string
      }, InvalidArgumentError)
      assert.strictEqual(
        authHeaderOf(cred),
        'Basic ' + Buffer.from('alice:pw').toString('base64')
      )
    })

    it('rejects undefined username/password (no silent broken credential)', function () {
      assert.throws(
        () =>
          new Credential(
            undefined as unknown as string,
            undefined as unknown as string
          ),
        InvalidArgumentError
      )
    })
  })

  describe('JWT', function () {
    it('constructor builds a Bearer header', function () {
      const cred = new JwtCredential(SAMPLE_JWT)
      assert.strictEqual(authHeaderOf(cred), `Bearer ${SAMPLE_JWT}`)
    })

    it('rejects an empty token', function () {
      assert.throws(() => new JwtCredential(''), InvalidArgumentError)
    })

    it('rejects a non-string token', function () {
      assert.throws(
        () => new JwtCredential(1234 as unknown as string),
        InvalidArgumentError
      )
    })

  })

  describe('Cluster.setCredential', function () {
    it('rejects a null/undefined initial credential', function () {
      assert.throws(
        () =>
          createInstance(
            'http://localhost:8095',
            null as unknown as ClusterCredential
          ),
        InvalidArgumentError
      )
      assert.throws(
        () =>
          createInstance(
            'http://localhost:8095',
            undefined as unknown as ClusterCredential
          ),
        InvalidArgumentError
      )
    })

    it('rejects a username/password credential object', function () {
      assert.throws(
        () =>
          createInstance('http://localhost:8095', {
            username: 'alice',
            password: 'pw',
          } as unknown as ClusterCredential),
        InvalidArgumentError
      )
    })

    it('rotates a JWT in place', function () {
      const cluster = createInstance(
        'http://localhost:8095',
        new JwtCredential(SAMPLE_JWT)
      )
      try {
        const next = 'rotated.jwt.token'
        cluster.setCredential(new JwtCredential(next))
        const req = cluster.httpClient.genericRequestOptions()
        assert.strictEqual(
          (req.headers as Record<string, string>).Authorization,
          `Bearer ${next}`
        )
      } finally {
        cluster.close()
      }
    })

    it('rotates a password in place', function () {
      const cluster = createInstance(
        'http://localhost:8095',
        new Credential('alice', 'old')
      )
      try {
        cluster.setCredential(new Credential('alice', 'new'))
        const req = cluster.httpClient.genericRequestOptions()
        assert.strictEqual(
          (req.headers as Record<string, string>).Authorization,
          'Basic ' + Buffer.from('alice:new').toString('base64')
        )
      } finally {
        cluster.close()
      }
    })

    it('rejects invalid credential-shaped objects without mutating state', function () {
      const cluster = createInstance(
        'http://localhost:8095',
        new Credential('alice', 'pw')
      )
      try {
        const before = cluster.httpClient.genericRequestOptions()
          .headers as Record<string, string>
        assert.throws(
          () =>
            cluster.setCredential({
              credentialType: 'password',
            } as unknown as ClusterCredential),
          InvalidArgumentError
        )
        const after = cluster.httpClient.genericRequestOptions()
          .headers as Record<string, string>
        assert.strictEqual(after.Authorization, before.Authorization)
      } finally {
        cluster.close()
      }
    })

    it('rejects switching credential type at runtime', function () {
      const cluster = createInstance(
        'http://localhost:8095',
        new Credential('alice', 'pw')
      )
      try {
        assert.throws(
          () => cluster.setCredential(new JwtCredential(SAMPLE_JWT)),
          InvalidArgumentError
        )
      } finally {
        cluster.close()
      }
    })

    it('rejects a null/undefined credential', function () {
      const cluster = createInstance(
        'http://localhost:8095',
        new Credential('alice', 'pw')
      )
      try {
        assert.throws(
          () => cluster.setCredential(null as unknown as ClusterCredential),
          InvalidArgumentError
        )
        assert.throws(
          () =>
            cluster.setCredential(undefined as unknown as ClusterCredential),
          InvalidArgumentError
        )
      } finally {
        cluster.close()
      }
    })

    it('sends the Authorization header on the actual query request', async function () {
      // Regression guard: the executor's `headers` literal must not clobber
      // the `Authorization` header pulled in from genericRequestOptions().
      this.timeout(10000)
      let capturedAuth: string | undefined
      const server = http.createServer((req) => {
        capturedAuth = req.headers.authorization
        req.socket.destroy()
      })
      await new Promise<void>((resolve) =>
        server.listen(0, '127.0.0.1', resolve)
      )
      const port = (server.address() as AddressInfo).port

      const cluster = createInstance(
        `http://127.0.0.1:${port}`,
        new JwtCredential(SAMPLE_JWT)
      )
      try {
        await cluster
          .executeQuery('SELECT 1', { maxRetries: 0, timeout: 2000 })
          .catch(() => undefined)
        assert.strictEqual(capturedAuth, `Bearer ${SAMPLE_JWT}`)
      } finally {
        cluster.close()
        await new Promise<void>((resolve, reject) =>
          server.close((err) => (err ? reject(err) : resolve()))
        )
      }
    })

    it('credential rotation between retries takes effect on the next retry', async function () {
      this.timeout(10000)
      const seenAuth: string[] = []
      let cluster: ReturnType<typeof createInstance> | undefined
      const server = http.createServer((req) => {
        seenAuth.push(req.headers.authorization || '')
        if (seenAuth.length === 1) {
          cluster?.setCredential(new JwtCredential('second.jwt.token'))
        }
        req.socket.destroy()
      })
      await new Promise<void>((resolve) =>
        server.listen(0, '127.0.0.1', resolve)
      )
      const port = (server.address() as AddressInfo).port

      cluster = createInstance(
        `http://127.0.0.1:${port}`,
        new JwtCredential('first.jwt.token')
      )
      try {
        const queryPromise = cluster.executeQuery('SELECT 1', {
          maxRetries: 3,
          timeout: 4000,
        })
        await queryPromise.catch(() => undefined)

        assert.isAtLeast(seenAuth.length, 2)
        assert.strictEqual(seenAuth[0], 'Bearer first.jwt.token')
        assert.strictEqual(
          seenAuth[seenAuth.length - 1],
          'Bearer second.jwt.token'
        )
      } finally {
        cluster.close()
        await new Promise<void>((resolve, reject) =>
          server.close((err) => (err ? reject(err) : resolve()))
        )
      }
    })

    it('does not mutate state when rotation is rejected', function () {
      const cluster = createInstance(
        'http://localhost:8095',
        new Credential('alice', 'pw')
      )
      try {
        const before = cluster.httpClient.genericRequestOptions()
          .headers as Record<string, string>
        try {
          cluster.setCredential(new JwtCredential(SAMPLE_JWT))
        } catch {
          // expected
        }
        const after = cluster.httpClient.genericRequestOptions()
          .headers as Record<string, string>
        assert.strictEqual(after.Authorization, before.Authorization)
      } finally {
        cluster.close()
      }
    })
  })
})
