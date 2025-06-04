/*
 *  Copyright 2016-2024. Couchbase, Inc.
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

'use strict'

const { setTimeout } = require('node:timers/promises')

const assert = require('chai').assert
const H = require('./harness')

const {
  QueryMetadata,
  QueryMetrics,
  QueryScanConsistency,
} = require('../lib/querytypes')
const {
  PassthroughDeserializer,
  JsonDeserializer,
} = require('../lib/deserializers')

function genericTests(instance) {
  describe('#queryTests', function () {
    before(async function () {
      H.skipIfIntegrationDisabled(this)
    })

    it('should successfully stream rows', async function () {
      let results = []
      const qs = `FROM RANGE(1, 100) AS i SELECT *`
      let res = await instance().executeQuery(qs)
      for await (const row of res.rows()) {
        results.push(row)
      }
      assert.equal(results.length, 100)
    })

    it('should successfully stream rows using events', async function () {
      const eventStreamQuery = (qRes) => {
        return new Promise((resolve, reject) => {
          const results = []
          const readable = qRes.rows()
          readable.on('data', (row) => {
            results.push(row)
          })
          readable.on('end', () => {
            const metadata = qRes.metadata()
            resolve({
              rows: results,
              meta: metadata,
            })
          })
          readable.on('error', (err) => {
            reject(err)
          })
        })
      }

      const qs = `FROM RANGE(1, 100) AS i SELECT *`
      let qRes = await instance().executeQuery(qs)
      let result
      try {
        result = await eventStreamQuery(qRes)
      } catch (err) {} // eslint-disable-line no-empty
      assert.equal(result.rows.length, 100)
      assert.instanceOf(result.meta, QueryMetadata)
    })

    it('should use the passthrough deserializer', async function () {
      let jsonRows = []
      let passthroughRows = []

      const qs = `SELECT 1=1`
      let jsonRes = await instance().executeQuery(qs, {
        deserializer: new JsonDeserializer(),
      })
      let passthroughRes = await instance().executeQuery(qs, {
        deserializer: new PassthroughDeserializer(),
      })
      for await (const row of jsonRes.rows()) {
        jsonRows.push(row)
      }
      for await (const row of passthroughRes.rows()) {
        passthroughRows.push(row)
      }

      assert.equal(jsonRows.length, 1)
      assert.equal(passthroughRows.length, 1)
      assert.isObject(jsonRows.at(0))
      assert.isString(passthroughRows.at(0))
    })

    it('should work with multiple options', async function () {
      const results = []
      const qs = `SELECT $five=5`

      let res = await instance().executeQuery(qs, {
        namedParameters: {
          five: 5,
        },
        readOnly: true,
        scanConsistency: QueryScanConsistency.NotBounded,
        priority: true,
      })

      for await (const row of res.rows()) {
        results.push(row)
      }

      assert.equal(results.length, 1)
      assert.isTrue(results.at(0)['$1'])
    })

    it('should should raise error on negative timeout', async function () {
      await H.throwsHelper(async () => {
        await instance().executeQuery("SELECT 'FOO' AS message", {
          timeout: -1,
        })
      }, Error)
    })

    it('should work with positional parameters', async function () {
      const results = []
      const qs = `SELECT $2=1`

      let res = await instance().executeQuery(qs, {
        positionalParameters: [undefined, 1],
      })

      for await (const row of res.rows()) {
        results.push(row)
      }

      assert.equal(results.length, 1)
      assert.isTrue(results.at(0)['$1'])
    })

    it('should successfully provide query metadata', async function () {
      let results = []
      const qs = `FROM RANGE(1, 100) AS i SELECT *`
      let res = await instance().executeQuery(qs)
      for await (const row of res.rows()) {
        results.push(row)
      }
      assert.equal(results.length, 100)

      const metadata = res.metadata()
      assert.instanceOf(metadata, QueryMetadata)
      assert.notStrictEqual(metadata.requestId, '')
      assert.isArray(metadata.warnings)
      assert.equal(metadata.warnings.length, 0)
      const metrics = metadata.metrics
      assert.instanceOf(metrics, QueryMetrics)
      assert.isAbove(metrics.resultSize, 0)
      assert.equal(metrics.resultCount, 100)
      assert.isAtLeast(metrics.processedObjects, 0)
      assert.isAbove(metrics.elapsedTime, 0)
      assert.isAbove(metrics.executionTime, 0)
    })

    it('should raise Error when query metadata is unavailable', async function () {
      let results = []
      const qs = `FROM RANGE(1, 100) AS i SELECT *`
      let res = await instance().executeQuery(qs)
      try {
        res.metadata()
      } catch (err) {
        assert.equal(
          err.message,
          'Metadata is only available once all rows have been iterated'
        )
      }

      // read one row
      for await (const row of res.rows().iterator({ destroyOnReturn: false })) {
        results.push(row)
        break
      }

      try {
        res.metadata()
      } catch (err) {
        assert.equal(
          err.message,
          'Metadata is only available once all rows have been iterated'
        )
      }

      for await (const row of res.rows()) {
        results.push(row)
      }
      assert.equal(results.length, 100)

      // now okay to get metrics
      const metadata = res.metadata()
      assert.instanceOf(metadata, QueryMetadata)
      assert.notStrictEqual(metadata.requestId, '')
      assert.isArray(metadata.warnings)
      assert.equal(metadata.warnings.length, 0)
      const metrics = metadata.metrics
      assert.instanceOf(metrics, QueryMetrics)
    })

    it('should fetch all databases', async function () {
      const results = []
      const qs = `SELECT RAW {\`DatabaseName\`, \`SystemDatabase\`}
          FROM \`System\`.\`Metadata\`.\`Database\``

      let res = await instance().executeQuery(qs)

      for await (const row of res.rows()) {
        results.push(row)
      }

      assert.isAtLeast(results.length, 2)

      const testDatabase = results.find((db) => db.DatabaseName === H.d.name)
      const systemDatabase = results.find((db) => db.DatabaseName === 'System')

      assert.isNotNull(systemDatabase)
      assert.isNotNull(testDatabase)
      assert.isTrue(systemDatabase['SystemDatabase'])
      assert.isFalse(testDatabase['SystemDatabase'])
    })

    it('should fetch all scopes', async function () {
      const results = []
      const qs = `SELECT RAW \`DataverseName\`
          FROM \`System\`.\`Metadata\`.\`Dataverse\`
          WHERE \`DatabaseName\` = ?`

      let res = await instance().executeQuery(qs, {
        positionalParameters: [H.d.name],
      })

      for await (const row of res.rows()) {
        results.push(row)
      }

      const testScope = results.find((scope) => scope === H.s.name)

      assert.isNotNull(testScope)
      assert.isAtLeast(results.length, 1)
    })

    it('should cancel prior to iterating', async function () {
      const qs = 'FROM range(0, 1000000) AS r SELECT *'
      const abortController = new AbortController()

      let qResPromise = instance().executeQuery(qs, {
        abortSignal: abortController.signal,
      })
      assert.instanceOf(qResPromise, Promise)
      await setTimeout(250).then(() => {
        abortController.abort()
      })
      let gotAbortError = false
      try {
        await qResPromise
      } catch (err) {
        if (err.name === 'AbortError') {
          gotAbortError = true
        }
      }

      assert.isTrue(gotAbortError)
    })

    it('should cancel while iterating using abort controller', async function () {
      const qs = `FROM RANGE(1, 100) AS i SELECT *`
      const abortController = new AbortController()
      const results = []
      const expectedCount = 5
      let count = 0
      let res = await instance().executeQuery(qs, {
        abortSignal: abortController.signal,
      })

      let stream
      try {
        stream = res.rows()
        for await (const row of stream) {
          if (count === 4) {
            abortController.abort()
          }
          results.push(row)
          count += 1
        }
      } catch (err) {
        assert.strictEqual(err.name, 'AbortError')
      }
      assert.strictEqual(results.length, expectedCount)
    })

    it('should cancel while iterating using query result cancel', async function () {
      const qs = `FROM RANGE(1, 100) AS i SELECT *`
      const results = []
      const expectedCount = 5
      let count = 0
      let res = await instance().executeQuery(qs)

      try {
        for await (const row of res.rows()) {
          if (count === expectedCount - 1) {
            res.cancel()
          }
          results.push(row)
          count += 1
        }
      } catch (err) {
        assert.strictEqual(err.name, 'AbortError')
      }
      assert.strictEqual(results.length, expectedCount)
    })
  })
}

describe('#Columnar query - cluster', function () {
  /* eslint-disable-next-line mocha/no-setup-in-describe */
  genericTests(() => H.c)
})
