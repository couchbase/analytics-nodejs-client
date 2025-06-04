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

'use strict'

const assert = require('chai').assert
const { runWithRetry, RequestBehaviour } = require('../lib/retries')

const H = require('./harness')
const { TimeoutError } = require('../lib/errors')

const whyIsNodeRunning = require('why-is-node-running')

describe('#Retries', function () {
  it('should retry on retriable errors and succeed eventually', async function () {
    let callCount = 0
    const failAttempts = 2

    const fn = async () => {
      callCount++
      if (callCount <= failAttempts) {
        throw new Error('Temporary failure')
      }
      return 'success'
    }

    const evaluate = (err) => {
      if (err.message === 'Temporary failure') {
        return RequestBehaviour.retry()
      }
      return RequestBehaviour.fail(err)
    }

    const result = await runWithRetry(fn, evaluate, Date.now() + 50000)
    assert.equal(result, 'success')
    assert.equal(callCount, failAttempts + 1)
  })

  it('should fail if deadline is exceeded', async function () {
    const fn = async () => {
      throw new Error('Temporary failure')
    }

    const evaluate = () => RequestBehaviour.retry()

    await H.throwsHelper(async () => {
      await runWithRetry(fn, evaluate, Date.now() + 500)
    }, TimeoutError)
  })

  it('should fail immediately on fatal error', async function () {
    let callCount = 0

    const fn = async () => {
      callCount++
      throw new Error('Fatal')
    }
    const evaluate = (err) => RequestBehaviour.fail(err)

    await H.throwsHelper(async () => {
      await runWithRetry(fn, evaluate, Date.now() + 500)
    }, Error)

    assert.equal(callCount, 1)
  })
})
