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
import { runWithRetry, RequestBehaviour } from '../lib/retries.js'
import { harness } from './harness.js'
import {AnalyticsError, TimeoutError} from '../lib/errors.js'
import { RequestContext } from '../lib/requestcontext.js'

describe('#Retries', function () {
  it('should retry on retriable errors and succeed eventually', async function () {
    let callCount = 0
    const failAttempts = 2

    const fn = async (): Promise<string> => {
      callCount++
      if (callCount <= failAttempts) {
        throw new Error('Temporary failure')
      }
      return 'success'
    }

    const evaluate = (err: Error) => {
      if (err.message === 'Temporary failure') {
        return RequestBehaviour.retry(err)
      }
      return RequestBehaviour.fail(err)
    }

    const result = await runWithRetry(
      fn,
      evaluate,
      Date.now() + 50000,
      new RequestContext(7)
    )
    assert.equal(result, 'success')
    assert.equal(callCount, failAttempts + 1)
  })

  it('should fail if deadline is exceeded', async function () {
    const fn = async (): Promise<never> => {
      throw new Error('Temporary failure')
    }

    const evaluate = (err: any) => RequestBehaviour.retry(err)

    await harness.throwsHelper(async () => {
      await runWithRetry(
        fn,
        evaluate,
        Date.now() + 500,
        new RequestContext(7)
      )
    }, TimeoutError)
  })

  it('should fail immediately on fatal error', async function () {
    let callCount = 0

    const fn = async (): Promise<never> => {
      callCount++
      throw new Error('Fatal')
    }
    const evaluate = (err: Error) => RequestBehaviour.fail(err)

    await harness.throwsHelper(async () => {
      await runWithRetry(
        fn,
        evaluate,
        Date.now() + 500,
        new RequestContext(7)
      )
    }, Error)

    assert.equal(callCount, 1)
  })

  it('should fail with the final error if retries are exceeded', async function () {
    this.timeout(5000)

    let callCount = 0
    const context = new RequestContext(3)

    const fn = async (): Promise<never> => {
      callCount++
      throw new Error('Temporary failure')
    }

    const evaluate = (errs: any) => {
      return RequestBehaviour.retry(errs)
    }

    try {
        await runWithRetry(
            fn,
            evaluate,
            Date.now() + 5000,
            context
        )
      assert(false)
    } catch (e) {
      assert.instanceOf(e, Error)
      assert.include(e.message, 'Temporary failure')
      assert.equal(callCount, 4)
    }
  })
})
