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

import { PromiseHelper } from './utilities'
import { TimeoutError } from './errors'

/**
 * Represents the behaviour of a request during retries.
 *
 * @internal
 */
export class RequestBehaviour {
  private constructor(public readonly error: Error | null) {
    this.error = error
  }

  /**
   * Creates a new RequestBehaviour instance indicating that the request should be retried.
   */
  static retry(): RequestBehaviour {
    return new RequestBehaviour(null)
  }

  /**
   * Creates a new RequestBehaviour instance indicating that the request has failed and should not be retried.
   *
   * @param err The error that caused the failure.
   */
  static fail(err: Error): RequestBehaviour {
    return new RequestBehaviour(err)
  }

  /**
   * Indicates whether the request should be retried.
   */
  shouldRetry(): boolean {
    return this.error === null
  }

  /**
   * Returns the error that caused the request to fail.
   */
  getError(): Error {
    return this.error!
  }
}

/**
 *
 * Helper function that runs a function with retry logic based on the provided evaluation function.
 *
 * @param fn The function to execute, which returns a Promise.
 * @param evaluate A function that evaluates the error and returns a RequestBehaviour indicating whether to retry or fail.
 * @param deadline The deadline timestamp by which the operation must complete.
 */
export async function runWithRetry<T>(
  fn: () => Promise<T>,
  evaluate: (errs: any) => RequestBehaviour,
  deadline: number
): Promise<T> {
  let attempt = 0
  for (;;) {
    attempt++
    const remainingTime = deadline - Date.now()
    if (remainingTime <= 0) {
      throw new TimeoutError('Query timeout exceeded during retry')
    }

    try {
      return await PromiseHelper.promiseWithTimeout(fn(), remainingTime)
    } catch (err) {
      const behaviour = evaluate(err)
      if (behaviour.shouldRetry()) {
        const delay = calculateBackoff(attempt)
        if (Date.now() + delay > deadline) {
          throw new TimeoutError(
            'Query timeout will exceed during retry backoff'
          )
        }
        await sleep(delay)
      } else {
        throw behaviour.getError()
      }
    }
  }
}

/**
 * Calculates an exponential backoff delay based on the retry count.
 *
 * @param retryCount The number of retries that have been attempted.
 */
function calculateBackoff(retryCount: number) {
  const baseDelay = 100
  const maxDelay = 60_000

  const exponentialDelay = baseDelay * Math.pow(2, retryCount)
  const cappedDelay = Math.min(exponentialDelay, maxDelay)

  return Math.random() * cappedDelay
}

/**
 * @internal
 */
function sleep(delay: number) {
  return new Promise((resolve) => setTimeout(resolve, delay))
}
