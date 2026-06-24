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

import { RequestContext } from './requestcontext.js'
import {
  AnalyticsError,
  InvalidCredentialError,
  QueryError,
  QueryNotFoundException,
  TimeoutError,
} from './errors.js'
import {
  ConnectionError,
  HttpStatusError,
  InternalConnectionTimeout,
} from './internalerrors.js'
import { RequestBehaviour } from './retries.js'

/**
 * Static class for shared error handling logic
 *
 * @internal
 */
export class ErrorHandler {
  /**
   * @internal
   */
  static handleErrors(errs: any, context: RequestContext): RequestBehaviour {
    if (errs instanceof HttpStatusError) {
      if (errs.statusCode === 401) {
        return RequestBehaviour.fail(
          new InvalidCredentialError(
            context.attachErrorContext('Invalid credentials')
          )
        )
      } else if (errs.statusCode === 404) {
        return RequestBehaviour.fail(
          new QueryNotFoundException(
            context.attachErrorContext('Query not found')
          )
        )
      } else if (errs.statusCode === 503) {
        return RequestBehaviour.retry(
          new AnalyticsError(
            context.attachErrorContext(
              'The server returned a 503 Service Unavailable error. This is likely a temporary issue.'
            )
          )
        )
      }

      return RequestBehaviour.fail(
        new AnalyticsError(
          context.attachErrorContext(
            `Unhandled HTTP status error occurred: ${errs}`
          )
        )
      )
    } else if (errs instanceof TimeoutError) {
      return RequestBehaviour.fail(errs)
    } else if (errs instanceof InternalConnectionTimeout) {
      return RequestBehaviour.retry(
        new TimeoutError(
          context.attachErrorContext(
            'Timed out attempting to establish a connection to a Node within the connectTimeout period.'
          )
        )
      )
    } else if (errs instanceof ConnectionError) {
      if (this._isRetriableConnectionError(errs)) {
        return RequestBehaviour.retry(
          new AnalyticsError(
            context.attachErrorContext(
              `Got a retriable connection error from the HTTP library, details: ${errs.cause}`
            )
          )
        )
      }

      return RequestBehaviour.fail(
        new AnalyticsError(
          context.attachErrorContext(
            `Got an unretriable error from the HTTP library, details: ${errs.cause.message}`
          )
        )
      )
    } else if (errs instanceof AnalyticsError) {
      return RequestBehaviour.fail(errs)
    } else if (errs.name && errs.name === 'AbortError') {
      // We consider AbortError a platform error so we don't wrap it in AnalyticsError
      return RequestBehaviour.fail(errs)
    } else if (Array.isArray(errs)) {
      // Server error array from query JSON response
      return this._parseServerErrors(errs, context)
    }

    return RequestBehaviour.fail(
      new AnalyticsError(
        context.attachErrorContext(`Unknown Error received: ${String(errs)}`)
      )
    )
  }

  /**
   * @internal
   */
  private static _parseServerErrors(
    errors: string[] | object[],
    context: RequestContext
  ): RequestBehaviour {
    const addRemainingErrorsToContext = () => {
      context.pushOtherServerErrors(
        ...errors.filter((_, i) => parsedErrors[i] !== selectedError)
      )
    }

    // Server error array from query JSON response
    let firstNonRetriableError: any = null
    let firstRetriableError: any = null

    // Each error will be a string if they come from the json streamer, or an object if they've already been parsed on a non-successful status code
    const parsedErrors = errors.map((err) =>
      typeof err === 'string' ? JSON.parse(err) : err
    )

    for (const jsonErr of parsedErrors) {
      const retriable = 'retriable' in jsonErr ? jsonErr.retriable : false

      if (!retriable && !firstNonRetriableError) {
        firstNonRetriableError = jsonErr
      }

      if (retriable && !firstRetriableError) {
        firstRetriableError = jsonErr
      }
    }

    const selectedError = firstNonRetriableError || firstRetriableError

    if (!selectedError) {
      return RequestBehaviour.fail(
        new AnalyticsError(
          context.attachErrorContext('Server returned an empty error array')
        )
      )
    }

    if (selectedError.code === 20000) {
      addRemainingErrorsToContext()
      return RequestBehaviour.fail(
        new InvalidCredentialError(
          context.attachErrorContext(
            `Server response indicated invalid credentials. Server message: ${selectedError.msg}. Server error code: ${selectedError.code}`
          )
        )
      )
    } else if (selectedError.code === 21002) {
      addRemainingErrorsToContext()
      return RequestBehaviour.fail(
        new TimeoutError(
          context.attachErrorContext(
            `Server side timeout occurred. Server message: ${selectedError.msg}. Server error code: ${selectedError.code}`
          )
        )
      )
    } else if (firstRetriableError && !firstNonRetriableError) {
      addRemainingErrorsToContext()
      return RequestBehaviour.retry(
        new QueryError(
          context.attachErrorContext(
            `Retriable server-side query error occurred: Server message: ${selectedError.msg}. Server error code: ${selectedError.code}`
          ),
          selectedError.msg,
          selectedError.code
        )
      )
    } else {
      addRemainingErrorsToContext()
      return RequestBehaviour.fail(
        new QueryError(
          context.attachErrorContext(
            `Server-side query error occurred: Server message: ${selectedError.msg}. Server error code: ${selectedError.code}`
          ),
          selectedError.msg,
          selectedError.code
        )
      )
    }
  }

  /**
   * @internal
   */
  private static _isRetriableConnectionError(error: ConnectionError): boolean {
    const nodeError = error.cause as NodeJS.ErrnoException
    if (!error.isRequestError || !nodeError || !nodeError.code) {
      return false
    }

    // Per the RFC, only DNS and TCP-dial failures retry; anything after the
    // socket is dialed (TLS handshake, cert verification) must fail fast.
    return connectionRetryAllowList.has(nodeError.code)
  }
}

/**
 * Node `err.code`s for the DNS and TCP-dial failures the RFC marks retriable.
 *
 * @internal
 */
const connectionRetryAllowList = new Set([
  // DNS resolution failures
  'EAI_AGAIN',
  'ENOTFOUND',
  // TCP-dial / connection failures
  'ECONNREFUSED',
  'ECONNRESET',
  'ECONNABORTED',
  'ETIMEDOUT',
  'EHOSTUNREACH',
  'EHOSTDOWN',
  'ENETUNREACH',
  'ENETDOWN',
  'ENETRESET',
  'EADDRNOTAVAIL',
  // EPIPE looks like a post-dial write failure, but it only reaches here as a
  // request-side error (isRequestError), meaning the peer tore down the
  // connection before accepting our request (e.g. a closed keep-alive socket,
  // an LB drop, or a restarting node). Nothing was processed, so retrying is
  // safe. A broken pipe after the server began responding surfaces as a
  // response-side error and fails fast instead.
  'EPIPE',
])
