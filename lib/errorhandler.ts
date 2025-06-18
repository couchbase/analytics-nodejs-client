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

import { RequestContext } from './requestcontext'
import {
  AnalyticsError,
  DnsRecordsExhaustedError,
  HttpLibraryError,
  HttpStatusError,
  InvalidCredentialError,
  QueryError,
  TimeoutError,
} from './errors'
import { RequestBehaviour } from './retries'

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
      if (errs.StatusCode === 401) {
        return RequestBehaviour.fail(
          new InvalidCredentialError(
            context.createErrorMessage('Invalid credentials')
          )
        )
      }

      return RequestBehaviour.fail(
        new AnalyticsError(
          context.createErrorMessage(
            `Unhalded HTTP status error occurred: ${errs}`
          )
        )
      )
    } else if (errs instanceof TimeoutError) {
      return RequestBehaviour.fail(errs)
    } else if (errs instanceof DnsRecordsExhaustedError) {
      return RequestBehaviour.fail(
        new AnalyticsError(
          context.createErrorMessage(
            'Attempted to perform query on every resolved DNS record, but all of them failed to connect.'
          )
        )
      )
    } else if (errs instanceof HttpLibraryError) {
      if (this._isRetriableConnectionError(errs)) {
        context.setPreviousAttemptErrors(errs)
        context.markRecordAsUsed(errs.DnsRecord as string)
        return RequestBehaviour.retry()
      }

      return RequestBehaviour.fail(
        new AnalyticsError(
          context.createErrorMessage(
            `Got an unretriable error from the HTTP library, details: ${errs.Cause.message}`
          )
        )
      )
    } else if (errs.name && errs.name === 'AbortError') {
      // We consider AbortError a platform error so we don't wrap it in AnalyticsError
      return RequestBehaviour.fail(errs)
    } else if (Array.isArray(errs)) {
      // Server error array from query JSON response
      return this._parseServerErrors(errs, context)
    }

    return RequestBehaviour.fail(
      new AnalyticsError(
        context.createErrorMessage(`Error received: ${String(errs)}`)
      )
    )
  }

  /**
   * @internal
   */
  private static _parseServerErrors(
    errors: string[],
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

    const parsedErrors = errors.map((err) => JSON.parse(err))

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
      context.pushOtherServerErrors(...errors)
      return RequestBehaviour.fail(
        new AnalyticsError(
          context.createErrorMessage('Server returned an empty error array')
        )
      )
    }

    if (selectedError.code === 20000) {
      addRemainingErrorsToContext()
      context.pushOtherServerErrors(
        ...errors.filter((_, i) => parsedErrors[i] !== selectedError)
      )

      return RequestBehaviour.fail(
        new InvalidCredentialError(
          context.createErrorMessage(
            `Server response indicated invalid credentials. Server message: ${selectedError.msg}. Server error code: ${selectedError.code}`
          )
        )
      )
    } else if (selectedError.code === 21002) {
      addRemainingErrorsToContext()
      return RequestBehaviour.fail(
        new TimeoutError(
          context.createErrorMessage(
            `Server side timeout occurred. Server message: ${selectedError.msg}. Server error code: ${selectedError.code}`
          )
        )
      )
    } else if (firstRetriableError && !firstNonRetriableError) {
      context.setPreviousAttemptErrors(errors)
      return RequestBehaviour.retry()
    } else {
      addRemainingErrorsToContext()
      return RequestBehaviour.fail(
        new QueryError(
          context.createErrorMessage(
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
  private static _isRetriableConnectionError(error: HttpLibraryError): boolean {
    // TODO: Figure out a more robust and correct way of determining if the error is a connection error. Perhaps something like what axios-retry does: https://github.com/softonic/axios-retry/blob/master/src/index.ts#L95
    const nodeError = error.Cause as NodeJS.ErrnoException
    if (!error.isRequestError || !nodeError || !nodeError.code) {
      return false
    }

    return CONNECTION_ERROR_CODES.has(nodeError.code)
  }
}

/**
 * Taken from https://man7.org/linux/man-pages/man3/errno.3.html
 * @internal
 */
const CONNECTION_ERROR_CODES = new Set([
  'ECONNREFUSED',
  'ECONNRESET',
  'ECONNABORTED',
  'ETIMEDOUT',
  'ENOTFOUND',
  'ENETDOWN',
  'ENETRESET',
  'ENETUNREACH',
  'EHOSTDOWN',
  'EHOSTUNREACH',
  'EPIPE',
])
