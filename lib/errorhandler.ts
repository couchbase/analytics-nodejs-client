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
  InvalidCredentialError,
  QueryError,
  TimeoutError,
} from './errors'
import {
  ConnectionError,
  DnsRecordsExhaustedError,
  HttpStatusError,
  InternalConnectionTimeout,
} from './internalerrors'
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
            context.attachErrorContext('Invalid credentials')
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
      context.markRecordAsUsed(errs.DnsRecord)
      if (context.recordsExhausted()) {
        return RequestBehaviour.fail(
          new TimeoutError(
            'Timed out attempting to connect to all available DNS records'
          )
        )
      }
      context.setPreviousAttemptErrors(errs)
      return RequestBehaviour.retry()
    } else if (errs instanceof DnsRecordsExhaustedError) {
      return RequestBehaviour.fail(
        new AnalyticsError(
          context.attachErrorContext(
            'Attempted to perform query on every resolved DNS record, but all of them failed to connect.'
          )
        )
      )
    } else if (errs instanceof ConnectionError) {
      if (this._isRetriableConnectionError(errs)) {
        context.setPreviousAttemptErrors(errs)
        if (errs.DnsRecord) context.markRecordAsUsed(errs.DnsRecord)
        return RequestBehaviour.retry()
      }

      return RequestBehaviour.fail(
        new AnalyticsError(
          context.attachErrorContext(
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
        context.attachErrorContext(`Error received: ${String(errs)}`)
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
          context.attachErrorContext('Server returned an empty error array')
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
      context.setPreviousAttemptErrors(errors)
      return RequestBehaviour.retry()
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
    const nodeError = error.Cause as NodeJS.ErrnoException
    if (!error.isRequestError || !nodeError || !nodeError.code) {
      return false
    }

    return !connectionDenyList.has(nodeError.code)
  }
}

/**
 * Taken from https://github.com/sindresorhus/is-retry-allowed
 * @internal
 */
const connectionDenyList = new Set([
  'ENOTFOUND',
  'ENETUNREACH',
  'UNABLE_TO_GET_ISSUER_CERT',
  'UNABLE_TO_GET_CRL',
  'UNABLE_TO_DECRYPT_CERT_SIGNATURE',
  'UNABLE_TO_DECRYPT_CRL_SIGNATURE',
  'UNABLE_TO_DECODE_ISSUER_PUBLIC_KEY',
  'CERT_SIGNATURE_FAILURE',
  'CRL_SIGNATURE_FAILURE',
  'CERT_NOT_YET_VALID',
  'CERT_HAS_EXPIRED',
  'CRL_NOT_YET_VALID',
  'CRL_HAS_EXPIRED',
  'ERROR_IN_CERT_NOT_BEFORE_FIELD',
  'ERROR_IN_CERT_NOT_AFTER_FIELD',
  'ERROR_IN_CRL_LAST_UPDATE_FIELD',
  'ERROR_IN_CRL_NEXT_UPDATE_FIELD',
  'OUT_OF_MEM',
  'DEPTH_ZERO_SELF_SIGNED_CERT',
  'SELF_SIGNED_CERT_IN_CHAIN',
  'UNABLE_TO_GET_ISSUER_CERT_LOCALLY',
  'UNABLE_TO_VERIFY_LEAF_SIGNATURE',
  'CERT_CHAIN_TOO_LONG',
  'CERT_REVOKED',
  'INVALID_CA',
  'PATH_LENGTH_EXCEEDED',
  'INVALID_PURPOSE',
  'CERT_UNTRUSTED',
  'CERT_REJECTED',
  'HOSTNAME_MISMATCH',
])
