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

import { Cluster } from './cluster'
import {
  QueryMetadata,
  QueryOptions,
  QueryResult,
  QueryResultStream,
  QueryScanConsistency,
} from './querytypes'
import * as http from 'node:http'
import { Parser, parser } from 'stream-json'
import { pipeline } from 'node:stream'
import { RequestBehaviour, runWithRetry } from './retries'
import {
  AnalyticsError,
  DnsRecordsExhaustedError,
  ErrorContext,
  HttpLibraryError,
  HttpStatusError,
  InvalidCredentialError,
  QueryError,
  TimeoutError,
} from './errors'
import { randomUUID } from 'node:crypto'
import { Deserializer } from './deserializers'
import { JsonTokenParserStream, PrimitiveFrame } from './jsonparser'
import https from 'node:https'
import { DnsClient } from './dnsclient'

/**
 * @internal
 */
export class QueryExecutor {
  private _cluster: Cluster
  private _dnsClient: DnsClient
  private _databaseName: string | undefined
  private _scopeName: string | undefined
  private _metadata: QueryMetadata | undefined
  private _deserializer: Deserializer
  private _abortController: AbortController
  private _signal: AbortSignal
  private _errorContext: ErrorContext

  /**
   * @internal
   */
  constructor(
    cluster: Cluster,
    deserializer: Deserializer,
    signal?: AbortSignal,
    databaseName?: string,
    scopeName?: string
  ) {
    this._cluster = cluster
    this._dnsClient = new DnsClient(cluster.httpClient.hostname)
    this._databaseName = databaseName
    this._scopeName = scopeName
    this._deserializer = deserializer
    this._abortController = new AbortController()
    this._errorContext = new ErrorContext()
    this._signal = signal
      ? AbortSignal.any([this._abortController.signal, signal])
      : this._abortController.signal

    this._signal.addEventListener('abort', () => {
      this.handleAbort()
    })
  }

  /**
   * @internal
   */
  get metadata(): QueryMetadata | undefined {
    return this._metadata
  }

  /**
   * @internal
   */
  get deserializer(): Deserializer {
    return this._deserializer
  }

  /**
   * @internal
   */
  get errorContext(): ErrorContext {
    return this._errorContext
  }

  /**
   * @internal
   */
  async query(statement: string, options: QueryOptions): Promise<QueryResult> {
    const deadline =
      Date.now() + (options.timeout || this._cluster.queryTimeout)

    this._errorContext.statement = `'${statement}'`
    this._errorContext.path = '/api/v1/request'
    this._errorContext.method = 'POST'

    const encodedOptions = this._buildRequestOptions(statement, options)
    const body = JSON.stringify(encodedOptions)

    const requestOptions: http.RequestOptions = {
      ...this._cluster.httpClient.genericRequestOptions(),
      method: 'POST',
      path: '/api/v1/request',
      headers: {
        'Content-Length': Buffer.byteLength(body),
        'Content-Type': 'application/json',
        ...(options.priority ? { 'Analytics-Priority': '-1' } : {}),
      },
    }

    let res
    try {
      res = await runWithRetry(
        () => this._attemptQuery(requestOptions, body, deadline),
        (errs) => this._handleErrors(errs),
        deadline
      )
    } catch (error) {
      // Special case with TimeoutError, since it can come from a static context in runWithRetry, we attach the error context here.
      if (error instanceof TimeoutError) {
        error.message = `${error.message}. ${this._errorContext.toString()}`
      }
      throw error
    }

    return res
  }

  /**
   * Attempts to execute a query.
   *
   * @internal
   */
  async _attemptQuery(
    requestOptions: http.RequestOptions,
    body: string,
    deadline: number
  ): Promise<QueryResult> {
    this._errorContext.numAttempts++
    requestOptions.hostname = await this._dnsClient.updateAndGetRandomRecord()

    return new Promise((resolve, reject) => {
      const abortHandler = () => {
        req.destroy()
        return reject(this._signal.reason)
      }

      this._signal.addEventListener('abort', abortHandler)

      const req = this._cluster.httpClient.module.request(
        requestOptions,
        (res) => {
          this._handleResponse(res, resolve, reject, deadline)
        }
      )

      req.once('close', () => {
        this._signal.removeEventListener('abort', abortHandler)
      })

      req.on('error', (err) => {
        req.destroy()
        this._signal.removeEventListener('abort', abortHandler)
        reject(
          new HttpLibraryError(err, true, requestOptions.hostname as string)
        )
      })

      this._attachConnectTimeout(req)

      req.write(body)
      req.end()
    })
  }

  private _handleResponse(
    res: http.IncomingMessage,
    resolve: (value: QueryResult) => void,
    reject: (err: any) => void,
    deadline: number
  ): void {
    res.once('error', (err) => {
      res.destroy()
      return reject(new HttpLibraryError(err, false))
    })

    if (res.socket.remoteAddress)
      this._errorContext.address = res.socket.remoteAddress
    if (res.statusCode) this._errorContext.statusCode = res.statusCode

    // TODO: Other HTTP status codes, 50X? Unsure if we are too eagerly assuming a valid response body
    if (res.statusCode === 401) {
      res.destroy()
      return reject(new HttpStatusError(res.statusCode))
    }

    const jsonTokenizer: Parser = parser()
    const jsonTokenParser = new JsonTokenParserStream()
    const queryStream = new QueryResultStream(this, deadline, this._signal)

    jsonTokenizer.once('error', (err) => {
      res.destroy()
      reject(
        new AnalyticsError(
          `Got an error parsing server JSON response, details: ${err.message}. ${this._errorContext.toString()}`
        )
      )
    })

    queryStream.once('readable', () => {
      return resolve(new QueryResult(this, queryStream))
    })

    queryStream.once('end', () => {
      this._metadata = QueryMetadata.parse(
        (jsonTokenParser.stack.pop() as PrimitiveFrame).value
      )
    })

    jsonTokenParser.once('errorsComplete', (errors) => {
      if (errors.length) {
        res.destroy()
        queryStream.destroy()
        return reject(errors)
      }
    })

    pipeline(res, jsonTokenizer, jsonTokenParser, queryStream, (err) => {
      if (err)
        return reject(
          new AnalyticsError(
            `Error occurred during query pipeline: ${err.message}. ${this._errorContext.toString()}`
          )
        )
    })
  }

  private _attachConnectTimeout(req: http.ClientRequest): void {
    req.on('socket', (socket) => {
      if (!socket.connecting) {
        return
      }

      const timeoutMs = this._cluster.connectTimeout

      const timeoutId = setTimeout(() => {
        req.destroy()
        req.emit(
          'error',
          new TimeoutError(
            `Timed out waiting for the connection. ${this._errorContext.toString()}`
          )
        )
      }, timeoutMs)

      const clear = () => clearTimeout(timeoutId)

      if (this._cluster.httpClient.module === https) {
        socket.once('secureConnect', clear)
      } else {
        socket.once('connect', clear)
      }

      socket.once('close', clear)
    })
  }

  /**
   * @internal
   */
  private _buildRequestOptions(
    statement: string,
    options: QueryOptions
  ): BuiltQueryRequest {
    const opts: Partial<BuiltQueryRequest> = {
      statement: statement,
      client_context_id: options.clientContextId || randomUUID(),
    }

    if (this._databaseName && this._scopeName) {
      opts.query_context = `default:\`${this._databaseName}\`.\`${this._scopeName}\``
    }

    if (options.positionalParameters) {
      opts.args = options.positionalParameters
    }
    if (options.namedParameters) {
      for (const [origK, v] of Object.entries(options.namedParameters)) {
        const k = origK.startsWith('$') ? origK : `$${origK}`
        opts[k] = v
      }
    }
    if (options.readOnly !== undefined) {
      opts.readonly = options.readOnly
    }
    if (options.scanConsistency) {
      switch (options.scanConsistency) {
        case QueryScanConsistency.NotBounded:
          opts.scan_consistency = 'not_bounded'
          break
        case QueryScanConsistency.RequestPlus:
          opts.scan_consistency = 'request_plus'
          break
      }
    }
    const timeout = options.timeout || this._cluster.queryTimeout
    const serverTimeout = timeout + 5000
    opts.timeout = `${serverTimeout}ms`

    if (options.raw) {
      for (const [k, v] of Object.entries(options.raw)) {
        opts[k] = v
      }
    }

    return opts as BuiltQueryRequest
  }

  /**
   * @internal
   */
  _handleErrors(errs: any): RequestBehaviour {
    if (errs instanceof HttpStatusError) {
      if (errs.StatusCode === 401) {
        return RequestBehaviour.fail(
          new InvalidCredentialError(
            `Invalid credentials. ${this._errorContext.toString()}`
          )
        )
      }

      return RequestBehaviour.fail(
        new AnalyticsError(
          `Unhandled HTTP status error occurred: ${errs}. ${this._errorContext.toString()}`
        )
      )
    } else if (errs instanceof TimeoutError) {
      return RequestBehaviour.fail(errs)
    } else if (errs instanceof DnsRecordsExhaustedError) {
      return RequestBehaviour.fail(
        new AnalyticsError(
          `Attempted to perform query on every resolved DNS record, but all of them failed to connect. ${this._errorContext.toString()}`
        )
      )
    } else if (errs instanceof HttpLibraryError) {
      if (this._isRetriableConnectionError(errs)) {
        this._errorContext.previousAttemptErrors = errs
        this._dnsClient.markRecordAsUsed(errs.DnsRecord as string)
        return RequestBehaviour.retry()
      }

      return RequestBehaviour.fail(
        new AnalyticsError(
          'Got an unretriable error from the HTTP library, details: ' +
            errs.Cause.message +
            `. ${this._errorContext.toString()}`
        )
      )
    } else if (errs.name && errs.name === 'AbortError') {
      // We consider AbortError a platform error so we don't wrap it in AnalyticsError
      return RequestBehaviour.fail(errs)
    } else if (Array.isArray(errs)) {
      // Server error array from query JSON response
      return this._parseServerErrors(errs)
    }

    return RequestBehaviour.fail(
      new AnalyticsError(
        `Error received: ${String(errs)}. ${this._errorContext.toString()}`
      )
    )
  }

  private _parseServerErrors(errors: string[]): RequestBehaviour {
    const addRemainingErrorsToContext = () => {
      this._errorContext.otherServerErrors.push(
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
      this._errorContext.otherServerErrors.push(...errors)
      return RequestBehaviour.fail(
        new AnalyticsError(
          `Server returned an empty error array. ${this._errorContext.toString()}`
        )
      )
    }

    if (selectedError.code === 20000) {
      addRemainingErrorsToContext()
      this._errorContext.otherServerErrors.push(
        ...errors.filter((_, i) => parsedErrors[i] !== selectedError)
      )
      return RequestBehaviour.fail(
        new InvalidCredentialError(
          `Server response indicated invalid credentials. Server message: ${selectedError.msg}. Server error code: ${selectedError.code}. ${this._errorContext.toString()}`
        )
      )
    } else if (selectedError.code === 21002) {
      addRemainingErrorsToContext()
      return RequestBehaviour.fail(
        new TimeoutError(
          `Server side timeout occurred. Server message: ${selectedError.msg}. Server error code: ${selectedError.code}. ${this._errorContext.toString()}`
        )
      )
    } else if (firstRetriableError && !firstNonRetriableError) {
      this._errorContext.previousAttemptErrors = errors
      return RequestBehaviour.retry()
    } else {
      addRemainingErrorsToContext()
      return RequestBehaviour.fail(
        new QueryError(
          `Server-side query error occurred: Server message: ${selectedError.msg}. Server error code: ${selectedError.code}. ${this._errorContext.toString()}`,
          selectedError.msg,
          selectedError.code
        )
      )
    }
  }

  /**
   * @internal
   */
  private _isRetriableConnectionError(error: HttpLibraryError): boolean {
    // TODO: Figure out a more robust and correct way of determining if the error is a connection error. Perhaps something like what axios-retry does: https://github.com/softonic/axios-retry/blob/master/src/index.ts#L95
    const nodeError = error.Cause as NodeJS.ErrnoException
    if (!error.isRequestError || !nodeError || !nodeError.code) {
      return false
    }

    return CONNECTION_ERROR_CODES.has(nodeError.code)
  }

  /**
   * @internal
   */
  handleAbort(): void {
    if (!this._signal.aborted) {
      this._abortController.abort()
    }
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

/**
 * @internal
 */
type BuiltQueryRequest = {
  statement: string
  client_context_id: string
  query_context?: string
  args?: any[]
  readonly?: boolean
  scan_consistency?: 'not_bounded' | 'request_plus'
  timeout: string
  [key: string]: any // named params and raw options
}
