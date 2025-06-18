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
import { runWithRetry } from './retries'
import {
  AnalyticsError,
  HttpLibraryError,
  HttpStatusError,
  TimeoutError,
} from './errors'
import { randomUUID } from 'node:crypto'
import { Deserializer } from './deserializers'
import { JsonTokenParserStream, PrimitiveFrame } from './jsonparser'
import https from 'node:https'
import { RequestContext } from './requestcontext'
import { ErrorHandler } from './errorhandler'

/**
 * @internal
 */
export class QueryExecutor {
  private _cluster: Cluster
  private _requestContext: RequestContext
  private _databaseName: string | undefined
  private _scopeName: string | undefined
  private _metadata: QueryMetadata | undefined
  private _deserializer: Deserializer
  private _abortController: AbortController
  private _signal: AbortSignal

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
    this._databaseName = databaseName
    this._scopeName = scopeName
    this._deserializer = deserializer
    this._requestContext = new RequestContext(this._cluster.httpClient.hostname)
    this._abortController = new AbortController()
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
  get requestContext(): RequestContext {
    return this._requestContext
  }

  /**
   * @internal
   */
  async query(statement: string, options: QueryOptions): Promise<QueryResult> {
    const deadline =
      Date.now() + (options.timeout || this._cluster.queryTimeout)

    this._requestContext.setGenericRequestContextFields(
      statement,
      '/api/v1/request',
      'POST'
    )
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
        (errs) => ErrorHandler.handleErrors(errs, this._requestContext),
        deadline
      )
    } catch (error) {
      // Special case with TimeoutError, since it can come from a static context in runWithRetry, we attach the error context here.
      if (error instanceof TimeoutError) {
        error.message = this._requestContext.createErrorMessage(error.message)
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
    requestOptions.hostname = this.requestContext.incrementAttemptAndGetRecord()

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

      req.on('connectTimeout', () => {
        req.destroy()
        this._signal.removeEventListener('abort', abortHandler)
        // TODO TimeoutError will result in a 'fast-fail', do we instead want to retry with another DNS record?
        reject(new TimeoutError(`Timed out connecting to ${req.host}`))
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

    this._requestContext.updateGenericResContextFields(res)

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
          this._requestContext.createErrorMessage(
            `Got an error parsing server JSON response, details: ${err.message}`
          )
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
            this._requestContext.createErrorMessage(
              `Error occurred during query pipeline: ${err.message}`
            )
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
        req.emit('connectTimeout')
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
  handleAbort(): void {
    if (!this._signal.aborted) {
      this._abortController.abort()
    }
  }
}

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
