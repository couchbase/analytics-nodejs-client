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

/**
 * A generic base error that all non-platform errors inherit.  Exposes the cause and
 * context of the error to enable easier debugging.
 *
 * @category Error Handling
 */
export class AnalyticsError extends Error {
  constructor(message: string) {
    super(message)
    this.name = this.constructor.name
  }
}

/**
 * Indicates that the user credentials are incorrect.
 *
 * @category Error Handling
 */
export class InvalidCredentialError extends AnalyticsError {
  constructor(message: string) {
    super(message)
  }
}

/**
 * Indicates that an interaction with the Columnar cluster does not complete before its timeout expires.
 *
 * @category Error Handling
 */
export class TimeoutError extends AnalyticsError {
  constructor(message: string) {
    super(message)
  }
}

/**
 * Indicates that the Columnar cluster returned an error message in response to a query request.
 *
 * @category Error Handling
 */
export class QueryError extends AnalyticsError {
  /**
   * A human-readable error message sent by the server, without the additional context contained in {@link Error.message}.
   */
  serverMessage: string

  // TODO: Add docs reference link with error codes
  /**
   * The Columnar error code sent by the server.
   */
  code: number

  constructor(message: string, serverMessage: string, code: number) {
    super(message)
    this.serverMessage = serverMessage
    this.code = code
  }
}

/**
 * Indicates that one of the passed arguments was invalid.
 *
 * @category Error Handling
 */
export class InvalidArgumentError extends Error {
  constructor(message: string) {
    super(message)
    this.name = this.constructor.name
  }
}

/**
 * Internal wrapper to indicate that the server returned an errored status code.
 *
 * @internal
 */
export class HttpStatusError extends Error {
  private statusCode: number

  constructor(statusCode: number) {
    super('HttpStatusCode error')
    this.name = this.constructor.name
    this.statusCode = statusCode
  }

  /**
   * @internal
   */
  get StatusCode(): number {
    return this.statusCode
  }
}

/**
 * Internal wrapper which represents an error that was emitted either by the HTTP request or response
 *
 * @internal
 */
export class HttpLibraryError extends Error {
  private cause: Error
  private request: boolean
  private dnsRecord?: string

  constructor(err: Error, request: boolean, dnsRecord?: string) {
    super('HttpLibraryError')
    this.name = this.constructor.name
    this.cause = err
    this.request = request
    this.dnsRecord = dnsRecord
  }

  /**
   * @internal
   */
  get Cause(): Error {
    return this.cause
  }

  /**
   * @internal
   */
  get isRequestError(): boolean {
    return this.request
  }

  /**
   * @internal
   */
  get DnsRecord(): string | undefined {
    return this.dnsRecord
  }
}

/**
 * @internal
 */
export class DnsRecordsExhaustedError extends Error {
  constructor(message: string) {
    super(message)
    this.name = this.constructor.name
  }
}

/**
 * @internal
 */
export class ErrorContext {
  address: string | undefined
  path: string | undefined
  method: string | undefined
  statusCode: number | undefined
  statement: string | undefined
  previousAttemptErrors: any
  otherServerErrors: any[] | undefined
  numAttempts: number = 0

  /**
   * @internal
   */
  toString(): string {
    const parts: string[] = []
    if (this.address) parts.push(`address=${this.address}`)
    if (this.method) parts.push(`method=${this.method}`)
    if (this.path) parts.push(`path=${this.path}`)
    if (this.statusCode) parts.push(`statusCode=${this.statusCode}`)
    if (this.statement) parts.push(`statement=${this.statement}`)
    if (this.previousAttemptErrors)
      parts.push(`previousAttemptErrors=${this.previousAttemptErrors}`)
    if (this.numAttempts) parts.push(`numAttempts=${this.numAttempts}`)
    if (this.otherServerErrors)
      parts.push(`otherServerErrors=${this.otherServerErrors}`)
    return `ErrorContext: ${parts.join(', ')}`
  }
}
