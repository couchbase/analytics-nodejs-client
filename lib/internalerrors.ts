/**
 * Indicates that we failed to connect to a node within the timeout period.
 *
 * @internal
 */
export class InternalConnectionTimeout extends Error {
  private dnsRecord: string
  constructor(dnsRecord: string) {
    super('Timed out waiting to connect to node')
    this.dnsRecord = dnsRecord
  }

  /**
   * @internal
   */
  get DnsRecord(): string {
    return this.dnsRecord
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
    super('HttpStatusCode error: ' + statusCode)
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
export class ConnectionError extends Error {
  private cause: Error
  private request: boolean
  private dnsRecord?: string

  constructor(err: Error, request: boolean, dnsRecord?: string) {
    super(`ConnectionError: ${err.message}`)
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
  lastDispatchedTo: string | undefined
  lastDispatchedFrom: string | undefined
  path: string | undefined
  method: string | undefined
  statusCode: number | undefined
  statement: string | undefined
  previousAttemptErrors: any
  otherServerErrors: any[] = []
  numAttempts: number = 0

  /**
   * @internal
   */
  toString(): string {
    const parts: string[] = []
    if (this.lastDispatchedTo)
      parts.push(`lastDispatchedTo=${this.lastDispatchedTo}`)
    if (this.lastDispatchedFrom)
      parts.push(`lastDispatchedFrom=${this.lastDispatchedFrom}`)
    if (this.method) parts.push(`method=${this.method}`)
    if (this.path) parts.push(`path=${this.path}`)
    if (this.statusCode) parts.push(`statusCode=${this.statusCode}`)
    if (this.statement) parts.push(`statement=${this.statement}`)
    if (this.previousAttemptErrors)
      parts.push(`previousAttemptErrors=${this.previousAttemptErrors}`)
    if (this.numAttempts) parts.push(`numAttempts=${this.numAttempts}`)
    if (this.otherServerErrors.length > 0)
      parts.push(`otherServerErrors=${this.otherServerErrors}`)
    return `ErrorContext: ${parts.join(', ')}`
  }
}
