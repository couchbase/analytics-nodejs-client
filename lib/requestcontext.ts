import { DnsClient } from './dnsclient'
import { ErrorContext } from './errors'
import http from 'node:http'

/**
 * Shared request context for tracking DNS records and the error context
 *
 * @internal
 */
export class RequestContext {
  private _dnsClient: DnsClient
  private _errorContext: ErrorContext

  constructor(hostname: string) {
    this._dnsClient = new DnsClient(hostname)
    this._errorContext = new ErrorContext()
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
  incrementAttempt(): void {
    this._errorContext.numAttempts++
  }

  /**
   * @internal
   */
  async incrementAttemptAndGetRecord(): Promise<string> {
    this.incrementAttempt()
    return await this._dnsClient.maybeUpdateAndGetRandomRecord()
  }

  /**
   * @internal
   */
  markRecordAsUsed(dnsRecord: string): void {
    this._dnsClient.markRecordAsUsed(dnsRecord)
  }

  /**
   * @internal
   */
  recordsExhausted(): boolean {
    return this._dnsClient.getAvailableRecords().length === 0
  }

  /**
   * @internal
   */
  updateGenericResContextFields(res: http.IncomingMessage): void {
    if (res.socket.remoteAddress)
      this.errorContext.lastDispatchedTo = res.socket.remoteAddress
    if (res.socket.localAddress)
      this.errorContext.lastDispatchedFrom = res.socket.localAddress
    if (res.statusCode) this.errorContext.statusCode = res.statusCode
  }

  /**
   * @internal
   */
  setGenericRequestContextFields(
    statement: string,
    path: string,
    method: string
  ): void {
    this._errorContext.statement = statement
    this._errorContext.path = path
    this._errorContext.method = method
  }

  /**
   * @internal
   */
  setPreviousAttemptErrors(errors: any): void {
    this._errorContext.previousAttemptErrors = errors
  }

  /**
   * @internal
   */
  pushOtherServerErrors(...errors: any): void {
    this._errorContext.otherServerErrors.push(errors)
  }

  /**
   * @internal
   */
  attachErrorContext(message: string): string {
    return `${message}. ${this._errorContext.toString()}`
  }
}
