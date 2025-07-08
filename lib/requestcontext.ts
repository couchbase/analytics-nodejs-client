import { ErrorContext } from './internalerrors.js'
import * as http from 'node:http'

/**
 * Shared request context for tracking DNS records and the error context
 *
 * @internal
 */
export class RequestContext {
  private _errorContext: ErrorContext
  private _numAttempts: number
  private _maxRetryAttempts: number

  constructor(maxRetryAttempts: number) {
    this._errorContext = new ErrorContext()
    this._maxRetryAttempts = maxRetryAttempts
    this._numAttempts = 0
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
  get numAttempts(): number {
    return this._numAttempts
  }

  /**
   * @internal
   */
  get maxRetryAttempts(): number {
    return this._maxRetryAttempts
  }

  /**
   * @internal
   */
  incrementAttempt(): void {
    this._numAttempts++
    this._errorContext.numAttempts = this._numAttempts
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
  pushOtherServerErrors(...errors: any): void {
    this._errorContext.otherServerErrors.push(errors)
  }

  /**
   * @internal
   */
  attachErrorContext(message: string): string {
    return `${message}. ${this._errorContext.toString()}`
  }

  /**
   * @internal
   */
  addPreviousAttemptErrorToContext(errors: any): void {
    if (errors) {
      this._errorContext.previousAttemptErrors = errors
    }
  }
}
