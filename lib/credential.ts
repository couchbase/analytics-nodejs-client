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

import * as http from 'node:http'
import { InvalidArgumentError } from './errors.js'

// eslint-disable-next-line no-control-regex
const CONTROL_CHAR_RE = /[\x00-\x1F\x7F]/

/**
 * Reject any control character (including CR/LF/NUL) in a value that will be
 * interpolated into an HTTP header, to prevent header-injection.
 *
 * @internal
 */
function assertNoControlChars(value: string, fieldName: string): void {
  if (CONTROL_CHAR_RE.test(value)) {
    throw new InvalidArgumentError(
      `${fieldName} must not contain control characters.`
    )
  }
}

/**
 * The kind of authentication a credential carries.
 *
 * @internal
 */
export enum CredentialType {
  /** Username and password authentication, sent as HTTP Basic. */
  Password = 'password',
  /** JSON Web Token authentication, sent as HTTP Bearer. */
  Jwt = 'jwt',
}

/**
 * Credential carries authentication material for an Analytics cluster.
 *
 * Use `new Credential(username, password)` (or the equivalent
 * {@link Credential.of}) for RBAC, or {@link Credential.ofJwt} for a JSON
 * Web Token.
 *
 * @category Authentication
 */
export class Credential {
  /** The username to authenticate with. Empty for JWT credentials. */
  username: string

  /** The password to authenticate with. Empty for JWT credentials. */
  password: string

  /** @internal */
  protected _credentialType: CredentialType

  /**
   * Constructs a {@link Credential} for an RBAC username and password. The
   * SDK sends `Authorization: Basic <base64(user:pass)>` on every request.
   *
   * @param username The username to authenticate with.
   * @param password The password to authenticate with.
   */
  constructor(username: string, password: string) {
    if (typeof username !== 'string') {
      throw new InvalidArgumentError('Username must be a string.')
    }
    if (typeof password !== 'string') {
      throw new InvalidArgumentError('Password must be a string.')
    }
    if (username.includes(':')) {
      throw new InvalidArgumentError(
        "Username must not contain ':' (the HTTP Basic auth separator)."
      )
    }
    this.username = username
    this.password = password
    this._credentialType = CredentialType.Password
  }

  /**
   * The kind of credential this instance carries. Used to enforce same-type
   * rotation in `Cluster.setCredential`.
   *
   * @internal
   */
  get credentialType(): CredentialType {
    return this._credentialType
  }

  /**
   * Apply this credential's auth to an outgoing HTTP request.
   *
   * @internal
   */
  applyToRequest(opts: http.RequestOptions): void {
    const headers = (opts.headers ??= {}) as Record<string, string>
    headers.Authorization =
      'Basic ' +
      Buffer.from(`${this.username}:${this.password}`, 'utf8').toString(
        'base64'
      )
  }

  /**
   * Equivalent to `new Credential(username, password)`.
   *
   * @param username The username to authenticate with.
   * @param password The password to authenticate with.
   */
  static of(username: string, password: string): Credential {
    return new Credential(username, password)
  }

  /**
   * Construct a {@link Credential} from a JSON Web Token. The SDK sends
   * `Authorization: Bearer <token>` on every request. To rotate the token
   * before it expires, pass a fresh one to `Cluster.setCredential`.
   *
   * @param token The JSON Web Token.
   */
  static ofJwt(token: string): Credential {
    return new JwtCredential(token)
  }
}

/**
 * @internal
 */
class JwtCredential extends Credential {
  private readonly _authorizationHeader: string

  constructor(token: string) {
    if (typeof token !== 'string') {
      throw new InvalidArgumentError('JWT token must be a string.')
    }
    const trimmed = token.trim()
    if (trimmed.length === 0) {
      throw new InvalidArgumentError('JWT token must not be empty.')
    }
    // The token is interpolated directly into the Authorization header, so any
    // control character would let a caller inject additional headers.
    assertNoControlChars(trimmed, 'JWT token')

    super('', '')
    this._credentialType = CredentialType.Jwt
    this._authorizationHeader = `Bearer ${trimmed}`
  }

  /**
   * @internal
   */
  applyToRequest(opts: http.RequestOptions): void {
    const headers = (opts.headers ??= {}) as Record<string, string>
    headers.Authorization = this._authorizationHeader
  }
}
