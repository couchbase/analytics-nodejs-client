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

/**
 * @internal
 */
function validateUsername(username: string): void {
  if (typeof username !== 'string') {
    throw new InvalidArgumentError('Username must be a string.')
  }
}

/**
 * @internal
 */
function validatePassword(password: string): void {
  if (typeof password !== 'string') {
    throw new InvalidArgumentError('Password must be a string.')
  }
}

/**
 * @internal
 */
function buildBasicAuthorizationHeader(
  username: string,
  password: string
): string {
  return (
    'Basic ' +
    Buffer.from(`${username}:${password}`, 'utf8').toString('base64')
  )
}

/**
 * Discriminates between password and JWT credentials.
 *
 * @internal
 */
enum CredentialType {
  /** Username and password authentication, sent as HTTP Basic. */
  Password = 'password',
  /** JSON Web Token authentication, sent as HTTP Bearer. */
  Jwt = 'jwt',
}

const passwordCredentialState = new WeakMap<
  Credential,
  { authorizationHeader: string }
>()
const jwtCredentialState = new WeakMap<
  JwtCredential,
  { authorizationHeader: string }
>()

/**
 * ICredential specifies a credential which uses an RBAC
 * username and password to authenticate with the cluster.
 *
 * @deprecated Retained for back-compat with code that imported the type
 *   from earlier versions. Use the {@link Credential} class directly. Not
 *   accepted as input to `createInstance` or `Cluster.setCredential` —
 *   both require a {@link Credential} or {@link JwtCredential} instance.
 *
 * @category Authentication
 */
export interface ICredential {
  /**
   * The username to authenticate with.
   */
  username: string

  /**
   * The password to authenticate with.
   */
  password: string
}

/**
 * RBAC username/password for authenticating to an Analytics cluster. For
 * a JSON Web Token instead, see {@link JwtCredential}.
 *
 * @category Authentication
 */
export class Credential implements ICredential {
  private _username: string
  private _password: string

  /**
   * Constructs a {@link Credential} for an RBAC username and password. The
   * SDK sends `Authorization: Basic <base64(user:pass)>` on every request.
   *
   * @param username The username to authenticate with.
   * @param password The password to authenticate with.
   */
  constructor(username: string, password: string) {
    validateUsername(username)
    validatePassword(password)
    this._username = username
    this._password = password
    refreshPasswordCredentialState(this)
  }

  /** The username to authenticate with. */
  get username(): string {
    return this._username
  }

  set username(username: string) {
    validateUsername(username)
    this._username = username
    refreshPasswordCredentialState(this)
  }

  /** The password to authenticate with. */
  get password(): string {
    return this._password
  }

  set password(password: string) {
    validatePassword(password)
    this._password = password
    refreshPasswordCredentialState(this)
  }
}

/**
 * A JSON Web Token for authenticating to an Analytics cluster.
 *
 * @category Authentication
 */
export class JwtCredential {
  private readonly _jwtCredentialBrand: undefined

  /**
   * Constructs a {@link JwtCredential}. The SDK sends
   * `Authorization: Bearer <token>` on every request.
   *
   * @param token The JSON Web Token.
   */
  constructor(token: string) {
    if (typeof token !== 'string') {
      throw new InvalidArgumentError('JWT token must be a string.')
    }
    if (token.length === 0) {
      throw new InvalidArgumentError('JWT token must not be empty.')
    }
    jwtCredentialState.set(this, { authorizationHeader: `Bearer ${token}` })
  }
}

/**
 * Credential variants accepted by an Analytics cluster.
 *
 * @category Authentication
 */
export type ClusterCredential = Credential | JwtCredential

/**
 * @internal
 */
function refreshPasswordCredentialState(credential: Credential): void {
  passwordCredentialState.set(credential, {
    authorizationHeader: buildBasicAuthorizationHeader(
      credential.username,
      credential.password
    ),
  })
}

/**
 * @internal
 */
export function assertClusterCredential(
  credential: unknown
): asserts credential is ClusterCredential {
  if (credential == null) {
    throw new InvalidArgumentError('credential must not be null/undefined.')
  }
  if (!(credential instanceof Credential || credential instanceof JwtCredential)) {
    throw new InvalidArgumentError(
      'credential must be a Credential or JwtCredential.'
    )
  }
}

/**
 * @internal
 */
export function getCredentialType(credential: ClusterCredential): CredentialType {
  assertClusterCredential(credential)
  if (credential instanceof Credential) {
    return CredentialType.Password
  }
  return CredentialType.Jwt
}

/**
 * @internal
 */
export function applyCredentialToRequest(
  credential: ClusterCredential,
  opts: http.RequestOptions
): void {
  assertClusterCredential(credential)
  const headers = (opts.headers ??= {}) as Record<string, string>
  if (credential instanceof Credential) {
    const state = passwordCredentialState.get(credential)
    if (!state) {
      throw new InvalidArgumentError('credential must be a Credential.')
    }
    headers.Authorization = state.authorizationHeader
    return
  }

  const state = jwtCredentialState.get(credential)
  if (!state) {
    throw new InvalidArgumentError('credential must be a JwtCredential.')
  }
  headers.Authorization = state.authorizationHeader
}
