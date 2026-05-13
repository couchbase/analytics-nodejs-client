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

import { InvalidArgumentError } from './errors.js'

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
  /** The username to authenticate with. */
  readonly username: string

  /** The password to authenticate with. */
  readonly password: string

  /** @internal */
  readonly type = 'password' as const

  /** @internal */
  readonly authorizationHeader: string

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
    this.username = username
    this.password = password
    this.authorizationHeader =
      'Basic ' +
      Buffer.from(`${username}:${password}`, 'utf8').toString('base64')
  }
}

/**
 * A JSON Web Token for authenticating to an Analytics cluster.
 *
 * @category Authentication
 */
export class JwtCredential {
  /** @internal */
  readonly type = 'jwt' as const

  /** @internal */
  readonly authorizationHeader: string

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
    this.authorizationHeader = `Bearer ${token}`
  }
}

/**
 * Credential variants accepted by an Analytics cluster.
 *
 * @category Authentication
 */
export type ClusterCredential = Credential | JwtCredential
