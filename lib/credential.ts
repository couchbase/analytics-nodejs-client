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
 * ICredential specifies a credential which uses an RBAC
 * username and password to authenticate with the cluster.
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
 * Credential implements a simple ICredential.
 *
 * @category Authentication
 */
export class Credential implements ICredential {
  /**
   * The username that will be used to authenticate with.
   */
  username: string

  /**
   * The password that will be used to authenticate with.
   */
  password: string

  /**
   * Constructs this Credential with the passed username and password.
   *
   * @param username The username to initialize this credential with.
   * @param password The password to initialize this credential with.
   */
  constructor(username: string, password: string) {
    this.username = username
    this.password = password
  }
}
