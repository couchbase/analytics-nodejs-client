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

import { Cluster } from './cluster.js'
import { Scope } from './scope.js'

/**
 * Volatile: This API is subject to change at any time.
 *
 * Exposes the operations which are available to be performed against a database.
 * Namely, the ability to access to Scopes as well as performing management
 * operations against the database.
 *
 * @category Core
 */
export class Database {
  private _cluster: Cluster
  private _name: string

  /**
     @internal
     */
  constructor(cluster: Cluster, databaseName: string) {
    this._cluster = cluster
    this._name = databaseName
  }

  /**
     @internal
     */
  get cluster(): Cluster {
    return this._cluster
  }

  /**
   * The name of the database this Database object references.
   */
  get name(): string {
    return this._name
  }

  /**
   * Creates a Scope object reference to a specific scope.
   *
   * @param scopeName The name of the scope to reference.
   */
  scope(scopeName: string): Scope {
    return new Scope(this, scopeName)
  }
}
