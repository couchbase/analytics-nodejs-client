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

import { Database } from './database.js'
import { Cluster } from './cluster.js'
import { QueryOptions, QueryResult } from './querytypes.js'
import { QueryExecutor } from './queryexecutor.js'

/**
 * Volatile: This API is subject to change at any time.
 *
 * Exposes the operations which are available to be performed against a scope.
 * Namely, the ability to access to Collections for performing operations.
 *
 * @category Core
 */
export class Scope {
  private _database: Database
  private _name: string

  /**
     @internal
     */
  constructor(database: Database, scopeName: string) {
    this._database = database
    this._name = scopeName
  }

  /**
     @internal
     */
  get database(): Database {
    return this._database
  }

  /**
     @internal
     */
  get cluster(): Cluster {
    return this._database.cluster
  }

  /**
   * Executes a query against the Analytics scope.
   *
   * @param statement The Analytics SQL++ statement to execute.
   * @param options Optional parameters for this operation.
   */
  executeQuery(
    statement: string,
    options?: QueryOptions
  ): Promise<QueryResult> {
    if (!options) {
      options = {}
    }

    if (options.timeout && options.timeout < 0) {
      throw new Error('timeout must be non-negative.')
    }

    const exec = new QueryExecutor(
      this.cluster,
      options.deserializer || this.cluster.deserializer,
      options.maxRetries || this.cluster.maxRetries,
      options.abortSignal,
      this._database.name,
      this._name
    )
    return exec.query(statement, options)
  }

  /**
   * The name of the scope this Scope object references.
   */
  get name(): string {
    return this._name
  }
}
