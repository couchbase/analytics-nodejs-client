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

import { Credential } from './credential'
import { Database } from './database'
import { Deserializer, JsonDeserializer } from './deserializers'
import { QueryOptions, QueryResult } from './querytypes'
import { QueryExecutor } from './queryexecutor'

import { HttpClient } from './httpclient'
import { InvalidArgumentError } from './errors'
import { ConnSpec } from './connspec'
import {
  CouchbaseLogger,
  LOG_LEVELS,
  Logger,
  NOOP_LOGGER,
  LogLevel,
  createConsoleLogger,
} from './logger'
import { ParsingUtilities } from './utilities'

/**
 * Specifies the timeout options for the client.
 *
 * @category Core
 */
export interface TimeoutOptions {
  /**
   * Specifies the default timeout allocated to complete bootstrap connection, specified in millseconds.
   */
  connectTimeout?: number

  /**
   * Specifies the default timeout for query operations, specified in millseconds.
   */
  queryTimeout?: number
}

/**
 * Specifies security options for the client.
 *
 * @category Core
 */
export interface SecurityOptions {
  /**
   * Specifies the SDK will only trust the Capella CA certificate(s).
   */
  trustOnlyCapella?: boolean

  /**
   * Specifies the SDK will only trust the PEM-encoded certificate(s)
   * at the specified file path.
   */
  trustOnlyPemFile?: string

  /**
   * Specifies the SDK will only trust the PEM-encoded certificate(s)
   * in the specified string.
   */
  trustOnlyPemString?: string

  /**
   * Specifies the SDK will only trust the PEM-encoded certificate(s)
   * specified.
   */
  trustOnlyCertificates?: string[]

  /**
   * If disabled, SDK will trust any certificate regardless of validity.
   * Should not be disabled in production environments.
   */
  disableServerCertificateVerification?: boolean
}

/**
 * Specifies the options which can be specified when connecting
 * to a cluster.
 *
 * @category Core
 */
export interface ClusterOptions {
  /**
   * Specifies the security options for connections of this cluster.
   */
  securityOptions?: SecurityOptions

  /**
   * Specifies the default timeouts for various operations performed by the SDK.
   */
  timeoutOptions?: TimeoutOptions

  /**
   * Sets the default deserializer for converting query result rows into objects.
   * If not specified, the SDK uses an instance of the default {@link JsonDeserializer}.
   *
   * Can also be set per-operation with {@link QueryOptions.deserializer}.
   */
  deserializer?: Deserializer

  /**
   * Provides an implementation of the {@link Logger} interface to be used by the SDK.
   */
  logger?: Logger
}

/**
 * Exposes the operations which are available to be performed against a cluster.
 * Namely, the ability to access to Databases as well as performing management
 * operations against the cluster.
 *
 * @category Core
 */
export class Cluster {
  private _queryTimeout: number
  private _connectTimeout: number
  private _httpClient: HttpClient
  private _credential: Credential
  private _deserializer: Deserializer

  /**
     @internal
     */
  get queryTimeout(): number {
    return this._queryTimeout
  }

  /**
     @internal
     */
  get connectTimeout(): number {
    return this._connectTimeout
  }

  /**
     @internal
     */
  get deserializer(): Deserializer {
    return this._deserializer
  }

  /**
   * @internal
   */
  get httpClient(): HttpClient {
    return this._httpClient
  }

  /**
     @internal
     */
  private constructor(
    httpEndpoint: string,
    credential: Credential,
    options?: ClusterOptions
  ) {
    if (!options) {
      options = {}
    }

    if (!options.logger) {
      const envLogLevel = (
        process.env.NCBACLOGLEVEL || ''
      ).toLowerCase() as LogLevel

      options.logger = LOG_LEVELS.includes(envLogLevel)
        ? createConsoleLogger(envLogLevel)
        : NOOP_LOGGER
    }
    CouchbaseLogger.set(options.logger)

    const url = new URL(httpEndpoint)
    const connStrParams = ConnSpec.getConnStringParams(url)

    if (!options.timeoutOptions) {
      options.timeoutOptions = {}
    }

    if (connStrParams['timeout.connect_timeout']) {
      options.timeoutOptions.connectTimeout =
        ParsingUtilities.parseGolangSyntaxDuration(
          connStrParams['timeout.connect_timeout']
        )
    }

    if (connStrParams['timeout.query_timeout']) {
      options.timeoutOptions.queryTimeout =
        ParsingUtilities.parseGolangSyntaxDuration(
          connStrParams['timeout.query_timeout']
        )
    }

    this._validateTimeoutOptions(options.timeoutOptions)

    if (!options.securityOptions) {
      options.securityOptions = {}
    }

    if (connStrParams['security.trust_only_pem_file']) {
      options.securityOptions.trustOnlyPemFile =
        connStrParams['security.trust_only_pem_file']
    }

    if (connStrParams['security.disable_server_certificate_verification']) {
      options.securityOptions.disableServerCertificateVerification =
        ConnSpec.parseBoolean(
          connStrParams['security.disable_server_certificate_verification']
        )
    }

    this._validateSecurityOptions(options.securityOptions)

    this._credential = credential
    this._queryTimeout = options.timeoutOptions.queryTimeout || 600_000
    this._connectTimeout = options.timeoutOptions.connectTimeout || 10_000
    this._deserializer = options.deserializer || new JsonDeserializer()
    this._httpClient = new HttpClient(
      url,
      this._credential,
      options.securityOptions
    )
  }

  /**
   * Entry point for creating a new cluster object.
   *
   * @param httpEndpoint The HTTP endpoint of the cluster.
   * @param credential The credential to use for authenticating with the cluster.
   * @param options Options to configure the cluster connection and global settings.
   */
  static createInstance(
    httpEndpoint: string,
    credential: Credential,
    options?: ClusterOptions
  ): Cluster {
    return new Cluster(httpEndpoint, credential, options)
  }

  /**
   * Volatile: This API is subject to change at any time.
   *
   * Creates a database object reference to a specific database.
   *
   * @param databaseName The name of the database to reference.
   */
  database(databaseName: string): Database {
    return new Database(this, databaseName)
  }

  /**
   * Executes a query against the Columnar cluster.
   *
   * @param statement The columnar SQL++ statement to execute.
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
      throw new InvalidArgumentError('timeout must be non-negative.')
    }

    const exec = new QueryExecutor(
      this,
      options.deserializer || this._deserializer,
      options.abortSignal
    )
    return exec.query(statement, options)
  }

  /**
   * Shuts down this cluster object.  Cleaning up all resources associated with it.
   *
   */
  close(): void {
    this._httpClient.close()
  }

  /**
   * @internal
   */
  private _validateTimeoutOptions(timeoutOptions: TimeoutOptions): void {
    if (timeoutOptions.connectTimeout && timeoutOptions.connectTimeout < 0) {
      throw new Error('connectTimeout must be non-negative.')
    }

    if (timeoutOptions.queryTimeout && timeoutOptions.queryTimeout < 0) {
      throw new Error('queryTimeout must be non-negative')
    }
  }

  private _validateSecurityOptions(securityOptions: SecurityOptions): void {
    const trustOptionsCount =
      (securityOptions.trustOnlyCapella ? 1 : 0) +
      (securityOptions.trustOnlyPemFile ? 1 : 0) +
      (securityOptions.trustOnlyPemString ? 1 : 0) +
      (securityOptions.trustOnlyCertificates ? 1 : 0)

    if (trustOptionsCount > 1) {
      throw new InvalidArgumentError(
        'Only one of trustOnlyCapella, trustOnlyPemFile, trustOnlyPemString, or trustOnlyCertificates can be set.'
      )
    }
  }
}
