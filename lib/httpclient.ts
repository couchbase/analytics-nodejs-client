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

import { Agent as HttpAgent } from 'node:http'
import { Agent as HttpsAgent } from 'node:https'
import { AnalyticsError } from './errors'
import { Credential } from './credential'
import { SecurityOptions } from './cluster'
import * as tls from 'node:tls'
import { Certificates } from './certificates'
import fs from 'fs'
import http from 'node:http'
import https from 'node:https'

/**
 * @internal
 */
export class HttpClient {
  private _agent: HttpAgent | HttpsAgent
  private _module: typeof http | typeof https
  private _auth: string
  private _hostname: string
  private _port: string

  constructor(
    url: URL,
    credential: Credential,
    securityOptions: SecurityOptions
  ) {
    this._hostname = url.hostname
    this._auth = `${credential.username}:${credential.password}`

    if (url.protocol === 'http:') {
      this._port = url.port ?? '80'
      this._agent = new HttpAgent({
        keepAlive: true,
      })
      this._module = http
    } else if (url.protocol === 'https:') {
      this._port = url.port ?? '443'
      const tlsOptions = this._buildTlsOptions(securityOptions)
      this._agent = new HttpsAgent({
        keepAlive: true,
        ...tlsOptions,
      })
      this._module = https
    } else {
      throw new AnalyticsError(
        'Unsupported protocol provided in connection string'
      )
    }
  }

  /**
   * @internal
   */
  get module(): typeof http | typeof https {
    return this._module
  }

  /**
   * @internal
   */
  genericRequestOptions(): http.RequestOptions {
    return {
      agent: this._agent,
      port: this._port,
      auth: this._auth,
    }
  }

  /**
   * @internal
   */
  close(): void {
    if (this._agent) {
      this._agent.destroy()
    }
  }

  private _buildTlsOptions(
    securityOptions: SecurityOptions
  ): tls.ConnectionOptions {
    const tlsOptions: tls.ConnectionOptions = {}

    // Override the servername to use the hostname rather than the DNS record
    tlsOptions.servername = this._hostname

    tlsOptions.minVersion = 'TLSv1.3'

    if (Object.keys(securityOptions).length === 0) {
      // By default, we trust the platform root certificates and the capella certs
      tlsOptions.ca = [
        ...tls.rootCertificates,
        ...Certificates.getCapellaCertificates(),
      ]
      return tlsOptions
    }

    if (securityOptions.trustOnlyCapella) {
      tlsOptions.ca = Certificates.getCapellaCertificates()
    }

    if (securityOptions.trustOnlyPemFile) {
      tlsOptions.ca = fs.readFileSync(securityOptions.trustOnlyPemFile)
    }

    if (securityOptions.trustOnlyPemString) {
      tlsOptions.ca = securityOptions.trustOnlyPemString
    }

    if (securityOptions.trustOnlyCertificates) {
      tlsOptions.ca = securityOptions.trustOnlyCertificates
    }

    if (securityOptions.disableServerCertificateVerification !== undefined) {
      tlsOptions.rejectUnauthorized =
        !securityOptions.disableServerCertificateVerification
    }

    return tlsOptions
  }

  /**
   * @internal
   */
  get hostname(): string {
    return this._hostname
  }
}
