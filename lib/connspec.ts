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

/* eslint jsdoc/require-jsdoc: off */

import { CouchbaseLogger } from './logger'

const SUPPORTED_PARAMETERS = new Set([
  'timeout.connect_timeout',
  'timeout.query_timeout',
  'security.trust_only_pem_file',
  'security.disable_server_certificate_verification',
])

export class ConnSpec {
  static getConnStringParams(url: URL): Record<string, string> {
    const params: Record<string, string> = {}

    for (const [key, value] of url.searchParams.entries()) {
      if (SUPPORTED_PARAMETERS.has(key)) {
        params[key] = value
      } else {
        CouchbaseLogger.warn(
          `Unsupported parameter in connection string: ${key}`
        )
      }
    }
    return params
  }

  static parseBoolean(value: string): boolean | undefined {
    if (value === 'true' || value === '1') return true
    if (value === 'false' || value === '0') return false
    throw new Error(
      `Unsupported boolean value: "${value}". Acceptable values are "true", "false", "1", or "0".`
    )
  }
}
