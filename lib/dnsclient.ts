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

import dns from 'node:dns'
import { CouchbaseLogger } from './logger'
import { ConnectionError, DnsRecordsExhaustedError } from './internalerrors'

/**
 * @internal
 */
export class DnsClient {
  private hostname: string
  private availableRecords: Map<string, boolean>

  constructor(hostname: string) {
    this.hostname = hostname
    this.availableRecords = new Map()
  }

  /**
   * @internal
   */
  async updateDnsRecords(): Promise<void> {
    let addresses: dns.LookupAddress[]
    try {
      addresses = await new Promise<dns.LookupAddress[]>((resolve, reject) => {
        dns.lookup(this.hostname, { all: true }, (err, result) => {
          if (err) {
            reject(err)
          } else {
            resolve(result)
          }
        })
      })
    } catch (err: any) {
      CouchbaseLogger.error(
        `DNS lookup failed for ${this.hostname}: ${err?.message || 'Unknown error'}`
      )
      throw new ConnectionError(err, true)
    }

    if (addresses.length === 0) {
      throw new ConnectionError(
        new Error(`No DNS records found for ${this.hostname}`),
        true
      )
    }

    for (const address of addresses) {
      if (!this.availableRecords.has(address.address)) {
        this.availableRecords.set(address.address, false)
      }
    }
  }

  /**
   * @internal
   */
  async maybeUpdateDnsRecords(): Promise<void> {
    if (this.availableRecords.size > 0) {
      return
    }
    await this.updateDnsRecords()
  }

  /**
   * @internal
   */
  getRandomRecord(): string {
    const availableRecords = this.getAvailableRecords()

    if (availableRecords.length === 0) {
      throw new DnsRecordsExhaustedError('No available DNS records found')
    }

    const randomIndex = Math.floor(Math.random() * availableRecords.length)
    return availableRecords[randomIndex]
  }

  /**
   * @internal
   */
  getAvailableRecords(): string[] {
    return Array.from(this.availableRecords.entries())
      .filter(([, used]) => !used)
      .map(([address]) => address)
  }

  /**
   * @internal
   */
  async maybeUpdateAndGetRandomRecord(): Promise<string> {
    await this.maybeUpdateDnsRecords()
    return this.getRandomRecord()
  }

  /**
   * @internal
   */
  markRecordAsUsed(record: string): void {
    if (this.availableRecords.has(record)) {
      this.availableRecords.set(record, true)
    } else {
      CouchbaseLogger.warn(
        `Attempted to mark non-existent record as used: ${record}`
      )
    }
  }
}
