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

import { assert } from 'chai'
import { ConnSpec } from '../lib/connspec.js'
import { harness } from './harness.js'
import { CouchbaseLogger } from '../lib/logger.js'
import { ParsingUtilities } from '../lib/utilities.js'

describe('#ConnSpec', function () {
  describe('stringify', function () {
    it('should return supported parameters from the connection string', function () {
      const url = new URL(
        'http://example.com?timeout.connect_timeout=5000ms&security.disable_server_certificate_verification=true'
      )
      const result = ConnSpec.getConnStringParams(url)

      assert.deepEqual(result, {
        'timeout.connect_timeout': '5000ms',
        'security.disable_server_certificate_verification': 'true',
      })
    })

    it('should log a warning on unsupported parameters', function () {
      const messages: string[] = []
      const logger = {
        warn(...args: string[]) {
          messages.push(...args)
        },
      }

      CouchbaseLogger.set(logger)
      const url = new URL('http://example.com?unsupported_param=123')
      ConnSpec.getConnStringParams(url)

      assert.equal(messages.length, 1)
      assert.equal(
        messages[0],
        'Unsupported parameter in connection string: unsupported_param'
      )
    })

    it('should parse duration values correctly', function () {
      const validDurations: [string, number][] = [
        ['0', 0],
        ['0s', 0],
        ['1h', 3.6e6],
        ['+1h', 3.6e6],
        ['1h10m', 4.2e6],
        ['1.h10m', 4.2e6],
        ['1.234h', 1.234 * 3.6e6],
        ['1h30m0s', 5.4e6],
        ['0.1h10m', 9.6e5],
        ['.1h10m', 9.6e5],
        ['0001h00010m', 4.2e6],
        ['100ns', 1e-4],
        ['100us', 0.1],
        ['100μs', 0.1],
        ['100µs', 0.1],
        ['1000000ns', 1],
        ['1000us', 1],
        ['1000μs', 1],
        ['1000µs', 1],
        ['3h15m10s500ms', 11710.5 * 1e3],
        ['1h1m1s1ms1us1ns', 3.6e6 + 60e3 + 1e3 + 1 + 0.001 + 0.000001],
        ['2m3s4ms', 123004],
        ['4ms3s2m', 123004],
        ['4ms3s2m5s', 128004],
        ['2m3.125s', 123125],
      ]

      for (const [input, expectedMillis] of validDurations) {
        const result = ParsingUtilities.parseGolangSyntaxDuration(input)
        // Assert that the result is within 1e-9ms of the expected value due to javascript's floating point precision
        assert.closeTo(result, expectedMillis, 1e-9)
      }
    })

    it('should fail parsing invalid duration values', async function () {
      const invalidDurations: string[] = [
        '',
        '10',
        '10Gs',
        'abc',
        '-',
        '+',
        '1h-',
        '1h 30m',
        '1h_30m',
        'h1',
        '-.5s',
        '1.2.3s',
      ]

      for (const duration of invalidDurations) {
        await harness.throwsHelper(() => {
          return Promise.resolve(ParsingUtilities.parseGolangSyntaxDuration(duration))
        }, Error)
      }
    })

    it('should parse boolean values correctly', function () {
      assert.isTrue(ConnSpec.parseBoolean('true'))
      assert.isTrue(ConnSpec.parseBoolean('1'))
      assert.isFalse(ConnSpec.parseBoolean('false'))
      assert.isFalse(ConnSpec.parseBoolean('0'))
    })
  })
})