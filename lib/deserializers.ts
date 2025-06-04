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
 * Converts query result row into an object or value.
 */
export interface Deserializer {
  /**
   * Deserializes raw input into target object.
   *
   * @param encoded The raw input.
   */
  deserialize(encoded: string): any
}

/**
 * The default JsonDeserializer parses the raw input from query into a Javascript value or object.
 */
export class JsonDeserializer implements Deserializer {
  /**
   * Deserializes the raw input into a Javascript value or object.
   *
   * @param encoded The raw input.
   *
   * @throws {SyntaxError} The input must be valid JSON.
   */
  deserialize(encoded: string): any {
    return JSON.parse(encoded)
  }
}

/**
 * The PassthroughDeserializer returns the raw input as received from the server.
 */
export class PassthroughDeserializer implements Deserializer {
  /**
   * Returns the raw input received from the server.
   *
   * @param encoded The raw input.
   */
  deserialize(encoded: string): any {
    return encoded
  }
}
