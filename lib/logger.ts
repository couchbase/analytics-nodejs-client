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
 * @internal
 */
export type LogLevel = 'debug' | 'info' | 'warn' | 'error'

/**
 * @internal
 */
export const LOG_LEVELS: LogLevel[] = ['error', 'warn', 'info', 'debug']

/**
 * Logger interface.
 */
export interface Logger {
  /**
   * Logs at the debug level.
   */
  debug?(...args: any[]): void
  /**
   * Logs at the info level.
   */
  info?(...args: any[]): void
  /**
   * Logs at the warn level.
   */
  warn?(...args: any[]): void
  /**
   * Logs at the error level.
   */
  error?(...args: any[]): void
}

/**
 * Default No-op logger.
 */
export const NOOP_LOGGER: Logger = {
  info: () => {},
  warn: () => {},
  error: () => {},
  debug: () => {},
}

/**
 * @internal
 */
export function createConsoleLogger(minLevel: LogLevel = 'info'): Logger {
  const minPriority = LOG_LEVELS.indexOf(minLevel)

  const makeLevel = (level: LogLevel, fn: (...args: any[]) => void) => {
    return LOG_LEVELS.indexOf(level) <= minPriority
      ? fn.bind(console)
      : () => {}
  }

  return {
    error: makeLevel('error', console.error),
    warn: makeLevel('warn', console.warn),
    info: makeLevel('info', console.info),
    debug: makeLevel('debug', console.debug),
  }
}

/**
 * Singleton that provides access to a logger instance.
 *
 * @internal
 */
export class CouchbaseLogger {
  private static instance: Logger

  private constructor() {}

  /**
   * Fetches the current logger instance.
   *
   * @internal
   */
  public static get(): Logger {
    return CouchbaseLogger.instance
  }

  /**
   * Sets the logger instance to be used.
   *
   * Users should not call this method directly, and instead set the logger via the {@link ClusterOptions.logger}
   * @internal
   */
  public static set(logger: Logger): void {
    CouchbaseLogger.instance = logger
  }

  /**
   * Internal helper for logging at the info level.
   *
   * @internal
   */
  public static info(...args: any[]): void {
    CouchbaseLogger.get().info?.(...args)
  }

  /**
   * Internal helper for logging at the warn level.
   *
   * @internal
   */
  public static warn(...args: any[]): void {
    CouchbaseLogger.get().warn?.(...args)
  }

  /**
   * Internal helper for logging at the error level.
   *
   * @internal
   */
  public static error(...args: any[]): void {
    CouchbaseLogger.get().error?.(...args)
  }

  /**
   * Internal helper for logging at the debug level.
   *
   * @internal
   */
  public static debug(...args: any[]): void {
    CouchbaseLogger.get().debug?.(...args)
  }
}
