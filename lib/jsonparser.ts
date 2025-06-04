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

import { Transform, TransformCallback } from 'stream'

type TokenName =
  | 'startObject'
  | 'endObject'
  | 'startArray'
  | 'endArray'
  | 'keyValue'
  | 'stringValue'
  | 'numberValue'
  | 'nullValue'
  | 'trueValue'
  | 'falseValue'

interface ContextFrame {
  type: 'context'
  template: '{}' | '[]'
  items: string[]
  isResults?: boolean
  isErrors?: boolean
  isRow?: boolean
}

interface KeyFrame {
  type: 'key'
  value: string
}

export interface PrimitiveFrame {
  type: 'primitive'
  value: string
}

export type Frame = ContextFrame | KeyFrame | PrimitiveFrame

/**
 * Responsible for parsing JSON tokens, pushes complete rows to the stream,
 * and stores metadata in the stack.
 *
 * @internal
 */
export class JsonTokenParserStream extends Transform {
  private _stack: Frame[] = []
  private _inResults = false

  constructor() {
    super({ objectMode: true })
  }

  /**
   * @inheritDoc
   */
  _transform(
    token: any,
    _enc: BufferEncoding,
    callback: TransformCallback
  ): void {
    const { name, value } = token

    switch (name) {
      case 'startObject': {
        const parent = this._stack[this._stack.length - 1]
        if (
          parent &&
          parent.type === 'context' &&
          parent.template === '[]' &&
          (parent as ContextFrame).isResults
        ) {
          // This indicates the start of a row
          this._stack.push({
            type: 'context',
            template: '{}',
            items: [],
            isRow: true,
          })
        } else {
          // Non-row object (either metadata or another object nested inside a row)
          this._pushContext('{}')
        }
        break
      }
      case 'startArray': {
        const peek = this._peekKey()
        if (peek === 'results') {
          this._inResults = true
          this._stack.push({
            type: 'context',
            template: '[]',
            items: [],
            isResults: true,
          })
          break
        }
        if (peek === 'errors') {
          this._stack.push({
            type: 'context',
            template: '[]',
            items: [],
            isErrors: true,
          })
          break
        }
        this._pushContext('[]')
        break
      }

      case 'endArray':
        this._popContext('[]')
        break

      case 'endObject':
        this._popContext('{}')
        break

      case 'keyValue':
        if (typeof value === 'string') {
          this._stack.push({ type: 'key', value })
        }
        break

      case 'stringValue':
      case 'numberValue':
      case 'nullValue':
      case 'trueValue':
      case 'falseValue':
        this._pushValue(name, value)
        break
      default:
        break
    }

    callback()
  }

  /**
   * Pushes a new context frame onto the stack.
   *
   * @param template The template of the context.
   */
  private _pushContext(template: '{}' | '[]') {
    this._stack.push({ type: 'context', template, items: [] })
  }

  /**
   * Peeks at the top key in the stack without removing it.
   */
  private _peekKey(): string | null {
    const top = this._stack[this._stack.length - 1]
    return top && top.type === 'key' ? top.value : null
  }

  /**
   * Pushes a value onto the current context.
   *
   * @param name The name of the token.
   * @param val The value to push.
   */
  private _pushValue(name: TokenName, val: string | number | boolean | null) {
    let jsonVal: string
    if (name === 'stringValue') jsonVal = JSON.stringify(val as string)
    else if (name === 'nullValue') jsonVal = 'null'
    else if (name === 'trueValue') jsonVal = 'true'
    else if (name === 'falseValue') jsonVal = 'false'
    else jsonVal = String(val)

    const top = this._stack[this._stack.length - 1]
    if (
      top &&
      top.type === 'context' &&
      top.template === '[]' &&
      top.isResults
    ) {
      this.push(jsonVal)
      return
    }

    if (top && top.type === 'key') {
      const key = (this._stack.pop() as KeyFrame).value
      this._appendToCurrent(`${JSON.stringify(key)}:${jsonVal}`)
    } else {
      this._appendToCurrent(jsonVal)
    }
  }

  /**
   * Appends a piece of JSON to the current context or stack.
   *
   * @param piece The JSON piece to append.
   */
  private _appendToCurrent(piece: string) {
    for (let i = this._stack.length - 1; i >= 0; i--) {
      const frame = this._stack[i]
      if (frame.type === 'context') {
        frame.items.push(piece)
        return
      }
    }
    this._stack.push({ type: 'primitive', value: piece })
  }

  /**
   * Pops the current context.
   *
   * @param template The template of the context.
   */
  private _popContext(template: '{}' | '[]') {
    let ctx: Frame | undefined

    // Loop until we find the first context frame that matches the template
    for (;;) {
      ctx = this._stack.pop()
      if (!ctx) throw new Error('Malformed JSON token stream')
      if (ctx.type === 'context' && ctx.template === template) break
    }

    const { items, isResults, isErrors } = ctx as ContextFrame
    const json =
      template === '[]' ? `[${items.join(',')}]` : `{${items.join(',')}}`

    if (this._inResults && template === '{}' && !isResults && ctx.isRow) {
      this.push(json)
      return
    }

    if (this._inResults && template === '[]' && isResults) {
      this._inResults = false
      return
    }

    // Emit whole error array at the end
    if (template === '[]' && isErrors) {
      this.emit('errorsComplete', ctx.items)
      return
    }

    const parent = this._stack[this._stack.length - 1]
    if (parent && parent.type === 'key') {
      const key = (this._stack.pop() as KeyFrame).value
      this._appendToCurrent(`${JSON.stringify(key)}:${json}`)
    } else {
      this._appendToCurrent(json)
    }
  }

  get stack(): Frame[] {
    return this._stack
  }
}
