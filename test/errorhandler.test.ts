/*
 *  Copyright 2016-2026. Couchbase, Inc.
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
import { ErrorHandler } from '../lib/errorhandler.js'
import { ConnectionError } from '../lib/internalerrors.js'
import { RequestContext } from '../lib/requestcontext.js'

// Build a ConnectionError wrapping a Node system error carrying `code`,
// mirroring what the executor produces from a request `error` event.
function connectionError(code: string | undefined, isRequestError = true): ConnectionError {
  const cause = new Error(`synthetic ${code ?? 'no-code'} error`) as NodeJS.ErrnoException
  if (code !== undefined) {
    cause.code = code
  }
  return new ConnectionError(cause, isRequestError)
}

function classify(err: ConnectionError): boolean {
  // handleErrors routes through the private _isRetriableConnectionError.
  return ErrorHandler.handleErrors(err, new RequestContext(7)).shouldRetry()
}

// Per the RFC, only DNS failures and TCP-dial connection failures are eligible
// for retry (allowlist). Anything that happens after the socket is dialed -- the
// TLS handshake, cert/hostname verification, response read/write -- must fail
// fast.
describe('#ConnectionError retry classification', function () {
  it('does not retry a TLS hostname/altname mismatch', function () {
    assert.isFalse(classify(connectionError('ERR_TLS_CERT_ALTNAME_INVALID')))
  })

  it('does not retry other ERR_TLS_* handshake errors', function () {
    assert.isFalse(classify(connectionError('ERR_TLS_HANDSHAKE_TIMEOUT')))
  })

  it('does not retry ERR_SSL_* errors', function () {
    assert.isFalse(classify(connectionError('ERR_SSL_WRONG_VERSION_NUMBER')))
  })

  it('does not retry an OpenSSL-style cert verification code', function () {
    // e.g. an expired/self-signed cert -- a handshake failure, not a dial failure.
    assert.isFalse(classify(connectionError('CERT_HAS_EXPIRED')))
    assert.isFalse(classify(connectionError('DEPTH_ZERO_SELF_SIGNED_CERT')))
  })

  it('retries TCP-dial connection failures', function () {
    assert.isTrue(classify(connectionError('ECONNREFUSED')))
    assert.isTrue(classify(connectionError('ECONNRESET')))
    assert.isTrue(classify(connectionError('ETIMEDOUT')))
    // Network-unreachable is a connection failure -- the old denylist wrongly
    // blocked it; the allowlist restores RFC-correct retry behavior.
    assert.isTrue(classify(connectionError('ENETUNREACH')))
  })

  it('retries DNS-resolution failures', function () {
    assert.isTrue(classify(connectionError('EAI_AGAIN')))
    assert.isTrue(classify(connectionError('ENOTFOUND')))
  })

  it('does not retry an unrecognized read/write error code', function () {
    // Not a DNS or TCP-dial failure -> not on the allowlist -> fail fast.
    assert.isFalse(classify(connectionError('EPROTO')))
    assert.isFalse(classify(connectionError('ERR_STREAM_PREMATURE_CLOSE')))
  })

  it('does not retry a response (non-request) error', function () {
    // Errors raised while reading the response are read/write-after-dial.
    assert.isFalse(classify(connectionError('ECONNREFUSED', false)))
  })

  it('does not retry a connection error with no code', function () {
    assert.isFalse(classify(connectionError(undefined)))
  })
})
