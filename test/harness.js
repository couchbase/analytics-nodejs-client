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

'use strict'

const assert = require('chai').assert
const fs = require('fs')
const ini = require('ini')
const path = require('path')
const semver = require('semver')
const analytics = require('../lib/analytics')
const crypto = require('crypto')
const { Credential } = require('../lib/credential')

const TEST_CONFIG_INI = path.join(
  path.resolve(__dirname, '..'),
  'test',
  'testConfig.ini'
)

const ServerFeatures = {}

class ServerVersion {
  constructor(major, minor, patch) {
    this.major = major
    this.minor = minor
    this.patch = patch
  }

  isAtLeast(major, minor, patch) {
    if (this.major === 0 && this.minor === 0 && this.patch === 0) {
      // if no version is provided, assume latest
      return true
    }

    if (major < this.major) {
      return true
    } else if (major > this.major) {
      return false
    }

    if (minor < this.minor) {
      return true
    } else if (minor > this.minor) {
      return false
    }

    return patch <= this.patch
  }
}

var TEST_CONFIG = {
  connstr: undefined,
  version: new ServerVersion(0, 0, 0),
  database: 'Default',
  scope: 'Default',
  collection: 'Default',
  user: undefined,
  pass: undefined,
  nonprod: true,
  disableCertVerification: false,
  features: [],
}

let configIni
try {
  configIni = ini.parse(fs.readFileSync(TEST_CONFIG_INI, 'utf-8'))
} catch (e) {
  // config.ini is optional
}

if (configIni && configIni.connstr !== undefined) {
  TEST_CONFIG.connstr = configIni.connstr
} else if (process.env.NCBACCSTR !== undefined) {
  TEST_CONFIG.connstr = process.env.NCBACCSTR
}

if ((configIni && configIni.version) || process.env.NCBACCVER !== undefined) {
  assert(!!TEST_CONFIG.connstr, 'must not specify a version without a connstr')
  var ver = configIni.version || process.env.NCBACCVER
  var major = semver.major(ver)
  var minor = semver.minor(ver)
  var patch = semver.patch(ver)
  TEST_CONFIG.version = new ServerVersion(major, minor, patch)
}

let fqdnTokens = []
if (configIni && configIni.fqdn !== undefined) {
  fqdnTokens = configIni.fqdn.split('.')
} else if (process.env.NCBACFQDN !== undefined) {
  fqdnTokens = process.env.NCBACFQDN.split('.')
}

if (fqdnTokens.length > 0) {
  if (fqdnTokens.length != 3) {
    throw new Error(`Invalid FQDN provided. FQDN=${fqdnTokens.join('.')}`)
  }
  TEST_CONFIG.database = fqdnTokens[0]
  TEST_CONFIG.scope = fqdnTokens[1]
  TEST_CONFIG.collection = fqdnTokens[2]
}

if (configIni && configIni.username !== undefined) {
  TEST_CONFIG.user = configIni.username
} else if (process.env.NCBACUSER !== undefined) {
  TEST_CONFIG.user = process.env.NCBACUSER
}

if (configIni && configIni.password !== undefined) {
  TEST_CONFIG.pass = configIni.password
} else if (process.env.NCBACPASS !== undefined) {
  TEST_CONFIG.pass = process.env.NCBACPASS
}

if (configIni && configIni.nonprod !== undefined) {
  TEST_CONFIG.nonprod = configIni.nonprod
} else if (process.env.NCBACNONPROD !== undefined) {
  TEST_CONFIG.nonprod = process.env.NCBACNONPROD
}

if (configIni && configIni.disable_cert_verification !== undefined) {
  TEST_CONFIG.disableCertVerification = configIni.disable_cert_verification
} else if (process.env.NCBACDISABLECERTVERIFICATION !== undefined) {
  TEST_CONFIG.disableCertVerification = process.env.NCBACDISABLECERTVERIFICATION
}

if ((configIni && configIni.features) || process.env.NCBACFEAT !== undefined) {
  var featureStrs = (configIni.features || process.env.NCBACFEAT).split(',')
  featureStrs.forEach((featureStr) => {
    var featureName = featureStr.substr(1)

    var featureEnabled = undefined
    if (featureStr[0] === '+') {
      featureEnabled = true
    } else if (featureStr[0] === '-') {
      featureEnabled = false
    }

    TEST_CONFIG.features.push({
      feature: featureName,
      enabled: featureEnabled,
    })
  })
}

class Harness {
  get Features() {
    return ServerFeatures
  }

  constructor() {
    this._connstr = TEST_CONFIG.connstr
    this._version = TEST_CONFIG.version
    this._database = TEST_CONFIG.database
    this._scope = TEST_CONFIG.scope
    this._collection = TEST_CONFIG.collection
    this._user = TEST_CONFIG.user
    this._pass = TEST_CONFIG.pass
    this._nonprod = TEST_CONFIG.nonprod
    this._disableCertVerification = TEST_CONFIG.disableCertVerification
    this._integrationEnabled = true

    if (!this._connstr) {
      console.info(
        'Connection string is not set, integration tests will not be run'
      )
      this._integrationEnabled = false
    }

    this._testKey = crypto.randomUUID()
    this._testCtr = 1

    this._testCluster = null
    this._testDatabase = null
    this._testScope = null
  }

  get connStr() {
    return this._connstr
  }

  get databaseName() {
    return this._database
  }

  get scopeName() {
    return this._scope
  }

  get collectionName() {
    return this._collection
  }

  get fqdn() {
    return `\`${this._database}\`.\`${this._scope}\`.\`${this._collection}\``
  }

  get integrationEnabled() {
    return this._integrationEnabled
  }

  get credentials() {
    return new Credential(this._user, this._pass)
  }

  get nonprod() {
    return this._nonprod
  }

  get disableCertVerification() {
    return this._disableCertVerification
  }

  async throwsHelper(fn) {
    var assertArgs = Array.from(arguments).slice(1)

    var savedErr = null
    try {
      await fn()
    } catch (err) {
      savedErr = err
    }

    assert.throws(
      () => {
        if (savedErr) {
          throw savedErr
        }
      },
      ...assertArgs
    )
  }

  assertisFalse() {
    assert.isTrue(false)
  }

  genTestKey() {
    return this._testKey + '_' + this._testCtr++
  }

  async prepare() {
    var cluster = this.newCluster()
    var database = cluster.database(this._database)
    var scope = database.scope(this._scope)
    if (this._integrationEnabled) {
      await this.maybeCreateScope(scope)
    }

    this._testCluster = cluster
    this._testDatabase = database
    this._testScope = scope
  }

  async maybeCreateScope(scope) {
    try {
      await this.maybeCreateDatabase(scope.database)
      const qs = `CREATE SCOPE \`${scope.database.name}\`.\`${scope.name}\` IF NOT EXISTS`
      await scope.database.cluster.executeQuery(qs)
    } catch (e) {
      console.warn('Failed maybe creating scope/database: ' + e)
    }
  }

  async maybeCreateDatabase(database) {
    if (database.name !== 'Default') {
      const qs = `CREATE DATABASE \`${database.name}\` IF NOT EXISTS`
      await database.cluster.executeQuery(qs)
    }
  }

  newCluster(options) {
    if (!options) {
      options = {}
    }

    let username = options.username || this._user
    let password = options.password || this._pass

    const credential = new Credential(username, password)

    if (!options.connstr) {
      options.connstr = this._connstr
    }

    if (this.nonprod) {
      return analytics.Cluster.createInstance(options.connstr, credential, {
        securityOptions: {
          trustOnlyCertificates:
            analytics.Certificates.getNonprodCertificates(),
        },
      })
    } else if (this.disableCertVerification) {
      return analytics.Cluster.createInstance(options.connstr, credential, {
        securityOptions: {
          disableServerCertificateVerification: true,
        },
      })
    } else {
      return analytics.Cluster.createInstance(options.connstr, credential)
    }
  }

  async cleanup() {
    this._testDatabase = null
    this._testScope = null

    if (this._testCluster) {
      await this._testCluster.close()
      this._testCluster = null
    }
  }

  sleep(ms) {
    return new Promise((resolve) => setTimeout(resolve, ms))
  }

  supportsFeature(feature) {
    var featureEnabled = undefined

    TEST_CONFIG.features.forEach((cfgFeature) => {
      if (cfgFeature.feature === '*' || cfgFeature.feature === feature) {
        featureEnabled = cfgFeature.enabled
      }
    })

    if (featureEnabled === true) {
      return true
    } else if (featureEnabled === false) {
      return false
    }

    // eslint-disable-next-line no-empty
    switch (feature) {
    }

    throw new Error('invalid code for feature checking')
  }

  skipIfMissingFeature(test, feature) {
    if (!this.supportsFeature(feature)) {
      /* eslint-disable-next-line mocha/no-skipped-tests */
      test.skip()
      throw new Error('test skipped')
    }
  }

  skipIfIntegrationDisabled(test) {
    if (!this._integrationEnabled) {
      // eslint-disable-next-line mocha/no-skipped-tests
      test.skip()
      throw new Error('test skipped as integration tests are disabled')
    }
  }

  get lib() {
    return analytics
  }

  get c() {
    return this._testCluster
  }
  get d() {
    return this._testDatabase
  }
  get s() {
    return this._testScope
  }
}

var harness = new Harness()

// These are written as normal functions, not async lambdas
// due to our need to specify custom timeouts, which are not
// yet supported on before/after methods yet.
/* eslint-disable-next-line mocha/no-top-level-hooks */
before(function (done) {
  this.timeout(30000)
  harness.prepare().then(done).catch(done)
})
/* eslint-disable-next-line mocha/no-top-level-hooks */
after(function (done) {
  this.timeout(10000)
  harness.cleanup().then(done).catch(done)
})

/* eslint-disable-next-line mocha/no-exports */
module.exports = harness
