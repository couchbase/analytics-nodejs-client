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

import fs from 'fs'
import path from 'path'
import { fileURLToPath } from 'node:url'

const __dirname = path.dirname(fileURLToPath(import.meta.url))

const CERT_DIRS = [
  { src: 'nonProdCertificates', dest: 'nonProdCertificates' },
  { src: 'capellaCertificates', dest: 'capellaCertificates' },
]

const ROOT_DIR = path.resolve(__dirname, '..')
const SRC_BASE = path.join(ROOT_DIR, 'lib')
const DIST_BASE = path.join(ROOT_DIR, 'dist')

for (const { src, dest } of CERT_DIRS) {
  console.log(
    `src: ${src}, dest: ${dest}, SRC_BASE: ${SRC_BASE}, DIST_BASE: ${DIST_BASE}`
  )
  const srcDir = path.join(SRC_BASE, src)
  const dstDir = path.join(DIST_BASE, dest)

  let files = fs.readdirSync(srcDir)

  if (files.length > 0 && !fs.existsSync(dstDir)) {
    fs.mkdirSync(dstDir, { recursive: true })
  }

  for (const fileName of files) {
    const srcPath = path.join(srcDir, fileName)
    const dstPath = path.join(dstDir, fileName)

    if (fs.statSync(srcPath).isFile()) {
      fs.copyFileSync(srcPath, dstPath)
    }
  }
}
