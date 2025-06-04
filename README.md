# Couchbase Node.js Analytics Client
Node.js client for [Couchbase](https://couchbase.com) Analytics

# Installing the SDK<a id="installing-the-sdk"></a>

Install the SDK via `npm`:
```console
npm install couchbase-analytics
```

# Using the SDK<a id="using-the-sdk"></a>

Some more examples are provided in the [examples directory](https://github.com/couchbaselabs/analytics-nodejs-client/tree/main/examples).

## CommonJS
**Connecting and executing a query**
```javascript
const analytics = require('couchbase-analytics')

async function main() {
  // Update this to your cluster
  const clusterEndpoint = 'https://--your-instance--'
  const username = 'username'
  const password = 'password'
  // User Input ends here.

  const credential = new analytics.Credential(username, password)
  const cluster = analytics.createInstance(clusterConnStr, credential)

  // Execute a streaming query with positional arguments.
  let qs = 'SELECT * FROM `travel-sample`.inventory.airline LIMIT 10;'
  let res = await cluster.executeQuery(qs)
  for await (let row of res.rows()) {
    console.log('Found row: ', row)
  }
  console.log('Metadata: ', res.metadata())

  // Execute a streaming query with positional arguments.
  qs =
    'SELECT * FROM `travel-sample`.inventory.airline WHERE country=$1 LIMIT $2;'
  res = await cluster.executeQuery(qs, { parameters: ['United States', 10] })
  for await (let row of res.rows()) {
    console.log('Found row: ', row)
  }
  console.log('Metadata: ', res.metadata())

  // Execute a streaming query with named parameters.
  qs =
    'SELECT * FROM `travel-sample`.inventory.airline WHERE country=$country LIMIT $limit;'
  res = await cluster.executeQuery(qs, {
    namedParameters: { country: 'United States', limit: 10 },
  })
  for await (let row of res.rows()) {
    console.log('Found row: ', row)
  }
  console.log('Metadata: ', res.metadata())
}

main()
  .then(() => {
    console.log('Finished.  Exiting app...')
  })
  .catch((err) => {
    console.log('ERR: ', err)
    console.log('Exiting app...')
    process.exit(1)
  })

```

## ES Modules
**Connecting and executing a query**
```javascript
import { Certificates, Credential, createInstance } from "couchbase-analytics"

async function main() {
  // Update this to your cluster
  const clusterConnStr = 'https://--your-instance--'
  const username = 'username'
  const password = 'password'
  // User Input ends here.

  const credential = new Credential(username, password)
  const cluster = createInstance(clusterConnStr, credential)

  // Execute a streaming query with positional arguments.
  let qs = "SELECT * FROM `travel-sample`.inventory.airline LIMIT 10;"
  let res = await cluster.executeQuery(qs)
  for await (let row of res.rows()) {
    console.log("Found row: ", row)
  }
  console.log("Metadata: ", res.metadata())

  // Execute a streaming query with positional arguments.
  qs =
    "SELECT * FROM `travel-sample`.inventory.airline WHERE country=$1 LIMIT $2;"
  res = await cluster.executeQuery(qs, { parameters: ["United States", 10] })
  for await (let row of res.rows()) {
    console.log("Found row: ", row)
  }
  console.log("Metadata: ", res.metadata())

  // Execute a streaming query with named parameters.
  qs =
    "SELECT * FROM `travel-sample`.inventory.airline WHERE country=$country LIMIT $limit;"
  res = await cluster.executeQuery(qs, {
    namedParameters: { country: "United States", limit: 10 },
  })
  for await (let row of res.rows()) {
    console.log("Found row: ", row)
  }
  console.log("Metadata: ", res.metadata())
}

main()
  .then(() => {
    console.log("Finished.  Exiting app...")
  })
  .catch((err) => {
    console.log("ERR: ", err)
    console.log("Exiting app...")
    process.exit(1)
  })

```