#!/usr/bin/env node
'use strict'

const {Transform, Writable} = require('stream')
const level = require('level')
const now = require('nano-time')
const Promise = require('bluebird')
const store = level('./store', {valueEncoding: 'json'})

const subscribe   = (dp, dr) => oig => ({type: 'subscribe', dp, dr, oig})
const unsubscribe = (dp, dr) => oig => ({type: 'unsubscribe', dp, dr, oig})

const reducers = {
  subscriptions: (p, {type, dp, dr, oig}) => {
    const cid = `${dp}:${dr}`
    const connection = p[cid] || (p[cid] = [])
    const put = a => x => a.indexOf(x) === -1 && a.push(x)
    const subscribe = () => put(connection)(oig)
    const unsubscribe = () => {
      p[cid] = connection.filter(_ => _ != oig)
      if(p[cid].length === 0) p[cid] = undefined
    }

    const ops = { subscribe, unsubscribe }
    ops[type] && ops[type]()
    return p
  }
}

const populate = async () => {
  store.on('batch', entries => {
    const values = entries.map(({value, ...event}) => value ? value : event)
    Object.entries(reducers).forEach(([key, reducer]) => {
      store.get(`p:${key}`, (err, p) => {
        p = values.reduce(reducer, p || {})
        store.put(`p:${key}`, p)
      })
    })
  })

  const putAction = (batch, action) => batch.put(now(), action)

  await [
    subscribe   ('120000001', '120000002')('12000000100000000000'),
    subscribe   ('120000001', '120000002')('12000000100000000001'),
    subscribe   ('120000001', '120000002')('12000000100000000002'),
    subscribe   ('120000005', '120000002')('12000000500000000000'),
    subscribe   ('120000005', '120000002')('12000000500000000001')
  ].reduce(putAction, store.batch()).write()

  await [
    unsubscribe ('120000001', '120000002')('12000000100000000001'),
    subscribe   ('120000001', '120000002')('12000000100000000003'),
    unsubscribe ('120000001', '120000002')('12000000100000000000'),
    unsubscribe ('120000005', '120000002')('12000000500000000001'),
    subscribe   ('120000005', '120000002')('12000000500000000005')
  ].reduce(putAction, store.batch()).write()

  await [
    unsubscribe ('120000001', '120000002')('12000000100000000002'),
    unsubscribe ('120000001', '120000002')('12000000100000000003'),
    unsubscribe ('120000005', '120000002')('12000000500000000000'),
    unsubscribe ('120000005', '120000002')('12000000500000000005')
  ].reduce(putAction, store.batch()).write()

  setTimeout(async () => {
    console.log(await store.get('p:subscriptions'))
  }, 300)
}

const lastSnapshot = namespace => {
  const options = {
    keys: false, values: true,
    gte: `snapshot:${namespace}`,
    lte: `snapshot:${namespace}\xff`,
    reverse: true,
    limit: 1
  }

  return new Promise((resolve, reject) => {
    store.createReadStream(options)
      .on('error', reject)
      .on('data', resolve)
      .on('end', resolve)
  })
}

// Stream of all snapshots for namespace in reverse order.
const snapshots = namespace => {
  const options = {
    gte: `snapshot:${namespace}`,
    lte: `snapshot:${namespace}\xff`,
    reverse: true
  }

  return store.createReadStream(options)
}

class SkipStream extends Transform {
  constructor(n) {
    super({objectMode: true})
    this.n = n
    this.skipped = 0
  }

  _transform(chunk, encoding, callback) {
    if(this.skipped < this.n) this.skipped += 1
    else this.push(chunk)
    callback()
  }
}

class PrintStream extends Writable {
  constructor() {
    super({objectMode: true})
  }

  _write(chunk, encoding, callback) {
    console.log(chunk)
    callback()
  }
}

class DeleteStream extends Writable {
  constructor(store) {
    super({objectMode: true})
    this.batch = store.batch()
  }

  _write(chunk, encoding, callback) {
    this.batch.del(chunk.key)
    callback()
  }

  _final(callback) {
    this.batch.write(callback)
  }
}

// Delete snapshots except last/most current.
const purgeSnapshots = namespace =>
  snapshots(namespace).pipe(new SkipStream(1)).pipe(new DeleteStream(store))

const query = async () => {
  await store.put('snapshot:subscriptions:1523085867671174271', {})
  await store.put('snapshot:subscriptions:1523085867678449691', {})
  await store.put('snapshot:subscriptions:1523085867679616713', {yeah: 'baby'})
  await store.put('snapshot:connections:1523085867671159187', {snapshot: 1, timestamp: '1523085867671159187'})
  await store.put('snapshot:connections:1523085867678238770', {snapshot: 2, timestamp: '1523085867678238770'})
  await store.put('snapshot:connections:1523085867679616713', {snapshot: 3, timestamp: '1523085867679616713'})
  await store.put('x', {tag: 'end'})

  console.log(await lastSnapshot('connections'))

  purgeSnapshots('subscriptions').on('finish', () => {
    snapshots('subscriptions').pipe(new PrintStream())
  })
}

//populate()
query()
