const {MongoClient} = require('mongodb')
const through = require('through2').obj
const pumpify = require('pumpify').obj
const debug = require('debug')('mongo-write-stream')
const batchify = require('@jdesboeufs/batch-stream')

const DEFAULT_HOST = 'localhost'
const DEFAULT_PORT = 27017
const DEFAULT_BATCH_SIZE = 1000

function createMongoWriter(options = {}) {
  if (!options.db) {
    throw new Error('db is required')
  }
  if (!options.collection) {
    throw new Error('collection is required')
  }
  let client
  let collection
  const pipeline = pumpify(
    through(async (item, enc, cb) => {
      if (collection) {
        cb(null, item)
      } else {
        debug('No connection. Establishing…')
        try {
          const url = `mongodb://${options.host || DEFAULT_HOST}:${options.port || DEFAULT_PORT}`
          client = await MongoClient.connect(url, {useNewUrlParser: true, reconnectTries: 1})
          collection = client.db(options.db).collection(options.collection)
          cb(null, item)
        } catch (error) {
          cb(error)
        }
      }
    }),
    batchify(options.batchSize || DEFAULT_BATCH_SIZE),
    through(async (items, enc, cb) => {
      try {
        debug(`Inserting ${items.length} items…`)
        await collection.insertMany(
          options.map ? items.map(options.map) : items
        )
        cb()
      } catch (error) {
        cb(error)
      }
    })
  )
  pipeline.on('finish', () => {
    if (client) {
      client.close()
    }
  })
  return pipeline
}

module.exports = createMongoWriter
