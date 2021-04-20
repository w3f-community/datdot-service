const p2plex = require('p2plex')
const sub = require('subleveldown')
const defer = require('promise-defer')
const reallyReady = require('hypercore-really-ready')
const { seedKeygen } = require('noise-peer')

const HosterStorage = require('./hoster-storage')
const peerConnect = require('../p2plex-connection')
const requestResponse = require('../requestResponse')
const hypercore = require('hypercore')
const RAM = require('random-access-memory')
const { toPromises } = require('hypercore-promisifier')
const Hyperbeam = require('hyperbeam')
const derive_topic = require('../derive_topic')
const getRangesCount = require('../getRangesCount')

const NAMESPACE = 'datdot-hoster'
const NOISE_NAME = 'noise'
const ALL_KEYS_KEY = 'all_keys'
const DEFAULT_OPTS = { ranges: [{ start: 0, end: Infinity }], watch: true }
const DEFAULT_TIMEOUT = 7500

module.exports = class Hoster {
  constructor ({ db, sdk, EncoderDecoder }, log) {
    const { Hypercore } = sdk
    this.log = log
    this.storages = new Map()
    this.keyOptions = new Map()
    this.watchingFeeds = new Set()
    this.loaderCache = new Map()
    this.db = db
    this.hosterDB = sub(this.db, 'hoster')
    this.sdk = sdk
    this.Hypercore = Hypercore
    this.EncoderDecoder = EncoderDecoder
  }

  async init () {
    const noiseSeed = await this.sdk.deriveSecret(NAMESPACE, NOISE_NAME)
    const noiseKeyPair = seedKeygen(noiseSeed)
    this.communication = p2plex({ keyPair: noiseKeyPair, maxPeers: Infinity })
    this.communication.setMaxListeners(128)
    this.publicKey = noiseKeyPair.publicKey
    const keys = await this.listKeys()
    for (const { key, options } of keys) {
      await this.setOpts(key, options)
      await this.getStorage(key)
      await this.loadFeedData(key)
    }
  }

  static async load (opts, log) {
    const hoster = new Hoster(opts, log)
    await hoster.init()
    return hoster
  }

      /* ------------------------------------------- 
            1. GET ENCODED AND START HOSTING
      -------------------------------------------- */

  async hostFor ({ amendmentID, feedKey, hosterKey, attestorKey, plan, ranges }) {
    await this.setOpts(feedKey, plan)
    await this.addKey(feedKey, plan)
    await this.loadFeedData(feedKey)
    await this.getEncodedDataFromAttestor({ amendmentID, hosterKey, attestorKey, feedKey, ranges })
  }
  
  async getEncodedDataFromAttestor ({ amendmentID, hosterKey, attestorKey, feedKey, ranges }) {
    const hoster = this
    const log2attestor = hoster.log.sub(`<-Attestor ${attestorKey.toString('hex').substring(0,5)}`)

    return new Promise(async (resolve, reject) => {
      const expectedChunkCount = getRangesCount(ranges)
      const all_hosted = []
      let counter = 0
      
      /* ------------------------------------------- 
      a. CONNECT TO ATTESTOR
      -------------------------------------------- */
      const topic_attestor1 = derive_topic({ senderKey: attestorKey, feedKey, receiverKey: hosterKey, id: amendmentID })
      const beam1 = new Hyperbeam(topic_attestor1)
      
      // get the key and replicate attestor hypercore
      const temp_topic1 = topic_attestor1 + 'once'
      const beam_temp1 = new Hyperbeam(temp_topic1)
      beam_temp1.once('data', async (data) => {
        const message = JSON.parse(data.toString('utf-8'))
        if (message.type === 'feedkey') replicate(Buffer.from(message.feedkey, 'hex'))
      })
      
      async function replicate (feedkey) {
        const clone1 = toPromises(new hypercore(RAM, feedkey, {
          valueEncoding: 'utf-8',
          sparse: true
        }))
        
        // pipe streams
        const clone1Stream = clone1.replicate(false, { live: true })
        clone1Stream.pipe(beam1).pipe(clone1Stream)
        
        // // get replicated data
        for (var i = 0; i < expectedChunkCount; i++) {
          log2attestor({ type: 'hoster', data: [`Getting data: counter ${i}`] })
          all_hosted.push(store_data(clone1.get(i)))
          // beam_temp1.destroy()
        }

        // resolve
        const results = await Promise.all(all_hosted).catch(err => {
          log2attestor({ type: 'error', data: [`Error getting results ${err}`] })
        })
        // console.log({results})
        if (results.length === expectedChunkCount) {
          log2attestor({ type: 'hoster', data: [`All data (${expectedChunkCount} chunks) successfully hosted`] })
          // beam1.destroy()
          resolve(`All data chunks successfully hosted`)
        }
    
        /* ------------------------------------------- 
                    b. STORE
        -------------------------------------------- */

        async function store_data (data_promise) {
          const message = await data_promise
          const data = JSON.parse(message.toString('utf-8'))
          log2attestor({ type: 'hoster', data: [`RECV_MSG with index: ${data.index} from attestor ${attestorKey.toString('hex')}`] })
          
          return new Promise(async (resolve, reject) => {
            counter++
            const { type } = data
            if (type === 'verified') {
              if (!is_valid_data(data)) return
              const { feed, index, encoded, proof, nodes, signature } = data
              log2attestor({ type: 'hoster', data: [`Storing verified message with index: ${data.index}`] })
              const key = Buffer.from(feed)
              const isExisting = await hoster.hasKey(key)
              // Fix up the JSON serialization by converting things to buffers
              for (const node of nodes) node.hash = Buffer.from(node.hash)
              if (!isExisting) {
                const error = { type: 'encoded:error', error: 'UNKNOWN_FEED', ...{ key: key.toString('hex') } }
                // stream.write(error)
                // stream.end()
                return reject(error)
              }
              try {
                await hoster.storeEncoded({
                  key,
                  index,
                  proof: Buffer.from(proof),
                  encoded: Buffer.from(encoded),
                  nodes,
                  signature: Buffer.from(signature)
                })
                // console.log('Received', index)
                log2attestor({ type: 'hoster', data: [`Hoster received & stored index: ${index} (${counter}/${expectedChunkCount}`] })
                resolve({ type: 'encoded:stored', ok: true, index: data.index })
              } catch (e) {
                // Uncomment for better stack traces
                const error = { type: 'encoded:error', error: `ERROR_STORING: ${e.message}`, ...{ e }, data }
                log2attestor({ type: 'error', data: [`Error: ${error}`] })
                // beam1.destroy()
                return reject(error)
              }
            } else {
              log2attestor({ type: 'error', data: [`UNKNOWN_MESSAGE messageType: ${type}`] })
              const error ={ type: 'encoded:error', error: 'UNKNOWN_MESSAGE', ...{ messageType: type } }
              // beam1.destroy()
              return reject(error)
            }
          })
    
          async function is_valid_data (data) {
            const { feed, index, encoded, proof, nodes, signature } = data
            return !!(feed && index && encoded && proof && nodes && signature)
          }
        }
  
      }
    })
    
  }

    /* ------------------------------------------- 
        2. CHALLENGES
  -------------------------------------------- */

  async sendStorageChallenge ({ storageChallenge, hosterKey, feedKey, attestorKey }) {
    const hoster = this
    const sent_chunks = {}
    return new Promise(async (resolve, reject) => {
      const log2attestor4Challenge = hoster.log.sub(`<-Attestor4challenge ${attestorKey.toString('hex').substring(0,5)}`)
      const { id, chunks } = storageChallenge
      const topic = derive_topic({ senderKey: hosterKey, feedKey, receiverKey: attestorKey, id })
      const tid = setTimeout(() => {
        beam.destroy()
        reject({ type: `attestor_timeout` })
      }, DEFAULT_TIMEOUT)
      const beam = new Hyperbeam(topic)
      beam.on('error', err => { 
        console.log({err})
        clearTimeout(tid)
        beam.destroy()
        if (beam_once) {
          beam_once.destroy()
          reject({ type: `attestor_connection_fail`, data: err })
        }
      })
      const once_topic = topic + 'once'
      var beam_once = new Hyperbeam(once_topic)
      beam_once.on('error', err => {
        clearTimeout(tid)
        beam_once.destroy()
        beam.destroy()
        reject({ type: `hoster_connection_fail`, data: err })
      })
      const core = toPromises(new hypercore(RAM, { valueEncoding: 'utf-8' }))
      await core.ready()
      core.on('error', err => {
        Object.values(chunks).forEach(({ reject }) => reject(err))
      })
      const coreStream = core.replicate(true, { live: true, ack: true })
      coreStream.pipe(beam).pipe(coreStream)
      coreStream.on('ack', ack => {
        const index = ack.start
        const resolve = sent_chunks[index].resolve
        delete sent_chunks[index]
        resolve('attestor received storage proofs')
      })
      beam_once.write(JSON.stringify({ type: 'feedkey', feedkey: core.key.toString('hex')}))
      const all = []
      for (var i = 0; i < chunks.length; i++) {
        const index = chunks[i]
        const storageChallengeID = id
        var signed_event = (i === 0) ? get_signed_event(storageChallengeID) : undefined
        const data = await hoster.getStorageChallenge(feedKey, index) 
        if (!data) return
        const message = { type: 'proof', storageChallengeID, data, signed_event }
        log2attestor4Challenge({ type: 'hoster', data: [`Storage proof: appending chunk ${i} for index ${index}`] })
        all.push(send(message, i))
      }
      function get_signed_event (storageChallengeID) {
        const sig = 'foobar'
        return sig
      }
      function send (message, i) {
        return new Promise(async (resolve, reject) => {
          await core.append(JSON.stringify(message))
          sent_chunks[i] = { resolve, reject }
        })
      }
      try {
        const results = await Promise.all(all).catch((error) => {
          console.log({error})
          log2attestor4Challenge({ type: 'error', data: [`error: ${error}`] })
          clearTimeout(tid)
          beam_once.destroy()
          beam.destroy()
          reject({ type: `hoster_proof_fail`, data: error })
        })
        if (!results) log2attestor4Challenge({ type: 'error', data: [`No results`] })
        console.log({results})
        log2attestor4Challenge({ type: 'hoster', data: [`${all.length} responses received from the attestor`] })
        log2attestor4Challenge({ type: 'hoster', data: [`Destroying communication with the attestor`] })
        clearTimeout(tid)
        beam_once.destroy()
        beam.destroy()
        resolve({ type: `DONE`, data: results })
      } catch (err) {
        log2attestor4Challenge({ type: 'error', data: [`Error: ${err}`] })
        clearTimeout(tid)
        beam_once.destroy()
        beam.destroy()
        reject({ type: `hoster_proof_fail`, data: err })
      }
    })
  }

  async removeFeed (key) {
    this.log({ type: 'hoster', data: [`Removing the feed`] })
    const stringKey = key.toString('hex')
    if (this.storages.has(stringKey)) {
      const storage = await this.getStorage(key)
      await storage.destroy()
      this.storages.delete(stringKey)
    }
    await this.setOpts(stringKey, null)
    await this.removeKey(key)
  }

  async loadFeedData (key) {
    const stringKey = key.toString('hex')
    const deferred = defer()
    // If we're already loading this feed, queue up our promise after the current one
    if (this.loaderCache.has(stringKey)) {
      // Get the existing promise for the loader
      const existing = this.loaderCache.get(stringKey)
      // Create a new promise that will resolve after the previous one and
      this.loaderCache.set(stringKey, existing.then(() => deferred.promise))
      // Wait for the existing loader to resolve
      await existing
    } else {
      // If the feed isn't already being loaded, set this as the current loader
      this.loaderCache.set(stringKey, deferred.promise)
    }
    try {
      const { ranges, watch } = await this.getOpts(key)
      const storage = await this.getStorage(key)
      const { feed } = storage
      await feed.ready()
      const { length } = feed
      for (const { start, wantedEnd } of ranges) {
        const end = Math.min(wantedEnd, length)
        feed.download({ start, end })
      }
      if (watch) this.watchFeed(feed)
      this.loaderCache.delete(stringKey)
      deferred.resolve()
    } catch (e) {
      this.loaderCache.delete(stringKey)
      deferred.reject(e)
    }
  }

  async watchFeed (feed) {
    this.warn('Watching is not supported since we cannot ask the chain for attestors')
    /* const stringKey = feed.key.toString('hex')
    if (this.watchingFeeds.has(stringKey)) return
    this.watchingFeeds.add(stringKey)
    feed.on('update', onUpdate)
    async function onUpdate () {
      await this.loadFeedData(feed.key)
    } */
  }

  async storeEncoded ({ key, index, proof, encoded, nodes, signature }) {
    const storage = await this.getStorage(key)
    return storage.storeEncoded(index, proof, encoded, nodes, signature)
  }

  async getStorageChallenge (key, index) {
    const storage = await this.getStorage(key)
    // const _db = storage.db
    // console.log({_db})
    const data = await storage.getStorageChallenge(index)
    return data
  }

  async hasKey (key) {
    const stringKey = key.toString('hex')
    return this.storages.has(stringKey)
  }

  async getStorage (key) {
    const stringKey = key.toString('hex')
    if (this.storages.has(stringKey)) {
      return this.storages.get(stringKey)
    }
    const feed = this.Hypercore(key, { sparse: true })
    const db = sub(this.db, stringKey, { valueEncoding: 'binary' })
    await reallyReady(feed)
    const storage = new HosterStorage({ EncoderDecoder: this.EncoderDecoder, db, feed, log: this.log })
    this.storages.set(stringKey, storage)
    return storage
  }

  async listKeys () {
    try {
      const keys = await this.hosterDB.get(ALL_KEYS_KEY)
      return keys
    } catch {
      // Must not have any keys yet
      return []
    }
  }

  async saveKeys (keys) {
    await this.hosterDB.put(ALL_KEYS_KEY, keys)
  }

  async addKey (key, options) {
    const stringKey = key.toString('hex')
    const existing = await this.listKeys()
    const data = { key: stringKey, options }
    const final = existing.concat(data)
    await this.saveKeys(final)
  }

  async removeKey (key) {
    this.log({ type: 'hoster', data: [`Removing the key`] })
    const stringKey = key.toString('hex')
    const existing = await this.listKeys()
    const final = existing.filter((data) => data.key !== stringKey)
    await this.saveKeys(final)
    this.log({ type: 'hoster', data: [`Key removed`] })
  }

  async setOpts (key, options) {
    const stringKey = key.toString('hex')
    this.keyOptions.set(stringKey, options)
  }

  async getOpts (key) {
    const stringKey = key.toString('hex')
    return this.keyOptions.get(stringKey) || DEFAULT_OPTS
  }

  async close () {
    // await this.communication.destroy()
    // Close the DB and hypercores
    for (const storage of this.storages.values()) {
      await storage.close()
    }
  }

}