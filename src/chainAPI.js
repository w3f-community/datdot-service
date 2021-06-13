const ApiPromise = require('../lab/simulations/simulate-polkadotjs')
// const { ApiPromise, WsProvider, Keyring } = require('@polkadot/api')
// const { randomAsU8a } = require('@polkadot/util-crypto') // make sure version matches api version
// const { hexToBn, u8aToBuffer, bufferToU8a } = require('@polkadot/util')
const fs = require('fs')
const path = require('path')
const filename = path.join(__dirname, './types.json')
const types = JSON.parse(fs.readFileSync(filename).toString())
// const types = require('datdot-substrate/types.json')

module.exports = datdotChain

async function datdotChain (profile, provider) {
  const { name } = profile
  const log = profile.log.sub('chain')
  // provider = new WsProvider(`${address}:${port}`)
  const API = await rerun(() => ApiPromise.create({ name, provider, types }))
  const chainAPI = {
    getBlockNumber,
    newUser,
    registerForWork,
    publishFeed,
    publishPlan,
    amendmentReport,
    submitStorageChallenge,
    submitPerformanceChallenge,
    listenToEvents,
    getFeedByID,
    getPlanByID,
    getAmendmentByID,
    getContractByID,
    getStorageChallengeByID,
    getPerformanceChallengeByID,
    getItemByID,
    getFeedKey,
    getUserAddress,
    getUserIDByNoiseKey,
    getUserIDBySigningKey,
    getHosterKey,
    getEncoderKey,
    getAttestorKey,
    getSigningKey,
  }

  return chainAPI

  async function status ({ events = [], status }) {
    if (status.isInBlock) {
      events.forEach(({ phase, event: { data, method, section } }) => {
        log({ type: 'chainAPI', data: ['\t', phase.toString(), `: ${section}.${method}`, data.toString()] })
      })
    }
  }

  async function getBlockNumber () {
    const header = await API.derive.chain.getHeader()
    return header.number
  }

  async function makeNonce (nonce) {
    const NONCE = await API.createType('Index', nonce)
    return { nonce: NONCE }
  }
  async function newUser ({ signer, nonce, data }) {
    const tx = await API.tx.datVerify.newUser(data)
    // tx.signAndSend(signer, await makeNonce(nonce))
    tx.signAndSend(signer, await makeNonce(nonce), status)
  }
  async function registerForWork ({ form, hosterKey, signer, nonce }) {
    // hosterKey = bufferToU8a(hosterKey)
    const tx = await API.tx.datVerify.registerForWork(form)
    tx.signAndSend(signer, await makeNonce(nonce), status)
  }
  async function publishFeed (opts) {
    const { merkleRoot, signer, nonce } = opts
    //   merkleRoot[0] = bufferToU8a(merkleRoot[0])
    const tx = await API.tx.datVerify.publishFeed(merkleRoot)
    // tx.signAndSend(signer, await makeNonce(nonce))
    tx.signAndSend(signer, await makeNonce(nonce), status)
  }
  async function publishPlan (opts) {
    const { data, signer, nonce } = opts
    const tx = await API.tx.datVerify.publishPlan(data)
    // tx.signAndSend(signer, await makeNonce(nonce))
    tx.signAndSend(signer, await makeNonce(nonce), status)
  }
  async function getFeedKey (feedID) {
    // const feed = (await API.query.datVerify.getFeedByID(feedID)).unwrap()
    // return u8aToBuffer(feed.publickey.toU8a())
    const feed = (await API.query.datVerify.getFeedByID(feedID))
    return Buffer.from(feed.feedkey, 'hex')
  }
  async function getItemByID (id) {
    // const feed = (await API.query.datVerify.getItemByID(id)).unwrap()
    return await API.query.datVerify.getItemByID(id)
  }
  async function getFeedByID (feedID) {
    // const feed = (await API.query.datVerify.getFeedByID(feedID)).unwrap()
    const feed = (await API.query.datVerify.getFeedByID(feedID))
    feed.feedkey = Buffer.from(feed.feedkey, 'hex')
    return feed
  }
  async function getUserIDByNoiseKey (key) {
    // const user = (await API.query.datVerify.getUserByID(id)).unwrap()
    // return user.address.toString()
    return await API.query.datVerify.getUserIDByNoiseKey(key)
  }
  async function getUserIDBySigningKey (key) {
    // const user = (await API.query.datVerify.getUserByID(id)).unwrap()
    // return user.address.toString()
    return await API.query.datVerify.getUserIDBySigningKey(key)
  }
  async function getUserAddress (id) {
    // const user = (await API.query.datVerify.getUserByID(id)).unwrap()
    // return user.address.toString()
    const user = await API.query.datVerify.getUserByID(id)
    return user.address
  }
  async function getSigningKey (id) {
    const user = (await API.query.datVerify.getUserByID(id))
    return Buffer.from(user.signingKey, 'hex')
  }
  async function getHosterKey (id) {
    const user = (await API.query.datVerify.getUserByID(id))
    return Buffer.from(user.noiseKey, 'hex')
  }
  async function getEncoderKey (id) {
    // return u8aToBuffer(user.noise_key.toU8a().slice(1))
    const user = (await API.query.datVerify.getUserByID(id))
    return Buffer.from(user.noiseKey, 'hex')
  }
  async function getAttestorKey (id) {
    // const user = (await API.query.datVerify.getUserByID(id)).unwrap()
    // return u8aToBuffer(user.noise_key.toU8a().slice(1))
    const user = (await API.query.datVerify.getUserByID(id))
    return Buffer.from(user.noiseKey, 'hex')
  }
  async function getContractByID (id) {
    // return (await API.query.datVerify.getContractByID(id)).toJSON()
    return await API.query.datVerify.getContractByID(id)
  }
  async function getAmendmentByID (id) {
    // return (await API.query.datVerify.getAmendmentByID(id)).toJSON()
    return await API.query.datVerify.getAmendmentByID(id)
  }
  async function getPlanByID (id) {
    // return (await API.query.datVerify.getPlanByID(id)).toJSON()
    return await API.query.datVerify.getPlanByID(id)
  }
  async function getStorageChallengeByID (id) {
    // return (await API.query.datVerify.getStorageChallengeByID(id)).toJSON()
    return await API.query.datVerify.getStorageChallengeByID(id)
  }
  async function getPerformanceChallengeByID (id) {
    // return (await API.query.datVerify.getPerformanceChallengeByID(id)).toJSON()
    return await API.query.datVerify.getPerformanceChallengeByID(id)
  }
  async function amendmentReport (opts) {
    const { report, signer, nonce } = opts
    const tx = await API.tx.datVerify.amendmentReport(report)
    // tx.signAndSend(signer, await makeNonce(nonce))
    tx.signAndSend(signer, await makeNonce(nonce), status)
  }
  async function submitStorageChallenge (opts) {
    const { response, signer, nonce } = opts
    const tx = await API.tx.datVerify.submitStorageChallenge(response)
    // tx.signAndSend(signer, await makeNonce(nonce))
    tx.signAndSend(signer, await makeNonce(nonce), status)
  }
  async function submitPerformanceChallenge (opts) {
    const { performanceChallengeID, report, signer, nonce } = opts
    const tx = await API.tx.datVerify.submitPerformanceChallenge(performanceChallengeID, report)
    // tx.signAndSend(signer, await makeNonce(nonce))
    tx.signAndSend(signer, await makeNonce(nonce), status)
  }
  // LISTEN TO EVENTS
  async function listenToEvents (handleEvent) {
    return API.query.system.events((events) => {
      events.forEach(async (record) => {
        // log(record.event.method, record.event.data.toString())
        const event = record.event
        handleEvent(event)
      })
    })
  }
  function rerun (promiseFn, maxTries = 20, delay = 100) {
    let counter = 0
    while (true) {
      // Try to execute the promise function and return the result
      try {
        return promiseFn()
      } catch (error) {
        if (counter >= maxTries) throw error
      }
      // If we get an error maxTries time, we finally error
      // Otherwise we increase the counter and keep looping
      counter++
    }
  }
}
