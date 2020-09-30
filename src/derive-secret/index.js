const { randomBytes } = require('crypto')
const { Message } = require('messages')

module.exports = async function detiveSecret ({
  numBytes,
  broadcast,
  listenMessages,
  peers,
  sign,
  id,
  verify,
  hashBytes
}) {
  // Set up map of hashes
  // Start consuming messages
  // Generate random values
  let bytes = null
  let hash = null
  const gotHashes = new Map()
  const gotBytes = new Map()

  const [derived] = Promise.all([
    consumeMessages(),
    run()
  ])

  return derived

  async function run () {
    bytes = randomBytes(numBytes)
    hash = await hashBytes(bytes)
    await broadcastHash()
  }

  async function consumeMessages () {
    for await (const message of listenMessages()) {
      // Parse out message
      const { type, ...parsed } = Message.parse(message)
      try {
        if (type === 'hash') {
          const { signature, hash, id } = parsed

          await verifyValid(id, hash, signature)

          gotHashes.set(id.toString('hex'), hash)

          if (hasAllHashes()) {
            await broadcastRandomBytes()
          }
        } else if (type === 'bytes') {
          const { signature, bytes, id } = parsed

          await verifyValid(id, bytes, signature)

          await verifyStoredHash(id, bytes)

          gotBytes.set(id.toString('hex'), bytes)

          if (hasAllBytes()) {
            return generateDerived()
          }
        } else if (type === 'error') {
          // TODO: Verify the error
          // TODO: Make this message more verbose
          throw new Error('Got error from peers')
        }
      } catch (e) {
        broadcastMessage({
          ...parsed,
          type: 'error',
          error: e.message
        })

        throw e
      }
    }
  }

  async function verifyValid (id, data, signature) {
    const isValid = await verify(id, data, signature)
    if (!isValid) {
      throw new Error('Got invalid signature from peer')
    }
  }

  async function verifyStoredHash (id, bytes) {
    const storedHash = gotHashes.get(id.toString('hex'))
    if (!storedHash) throw new Error('Got bytes without hash')
    const givenHash = await hashBytes(bytes)

    if (!storedHash.equals(givenHash)) throw new Error('Bytes different from hash')
  }

  async function broadcastMessage (message) {
    await broadcast(Message.encode(message))
  }

  async function generateDerived () {}

  function hasAllBytes () {}

  async function broadcastHash () {
    const signature = await sign(hash)
    await broadcastMessage({
      type: 'hash',
      id,
      hash,
      signature
    })
  }

  function hasAllHashes () {

  }

  async function broadcastRandomBytes () {
    const signature = await sign(bytes)
    await broadcastMessage({
      type: 'bytes',
      id,
      bytes,
      signature
    })
  }
}
