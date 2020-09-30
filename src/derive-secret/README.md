# Derive-Secret

This module is used to derive a secret from a set of peers

## How it works

- Get a set of peer IDs
- Get a function to verify a some signed data for a peer ID
- Get a function to sign some data
- Get a function to broadcast a message to your peers (uses hyper-flood)
- Generate some random bytes
- Hash them, sign the hash, and sign the final value
- Broadcast the hash to your peers
- On getting a hash, save it
- Once you've seen everyone's hashes, broadcast your random bytes + signature
- On receiving random bytes from a peer, verify them against the hash
- If the bytes don't match the hash, send the signed random bytes and 

## Dream API

```js
// Will throw if there's an error deriving the secret
const secret = await deriveSecret({
  // How many bytes of randomness you should be generating
  numBytes = 32,
  // Pass in a function that will send a message to a given peer
  async broadcast(message),

  // Pass in a function that returns an async iterator for incoming messages
  async * listenMessages(),

  // Pass in the array of peer IDs
  peers: [id]

  // Pass in your own ID
  id,

  // Pass in a function to generate a signature
  async sign(message) => signature

  // Pass in a function to verify something was signed by a peer
  async verify(id, message, signature),

  // Pass in a function that will has your data
  async hashBytes(bytes),
})
```
