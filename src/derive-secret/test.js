const test = require('tape')
const { on, EventEmitter } = require('events')
const deriveSecret = require('./')

function makeHarness (peers) {
  const events = new EventEmitter()

  async function * listenMessages () {
    for await (const [message] of on(events, 'broadcast')) {
      yield message
    }
  }

  function broacast (from, message) {
    events.emit('broadcast', message, from)
  }

  return {
    listenMessages,
    broacast
  }
}

test('Derive between two peers', async (t) => {
  try {
    const { listenMessages, broacast } = makeHarness()

    const peers = [Buffer.from([1]), Buffer.from([2])]

    const [bytes1, bytes2] = await Promise.all([
      deriveSecret({
        id: peers[0],
        peers,
        sign: (message) => sign(peers[0], message),
        verify,
        hashBytes,
        listenMessages: () => listenMessages(peers[0]),
        broadcast: (message) => broacast(peers[0], message)
      }),
      deriveSecret({
        id: peers[1],
        peers,
        sign: (message) => sign(peers[1], message),
        verify,
        hashBytes,
        listenMessages: () => listenMessages(peers[1]),
        broadcast: (message) => broacast(peers[1], message)
      })
    ])

    t.pass('able to derive')

    t.deepEqual(bytes1, bytes2, 'Converged on same random value')
  } catch (e) {
    console.error(e)
    t.fail(e)
  } finally {
    t.end()
  }
})

async function sign (id, message) {
  console.log('Sign', id, message)
	return Buffer.concat([message, id])
}

async function verify (id, message, signature) {
  console.log('Verify', id, message, signature)
	return Buffer.concat([message, id]).equals(signature)
}

async function hashBytes (bytes) {
  return bytes
}
