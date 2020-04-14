const Hypercore = require('hypercore')
const hyperswarm = require('hyperswarm')
const swarm = hyperswarm()
const ram = require('random-access-memory')
var hypercore = Hypercore(ram)
let hypercoreArr = []
let feeds = {}

const colors = require('colors/safe');
const NAME = __filename.split('/').pop().split('.')[0].toUpperCase()
function LOG (...msgs) {
  msgs = [`[${NAME}] `, ...msgs].map(msg => colors.yellow(msg))
  console.log(...msgs)
}

module.exports = new Promise(getHypercoreArr)

async function getHypercoreArr (resolve, reject) {
  var demo = {
  	"Node": {
  		"index": "u64",
  		"hash": "H256",
  		"size": "u64"
  	},
    "Nod1e": {
      "index": "u64",
      "hash": "H256",
      "size": "u64"
    },
    "Node3": {
      "index": "u64",
      "hash": "H256",
      "size": "u64"
    },
  	"Proof": {}
  }

  const data = Buffer.from(JSON.stringify(demo), 'utf8')
  hypercore.append(data, (err) => {
    if (err) return LOG(err) && reject(err)
    getKey()
  })


  function getKey () {
    var address = hypercore.key
    LOG('HYPERCORE KEY:', address.toString('hex'))
    hypercoreArr.push(address) // ed25519::Public
    getRootHash(hypercoreArr)
  }
  function getRootHash (hypercoreArr) {
    const index = hypercore.length - 1
    const childrenArr = []
    hypercore.rootHashes(index, (err, res) => {
      if (err) return LOG(err) && reject(err)
      res.forEach(root => {
        childrenArr.push({
          hash: root.hash,
          hash_number: root.index,
          total_length: root.size
        })
      })
      hypercoreArr.push({
        hashType: 2, // u8 <= hard coded (internal substrate id)
        children: childrenArr //  Vec<ParentHashInRoot>
      })
      getSignature(hypercoreArr)
    })
  }
  function getSignature (hypercoreArr) {
    hypercore.signature((err, res) => {
      if (err) LOG(err) && reject(err)
      hypercoreArr.push(res.signature) // ed25519::Signature
      feeds[hypercore.key.toString('hex')] = hypercore
      resolve([hypercoreArr, feeds])
    })
  }
}