module.exports = audit_hypercore

async function audit_hypercore (feed) {
  return new Promise ((resolve, reject) => {
    feed.audit((err, res) => {
      console.log({err, res})
    })
  })
}