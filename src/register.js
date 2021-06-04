const dateToBlockNumber = require('dateToBlockNumber')
/******************************************************************************
  ROLE: User

    1. create account

******************************************************************************/

module.exports = register

async function register (profile, APIS) {
  const { name, log } = profile
  const { chainAPI, vaultAPI } = APIS
  
  const nonce = await vaultAPI.getNonce()
  const myAddress = await vaultAPI.chainKeypair.address
  log({ type: 'peer', data: [`My address ${myAddress}`] })
  const signer = await vaultAPI.chainKeypair
  const noiseKey = await vaultAPI.noisePublicKey
  const signingPublicKey = await vaultAPI.signingPublicKey
  const data = { signingPublicKey, noiseKey }
  log({ type: 'peer', data: [`New account created => ${myAddress}`] })
  await chainAPI.newUser({ signer, nonce, data })
}
