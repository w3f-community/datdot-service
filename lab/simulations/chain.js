const DB = require('../../src/DB')
const makeSets = require('../../src/makeSets')
const blockgenerator = require('../../src/scheduleAction')
const logkeeper = require('../scenarios/logkeeper')
const WebSocket = require('ws')
const PriorityQueue = require('../../src/priority-queue')
const priority_queue= PriorityQueue(compare)
const connections = {}
const handlers = []
const scheduler = init()
var header = 0

function compare (item) {
  return item
}

async function init () {
  const [json, logport] = process.argv.slice(2)
  const config = JSON.parse(json)
  const [host, PORT] = config.chain
  const name = `chain`
  const log = await logkeeper(name, logport)
  const wss = new WebSocket.Server({ port: PORT }, after)
  function after () {
    log({ type: 'chain', data: [`running on http://localhost:${wss.address().port}`] })
  }
  wss.on('connection', function connection (ws) {
    ws.on('message', async function incoming (message) {
      const { flow, type, data } = JSON.parse(message)
      const [from, id] = flow

      if (id === 0) { // a new connection
        // 1. do we have that user in the database already?
        // 2. is the message verifiable?
        // 3. => add to database
        // TODO:
        // OLD:
        if (!connections[from]) {
          connections[from] = { name: from, counter: id, ws, log: log.sub(from) }
          handlers.push([from, data => ws.send(JSON.stringify({ data }))])
        }
        else return ws.send(JSON.stringify({
          cite: [flow], type: 'error', data: 'name is already taken'
        }))

        return
      }
      // 1. is that message verifiable
      // ...


      const _log = connections[from].log
      _log({ type: 'chain', data: [`${JSON.stringify(type)} ${JSON.stringify(flow)}`] })
      const method = queries[type] || signAndSend
      if (!method) return ws.send({ cite: [flow], type: 'error', data: 'unknown type' })
      const result = await method(data, from, data => {
        // _log({ type: 'chain', data: [`send data after "${type}" to: ${from}`] })
        ws.send(JSON.stringify({ cite: [flow], type: 'data', data }))
      })
      if (!result) return
      const msg = { cite: [flow], type: 'done', data: result }
      // _log({ type: 'chain', data: [`sending "${type}" to: ${from}`] })
      ws.send(JSON.stringify(msg))
    })
  })
  return blockgenerator(log.sub('blockgenerator'), blockMessage => {
    header = blockMessage.data
    Object.entries(connections).forEach(([name, channel]) => {
      channel.ws.send(JSON.stringify(blockMessage))
    })
  })
}
/******************************************************************************
  QUERIES
******************************************************************************/
const queries = {
  getFeedByID,
  getFeedByKey,
  getUserByID,
  getUserIDByKey,
  getPlanByID,
  getAmendmentByID,
  getContractByID,
  getStorageChallengeByID,
  getPerformanceChallengeByID,
}

// function getFeedByID (id) { return DB.feeds[id] }
// function getUserByID (id) { return DB.users[id] }
// function getPlanByID (id) { return DB.plans[id] }
// function getContractByID (id) { return DB.contracts[id] }
// function getAmendmentByID (id) { return DB.amendments[id] }
// function getStorageChallengeByID (id) { return DB.storageChallenges[id] }
// function getPerformanceChallengeByID (id) { return DB.performanceChallenges[id] }
function getDatasetByID (id) { return getItem(id) }
function getFeedByID (id) { return getItem(id) }
function getUserByID (id) { return getItem(id) }
function getPlanByID (id) { return getItem(id) }
function getContractByID (id) { return getItem(id) }
function getAmendmentByID (id) { return getItem(id) }
function getStorageChallengeByID (id) { return getItem(id) }
function getPerformanceChallengeByID (id) { return getItem(id) }
// ---
function getFeedByKey (key) {
  const keyBuf = Buffer.from(key, 'hex')
  return DB.lookups.feedByKey[keyBuf.toString('hex')]
}
function getUserIDByKey(key) {
  const keyBuf = Buffer.from(key, 'hex')
  return DB.lookups.userIDByKey[keyBuf.toString('hex')]
}
/******************************************************************************
  ROUTING (sign & send)
******************************************************************************/
async function signAndSend (data, name, status) {
  const log = connections[name].log
  const { type, args, nonce, address } = data

  status({ events: [], status: { isInBlock:1 } })

  const user = await _loadUser(address, { name, nonce }, status)
  if (!user) return log({ type: 'chain', data: [`UNKNOWN SENDER of: ${data}`] }) // TODO: maybe use status() ??

  else if (type === 'publishPlan') _publishPlan(user, { name, nonce }, status, args)
  else if (type === 'registerEncoder') _registerEncoder(user, { name, nonce }, status, args)
  else if (type === 'registerAttestor') _registerAttestor(user, { name, nonce }, status, args)
  else if (type === 'registerHoster') _registerHoster(user, { name, nonce }, status, args)
  else if (type === 'amendmentReport') _amendmentReport(user, { name, nonce }, status, args)
  else if (type === 'requestStorageChallenge') _requestStorageChallenge(user, { name, nonce }, status, args)
  else if (type === 'requestPerformanceChallenge') _requestPerformanceChallenge(user, { name, nonce }, status, args)
  else if (type === 'submitStorageChallenge') _submitStorageChallenge(user, { name, nonce }, status, args)
  else if (type === 'submitPerformanceChallenge') _submitPerformanceChallenge(user, { name, nonce }, status, args)
  // else if ...
}
/******************************************************************************
  API
******************************************************************************/

async function _loadUser (address, { name, nonce }, status) {
  const log = connections[name].log
  let user
  if (DB.lookups.userByAddress[address]) {
    const pos = DB.lookups.userByAddress[address]
    user = getUserByID(pos)
  }
  else {
    // const id = DB.storage.length
    // user = { id, address: address }
    // DB.storage.push(user) // @NOTE: set id
    user = { address }
    await addItem(user)
    DB.lookups.userByAddress[address] = user.id // push to userByAddress lookup array
    log({ type: 'chain', data: [`New user: ${name}, ${user.id}, ${address}`] })
  }
  return user
}
/*----------------------
      STORE ITEM
------------------------*/
function addItem (item) {
  if ('id' in item) throw new Error('new items cannot have "id" property')
  const id = DB.storage.length
  item.id = id
  DB.storage.push([item])
  return id
}
function getItem (id) {
  if (!Number.isInteger(id)) return
  if (id < 0) return
  const len = DB.storage.length
  if (id >= len) return
  const history = DB.storage[id]
  if (!Array.isArray(history)) return
  const next = history.length
  const item = history[next - 1]
  return item
}
function delItem (id) {
  if (!Number.isInteger(id)) return
  if (id < 0) return
  const len = DB.storage.length
  if (id >= len) return
  const history = DB.storage[id]
  if (!Array.isArray(history)) return
  return !!history.push(void 0)
}
function updateItem (id, item) {
  if (!Number.isInteger(id)) return
  if (id < 0) return
  const len = DB.storage.length
  if (id >= len) return
  const history = DB.storage[id]
  if (!Array.isArray(history)) return
  return !!history.push(item)
}
/*----------------------
      PUBLISH FEED
------------------------*/
// TODO:
// * we wont start hosting a plan before the check
// * 3 attestors
// * provide signature for highest index in ranges
// * provide all root hash sizes required for ranges
// => native api feed.getRoothashes() provides the values

/*----------------------
      (UN)PUBLISH PLAN
------------------------*/
async function _publishPlan (user, { name, nonce }, status, args) {
  const log = connections[name].log
  log({ type: 'chain', data: [`Publishing a plan`] })
  let [plan] = args
  const { duration, swarmkey, program, components }  = plan
  const feed_ids = await Promise.all(components.feeds.map(async feed => await publish_feed(feed, user.id, log)))
  const component_ids = await publish_components(log, components, feed_ids)

  const updated_program = []
  for (var i = 0, len = program.length; i < len; i++) {
    const item = program[i]
    if (item.plans) updated_program.push(...getPrograms(item.plan))
    else updated_program.push(handleNew(item, component_ids))
  }
  plan = { duration, swarmkey, program: updated_program }
  if (!planValid({ plan })) return log({ type: 'chain', data: [`Plan from and/or until are invalid`] })
  plan.sponsor = user.id

  plan.contracts = []
  const id = await addItem(plan)

  priority_queue.add({ type: 'plan', id })
  take_next_from_priority(priority_queue.take(), log) // schedule the plan execution
}

async function unpublishPlan (user, { name, nonce }, status, args) {
  const [planID] = args
  const plan = getPlanByID(planID)
  if (!plan.sponsor === user.id) return log({ type: 'chain', data: [`Only a sponsor is allowed to unpublish the plan`] })
  cancelContracts(plan) // remove all hosted and draft contracts
}
/*----------------------
  (UN)REGISTER ROLES
------------------------*/
async function _registerRoles (user, { name, nonce }, status, args) {
  const log = connections[name].log
  if (!verify_registerRoles(args)) throw new Error('invalid args')
  const [role, roleKey, form] = args
  const { components } = form
  const { resources_ids, performances_ids, timetables_ids, regions_ids } = await publish_form_components(components)

  form.timetables = form.timetables.map(ref => { if (ref < 0) return timetables_ids[(Math.abs(ref) - 1)] })
  form.regions = form.regions.map(ref => { if (ref < 0) return regions_ids[(Math.abs(ref) - 1)] })
  form.performances = form.performances.map(ref => { if (ref < 0) return performances_ids[(Math.abs(ref) - 1)] })
  form.resources = form.resources.map(ref => { if (ref < 0) return resources_ids[(Math.abs(ref) - 1)] })

  const userID = user.id
  const registration = [userID]
  // registered.push(role)
  if (!user[role]) user[role] = {}
  if (user[role][roleKey]) return log({ type: 'chain', data: [`User is already registered as a ${role}`] })
  const keyBuf = Buffer.from(roleKey, 'hex')
  DB.lookups.userIDByKey[keyBuf.toString('hex')] = user.id // push to userByRoleKey lookup array
  user[role] = {
    key: keyBuf.toString('hex'),
    form,
    jobs: {},
    idleStorage: getItem(form.resources[0]).storage,
    capacity: 5, // TODO: calculate capacity for each job based on the form
  }
  const first = role[0].toUpperCase()
  const rest = role.substring(1)
  DB.status[`idle${first + rest}s`].push(userID)
  // TODO: replace with: `findNextJob()`
  try_next_amendment(log) // see if enough providers for new contract
  // tryNextChallenge({ attestorID: userID }, log) // check for attestor only jobs (storage & perf challenge)
  emitEvent(`RegistrationSuccessful`, registration, log)
}
function verify_registerRoles (args) {
  // TODO: verify args
  return true
}
async function unregisterRoles (user, { name, nonce }, status, args) {
  args.forEach(role => {
    const first = role[0].toUpperCase()
    const rest = role.substring(1)
    const idleProviders = DB[`idle${first + rest}s`]
    for (var i = 0; i < idleProviders.length; i++) {
      const providerID = idleProviders[i]
      if (providerID === id) idleProviders.splice(i, 1)
    }
    const { id, [role]: { jobs, key, form } } = user
    const jobIDs = Object.keys(jobs)
    jobsIDs.map(jobID => {
      // TODO: user[role].jobs
      // => ...see what to do? find REPLACEMENT users?
      if (role === 'hoster') {
        const feedID = getContractByID(contractID).feed
        const contract = getContractByID(contractID)
        for (var i = 0, len = contract.activeHosters.length; i < len; i++) {
          const { hosterID, amendmentID } = contract.activeHosters[i]
          if (hosterID !== user.id) continue
          contract.activeHosters.splice(i, 1)
          removeJobForRolesXXXX({ providers: { hosters: [id] }, jobID: contractID }, log)
        }
      }
      else if (role === 'encoder') {}
      else if (role === 'attestor') {}
    })
    user[role] = void 0
  })
}
/*----------------------
  (UN)REGISTER HOSTER
------------------------*/
async function _registerHoster (user, { name, nonce }, status, args) {
  _registerRoles(user, { name, nonce }, status, ['hoster', ...args])
}
async function unregisterHoster (user, { name, nonce }, status) {
  unregisterRoles(user, { name, nonce }, status, ['hoster'])
}
/*----------------------
  (UN)REGISTER ENCODER
------------------------*/
async function _registerEncoder (user, { name, nonce }, status, args) {
  _registerRoles(user, { name, nonce }, status, ['encoder', ...args])
}
async function unregisterEncoder (user, { name, nonce }, status) {
  unregisterRoles(user, { name, nonce }, status, ['encoder'])
}
/*----------------------
  (UN)REGISTER ATTESTOR
------------------------*/
async function _registerAttestor (user, { name, nonce }, status, args) {
  _registerRoles(user, { name, nonce }, status, ['attestor', ...args])
}
async function unregisterAttestor (user, { name, nonce }, status) {
  unregisterRoles(user, { name, nonce }, status, ['attestor'])
}
/*----------------------
  AMENDMENT REPORT
------------------------*/
async function _amendmentReport (user, { name, nonce }, status, args) {
  const log = connections[name].log
  const [ report ] = args
  console.log({report})
  const { id: amendmentID, failed } = report // [2,6,8]
  const amendment = getAmendmentByID(amendmentID)
  const { providers: { hosters, attestors, encoders }, contract: contractID } = amendment
  const contract = getContractByID(contractID)
  const { status: { schedulerID }, plan: planID } = contract
  const plan = getPlanByID(planID)
  const [attestorID] = attestors
  if (user.id !== attestorID) return log({ type: 'chain', data: [`Error: this user can not submit the attestation`] })
  if (contract.amendments[contract.amendments.length - 1] !== amendmentID) return log({ type: 'chain', data: [`Error: this amendment has expired`] })
  // cancel amendment schedule
  const { scheduleAction, cancelAction } = await scheduler
  if (!schedulerID) console.log('No scheduler in', JSON.stringify(contract))
  cancelAction(schedulerID)

  const meta = [user, name, nonce, status]
  // ALL SUCCESS 
  if (!failed.length) {
    contract.activeHosters = hosters
    for (var i = 0, len = hosters.length; i < len; i++) {
      console.log(`Hosting started: contract: ${contractID}, amendment: ${amendmentID}, hoster: ${hosters[i]}`)
      const opts = { plan, hosterID: hosters[i], contractID, meta, log }
      scheduleChallenges(opts)
    }
    removeJobForRolesXXXX({ providers: { attestors, encoders }, jobID: amendmentID }, log)
    // => until HOSTING STARTED event, everyone keeps the data around
    emitEvent('HostingStarted', [amendmentID], log)
    return
  }
  // NOT ALL SUCCESS => new amendment
  const opts = { failed, providers, contractID, plan, meta, log }
  retryAmendment(opts)
}


/*----------------------
  STORAGE CHALLENGE
------------------------*/
async function _requestStorageChallenge ({ contractID, hosterID, meta, log }) {
  const { user, name, nonce, status } = meta
  const contract = getContractByID(contractID)
  const plan = getPlanByID(contract.plan)
  if (!plan.sponsor === user.id) return log({ type: 'chain', data: [`Error: this user can not call this function`] })
  var chunks = []
  getRandomChunks({ ranges: contract.ranges, chunks })
  const storageChallenge = { contract: contract.id, hoster: hosterID, chunks }
  const id = await addItem(storageChallenge)
  DB.active.storageChallenges[id] = true
  // find attestor
  const newJob = storageChallenge.id
  const type = 'NewStorageChallenge'
  const avoid = makeAvoid(plan)
  avoid[hosterID] = true
  const idleProviders = DB.status.idleAttestors
  const selectedProviders = select({ idleProviders, role: 'attestor', newJob, amount: 1, avoid, plan, log })
  const [attestorID] = selectedProviders
  if (!attestorID) return DB.queues.attestorsJobQueue.push({ fnName: 'NewStorageChallenge', opts: { storageChallenge } })
  storageChallenge.attestor = attestorID
  giveJobToRoles({ type, selectedProviders, idleProviders, role: 'attestor', newJob }, log)
  // emit event
  log({ type: 'chain', data: [type, newJob] })
  emitEvent(type, [newJob], log)
}

async function _submitStorageChallenge (user, { name, nonce }, status, args) {
  const log = connections[name].log
  const [ response ] = args
  log({ type: 'chain', data: [`Received StorageChallenge ${JSON.stringify(response)}`] })

  const { hashes, storageChallengeID, signature } = response  // signed storageChallengeID, signed by hoster

  // const { proof, storageChallengeID, hosterSignature } = response
  // const hash0 // challenged chunk
  // const proof = [hash0, hash1, hash2, hash3, hash4]
  // const parenthash = nodetype+sizeLeft+sizeRight+hashLeft+hashRight

  // @NOTE: sizes for any required proof hash is already on chain
  // @NOTE: `feed/:id/chunk/:v` // size

  const storageChallenge = getStorageChallengeByID(storageChallengeID)
  const attestorID = storageChallenge.attestor
  if (user.id !== attestorID) return log({ type: 'chain', data: [`Only the attestor can submit this storage challenge`] })
  // TODO validate proof
  const isValid = validateProof(hashes, signature, storageChallenge)
  var method = isValid ? 'StorageChallengeConfirmed' : 'StorageChallengeFailed'
  emitEvent(method, [storageChallengeID], log)
  // attestor finished job, add them to idleAttestors again

  removeJobForRoleYYYY({
    id: attestorID,
    role: 'attestor',
    doneJob: storageChallengeID,
    idleProviders: DB.status.idleAttestors,
    action: () => tryNextChallenge({ attestorID }, log)
  }, log)
}
/*----------------------
  PERFORMANCE CHALLENGE
------------------------*/
async function _requestPerformanceChallenge ({ user, signingData, status, args }) {
  const { name, nonce } = signingData
  const log = connections[name].log
  const [ contractID, hosterID ] = args
  const plan = getPlanByID(getContractByID(contractID).plan)
  makePerformanceChallenge({ contractID, hosterID, plan }, log)
}

async function _submitPerformanceChallenge (user, { name, nonce }, status, args) {
  const log = connections[name].log
  const [ performanceChallengeID, report ] = args
  const userID = user.id
  log({ type: 'chain', data: [`Performance Challenge proof by attestor: ${userID} for challenge: ${performanceChallengeID}`] })
  const performanceChallenge = getPerformanceChallengeByID(performanceChallengeID)
  if (!performanceChallenge.attestors.includes(userID)) return log({ type: 'chain', data: [`Only selected attestors can submit this performance challenge`] })
  var method = report ? 'PerformanceChallengeFailed' : 'PerformanceChallengeConfirmed'
  emitEvent(method, [performanceChallengeID], log)
  // attestor finished job, add them to idleAttestors again
  removeJobForRoleYYYY({
    id: userID,
    role: 'attestor',

    doneJob: performanceChallengeID,
    idleProviders: DB.status.idleAttestors,
    action: () => tryNextChallenge({ attestorID: userID }, log)
  }, log)
}

/******************************************************************************
  HELPERS
******************************************************************************/

const setSize = 10 // every contract is for hosting 1 set = 10 chunks
const size = setSize*64 //assuming each chunk is 64kb
const blockTime = 6000

async function publish_feed (feed, sponsor_id, log) {
  const [key, {hashType, children}, signature] = feed
  const keyBuf = Buffer.from(key, 'hex')
  // check if feed already exists
  if (DB.lookups.feedByKey[keyBuf.toString('hex')]) return
  feed = { publickey: keyBuf.toString('hex'), meta: { signature, hashType, children } }
  const feedID = await addItem(feed)
  DB.lookups.feedByKey[keyBuf.toString('hex')] = feedID
  feed.publisher = sponsor_id
  emitEvent('FeedPublished', [feedID], log)
  return feedID
}

async function publish_components (log, components, feed_ids) {
  const { dataset_items, performance_items, timetable_items, region_items } = components
  const dataset_ids = await Promise.all(dataset_items.map(async item => {
    if (item.feed_id < 0) item.feed_id = feed_ids[(Math.abs(item.feed_id) - 1)]
    return await addItem(item)
  }))
  const performances_ids = await Promise.all(performance_items.map(async item => await addItem(item)))
  const timetables_ids = await Promise.all(timetable_items.map(async item => await addItem(item)))
  const regions_ids = await Promise.all(region_items.map(async item => await addItem(item)))
  return { dataset_ids, performances_ids, timetables_ids, regions_ids }
} 
async function publish_form_components (components) {
  const {  timetable_items, region_items, performance_items, resource_items } = components
  const timetables_ids = await Promise.all(timetable_items.map(async item => await addItem(item)))
  const regions_ids = await Promise.all(region_items.map(async item => await addItem(item)))
  const performances_ids = await Promise.all(performance_items.map(async item => await addItem(item)))
  const resources_ids = await Promise.all(resource_items.map(async item => await addItem(item)))
  return { resources_ids, performances_ids, timetables_ids, regions_ids }
}
function handleNew (item, ids) {
  const keys = Object.keys(item)
  for (var i = 0, len = keys.length; i < len; i++) {
    const type = keys[i]
    item[type] = item[type].map(id => {
      if (id < 0) return ids[`${type}_ids`][(Math.abs(id) - 1)]
    })
  }
  return item
}

function getPrograms (plans) {
  const programs = []
  for (var i = 0; i < plans.length; i++) { programs.push(...plans[i].programs) }
  return programs
}

async function take_next_from_priority (next, log) {
  const plan = await getPlanByID(next.id)
  const contract_ids = await make_contracts(plan, log)
  plan.contracts.push(...contract_ids)
  for (var i = 0, len = contract_ids.length; i < len; i++) {
    const id = contract_ids[i]
    const blockNow = header.number
    const delay = plan.duration.from - blockNow
    const { scheduleAction } = await scheduler
    scheduleAction({ 
      action: () => {
        const reuse = { encoders: [], attestors: [], hosters: [] }
        const amendment_id = init_amendment(id, reuse, log)
        add_to_pending(amendment_id, log)
        try_next_amendment(log)
      }, 
      delay, name: 'schedulingAmendment' 
    })
  }
}

// split plan into sets with 10 chunks
async function make_contracts (plan, log) {
  const dataset_ids = plan.program.map(item => item.dataset).flat()
  const datasets = get_datasets(plan)
  for (var i = 0; i < datasets.length; i++) {
    const feed = getFeedByID(datasets[i].feed_id)
    const ranges = datasets[i].ranges
    // split ranges to sets (size = setSize)
    const sets = makeSets({ ranges, setSize })
    return Promise.all(sets.map(async set => {
      // const contractID = DB.contracts.length
      const contract = {
        plan: plan.id,
        feed: feed.id,
        ranges: set,
        amendments: [],
        activeHosters: [],
        status: {}
       }
      await addItem(contract)
      log({ type: 'chain', data: [`New Contract: ${JSON.stringify(contract)}`] })
      return contract.id 
    }))
  }
}
// find providers for each contract (+ new providers if selected ones fail)
async function init_amendment (contractID, reuse, log) {
  const contract = getContractByID(contractID)
  if (!contract) return log({ type: 'chain', data: [`No contract with this ID: ${contractID}`] })
  log({ type: 'chain', data: [`Searching additional providers for contract: ${contractID}`] })
  // const id = DB.amendments.length
  const amendment = { contract: contractID }
  // DB.amendments.push(amendment) // @NOTE: set id
  const id = await addItem(amendment)
  amendment.providers = reuse
  contract.amendments.push(id)
  return id
}
function add_to_pending (amendmentID, log) {
  DB.queues.pendingAmendments.push(amendmentID) // TODO sort pendingAmendments based on priority (RATIO!)
}
async function try_next_amendment (log) {
  // const failed = []
  for (var start = new Date(); DB.queues.pendingAmendments.length && new Date() - start < 4000;) {
    const id = await DB.queues.pendingAmendments[0]
    const x = await activate_amendment(id, log)
    if (!x) DB.queues.pendingAmendments.shift()
  }
  // failed.forEach(x => add_to_pending(x, log))
}
async function activate_amendment (id, log) {
  const amendment = getAmendmentByID(id)
  const contract = getContractByID(amendment.contract)
  const { plan: plan_id } = getContractByID(amendment.contract)

  const newJob = id
  const type = 'NewAmendment'
  const providers = getProviders(getPlanByID(plan_id), amendment.providers, newJob, log)
  if (!providers) {
    log({ type: 'chain', data: [`not enough providers available for this amendment`] })
    return { id }
  }
  // schedule follow up action
  contract.status.schedulerID = await scheduleAmendmentFollowUp(id, log)
  ;['attestor','encoder','hoster'].forEach(role => {
    const first = role[0].toUpperCase()
    const rest = role.substring(1)
    giveJobToRoles({
      type,
      selectedProviders: providers[`${role}s`],
      idleProviders: DB[`idle${first + rest}s`],
      role,
      newJob
    }, log)
  })
  const keys = Object.keys(providers)
  for (var i = 0, len = keys.length; i < len; i++) {
    providers[keys[i]] = providers[keys[i]].map(item => item.id)
  }
  log({ type: 'chain', data: [`Providers for amendment (${id}): ${JSON.stringify(providers)}`] })
  amendment.providers = providers
  // emit event
  console.log(`New event emitted`, type, newJob)
  log({ type: 'chain', data: [type, newJob] })
  emitEvent(type, [newJob], log)
}

function getProviders (plan, reused, newJob, log) {
  const attestorAmount = 1 - (reused?.attestors?.length || 0)
  const encoderAmount = 3 - (reused?.encoders?.length || 0)
  const hosterAmount = 3 - (reused?.hosters?.length || 0)
  const avoid = makeAvoid(plan)
  if (!reused) reused = { encoders: [], attestors: [], hosters: [] }
  else {
    const reusedArr = [...reused.attestors, ...reused.hosters, ...reused.encoders]
    reusedArr.forEach(id => avoid[id] = true)
  }
  // TODO backtracking!! try all the options before returning no providers available
  const attestors = select({ idleProviders: DB.status.idleAttestors, role: 'attestor', newJob, amount: attestorAmount, avoid, plan, log })
  if (!attestors.length) return log({ type: 'chain', data: [`missing attestors`] })
  const encoders = select({ idleProviders: DB.status.idleEncoders, role: 'encoder',  newJob, amount: encoderAmount, avoid, plan, log })
  if (!encoders.length === encoderAmount) return log({ type: 'chain', data: [`missing encoders`] })
  const hosters = select({ idleProviders: DB.status.idleHosters, role: 'hoster', newJob, amount: hosterAmount, avoid, plan, log })
  if (!hosters.length === hosterAmount) return log({ type: 'chain', data: [`missing hosters`] })
  return {
    encoders: [...encoders, ...reused.encoders],
    hosters: [...hosters, ...reused.hosters],
    attestors: [...attestors, ...reused.attestors]
  }
}
function getRandom (items) {
  if (!items.length) return
  const pos = Math.floor(Math.random() * items.length)
  const item = items[pos]
  return [item, pos]
}
function getRandomPos(ranges) {
  min = 0
  var max = 0
  for (var j = 0, N = ranges.length; j < N; j++) {
    const range = ranges[j]
    for (var i = range[0]; i <= range[1]; i++) max++
  }
  return Math.floor(Math.random() * (max - min)) + min; //The maximum is exclusive and the minimum is inclusive
}
function getRandomChunks ({ ranges, chunks }) { // [[0,3], [5,7]]
  var pos = getRandomPos(ranges)
  counter = 0
  for (var j = 0, N = ranges.length; j < N; j++) {
    const range = ranges[j]
    for (var i = range[0]; i <= range[1]; i++) {
      if (counter === pos) return chunks.push(i)
      counter++
    }
  }
}
function validateProof (hashes, signature, storageChallenge) {
  // const chunks = storageChallenge.chunks
  // if (`${chunks.length}` === `${hashes.length}`) return true
  return true
}
function select ({ idleProviders, role, newJob, amount, avoid, plan, log }) {
  idleProviders.sort(() => Math.random() - 0.5) // TODO: improve randomness
  const selectedProviders = []
  for (var i = 0; i < idleProviders.length; i++) {
    const id = idleProviders[i]
    if (avoid[id]) continue // if id is in avoid, don't select it
    const provider = getUserByID(id)
    if (doesQualify(plan, provider, role)) {
      selectedProviders.push({id, index: i, role })
      avoid[id] = true
      if (selectedProviders.length === amount) return selectedProviders
    }
  }
  return []
}
function giveJobToRoles ({ type, selectedProviders, idleProviders, role, newJob }, log) {
  // @NOTE: sortedProviders makes sure those with highest index get sliced first
  // so lower indexes are unchanged until they get sliced too
  const sortedProviders = selectedProviders.sort((a,b) => a.index > b.index ? 1 : -1)
  const providers = sortedProviders.map(({ id, index, role }) => {
    const provider = getUserByID(id)
    provider[role].jobs[newJob] = true
    if (!hasCapacity(provider, role)) idleProviders.splice(index, 1)
    // TODO currently we reduce idleStorage for all providers
    // and for all jobs (also challenge)
    // => take care that it is INCREASED again when job is done
    provider[role].idleStorage -= size
    return id
  })
  // returns array of selected providers for select function
  return providers
}


function getJobByID (jobID) {
  return getItem(jobID)
}
// TODO payments: for each successfull hosting we pay attestor(1/3), this hoster (full), encoders (full, but just once)
async function removeJob ({ providers, jobID }, log) {
  const job = await getJobByID(jobID)
  const types = Object.keys(provider)
  for (var i = 0, ilen = types.length; i < len; i++) {
    const roles = types[i]//.slice(0, -1)
    const peerIDs = providers[roles]
    for (var k = 0, klen = peerIDs.length; k < klen; k++) {
      const id = peerIDs[k]

    }
  }
}

function removeJobForRolesXXXX ({ providers, jobID }, log) {
  const { hosters = [], attestors = [], encoders = [] } = providers
  hosters.forEach((hosterID, i) => {
    removeJobForRoleYYYY({
      id: hosterID,
      role: 'hoster',
      doneJob: jobID,
      idleProviders: DB.status.idleHosters,
      action: () => try_next_amendment(log)
    }, log)
  })
  encoders.map(id =>
    removeJobForRoleYYYY({
      id,
      role: 'encoder',
      doneJob: jobID,
      idleProviders: DB.status.idleEncoders,
      action: () => try_next_amendment(log)
    }, log))
  attestors.map(id =>
    removeJobForRoleYYYY({
      id,
      role: 'attestor',
      doneJob: jobID,
      idleProviders: DB.status.idleAttestors,
      action: () => try_next_amendment(log)
    }, log))
}
function removeJobForRoleYYYY ({ id, role, doneJob, idleProviders, action }, log) {
  const provider = getUserByID(id)
  if (provider[role].jobs[doneJob]) {
    log({ type: 'chain', data: [`Removing the job ${doneJob}`] })
    delete provider[role].jobs[doneJob]
    if (!idleProviders.includes(id)) idleProviders.push(id)
    provider[role].idleStorage += size
    action()
  }
}
function doesQualify (plan, provider, role) {
  const form = provider[role].form
  if (
    isScheduleCompatible(plan, form, role) &&
    hasCapacity(provider, role) &&
    hasEnoughStorage(role, provider)
  ) return true
}
async function isScheduleCompatible (plan, form, role) {
  const blockNow = header.number
  const isAvialableNow = form.duration.from <= blockNow
  const until = form.duration.until
  var jobDuration
  if (role === 'attestor') jobDuration = 3
  if (role === 'encoder') jobDuration = 2 // duration in blocks
  if (role === 'hoster') jobDuration = plan.duration.until -  blockNow
  return (isAvialableNow && (until >= (blockNow + jobDuration) || isOpenEnded))
}
function hasCapacity (provider, role) {
  const jobs = provider[role].jobs
  return (Object.keys(jobs).length < provider[role].capacity)
}
function hasEnoughStorage (role, provider) {
  return (provider[role].idleStorage > size)
}
function tryNextChallenge ({ attestorID }, log) {
  if (DB.queues.attestorsJobQueue.length) {
    const next = DB.queues.attestorsJobQueue[0]
    if (next.fnName === 'NewStorageChallenge' && DB.status.idleAttestors.length) {
      const storageChallenge = next.opts.storageChallenge
      const hosterID = storageChallenge.hoster
      const contract = getContractByID(storageChallenge.contract)
      const plan = getPlanByID(contract.plan)
      const avoid = makeAvoid(plan)
      avoid[hosterID] = true

      const newJob = storageChallenge.id
      const type = 'NewStorageChallenge'
      const idleProviders = DB.status.idleAttestors
      const selectedProviders = select({ idleProviders, role: 'attestor', newJob, amount: 1, avoid, plan, log })
      const [attestorID] = selectedProviders
      if (selectedProviders.length) {
        DB.queues.attestorsJobQueue.shift()
        storageChallenge.attestor = attestorID
        giveJobToRoles({ type, selectedProviders, idleProviders, role: 'attestor', newJob }, log)
      }
      // emit event
      log({ type: 'chain', data: [type, newJob] })
      emitEvent(type, [newJob], log)
    }
    if (next.fnName === 'NewPerformanceChallenge' && DB.status.idleAttestors.length >= 5) {
      const performanceChallenge = next.opts.performanceChallenge
      const hosterID = performanceChallenge.hoster
      const contract = getContractByID(performanceChallenge.contract)
      const plan = getPlanByID(contract.plan)
      const avoid = makeAvoid(plan)
      avoid[hosterID] = true

      const newJob = performanceChallenge.id
      const type = 'NewPerformanceChallenge'
      const attestors = select({ idleProviders: DB.status.idleAttestors, role: 'attestor', newJob, amount: 5, avoid, plan, log })
      if (attestors.length) {
        DB.queues.attestorsJobQueue.shift()
        performanceChallenge.attestors = attestors
        giveJobToRoles({
          type,
          selectedProviders: attestors,
          idleProviders: DB.status.idleAttestors,
          role: 'attestor',
          newJob
        }, log)
        // emit event
        log({ type: 'chain', data: [type, newJob] })
        emitEvent(type, [newJob], log)
      }
    }
  }
}
function makeAvoid (plan) {
  const avoid = {}
  avoid[plan.sponsor] = true // avoid[3] = true
  const datasets = get_datasets(plan)
  const feed_ids = [...new Set(datasets.map(dataset => dataset.feed_id))]
  feed_ids.forEach(id => {
    const feed = getFeedByID(id)
    avoid[feed.publisher] = true
  })
  return avoid
}
function cancelContracts (plan) {
  const contracts = plan.contracts
  for (var i = 0; i < contracts.length; i++) {
    const contractID = contracts[i]
    const contract = getContractByID(contractID)
    // tell hosters to stop hosting
    // TODO:
    // 1. figure out all active Hostings (=contracts) from plan (= active)
    // 2. figure out all WIP PerfChallenges for contracts from plan
    // 3. figure out all WIP StoreChallenges for contracts from plan
    // 4. figure out all WIP makeHosting (=amendments) from plan (= soon to become active)
    // 5. CHAIN ONLY: figure out all future scheduled makeHostings (=amendments) from plan

// for every hoster in last Amendment user.hoster.jobs[`NewAmendment${amendmentID}`] = false
// for every encoder in last  user.encoder.jobs[`NewAmendment${amendmentID}`] = false
// for every attestor in last  user.attestor.jobs[`NewAmendment${amendmentID}`] = false
// contract.activeHosters = []
// cancel scheduled challenges
// plan.contracts = [] => we need to rename to activeContracts
// add checks in extrinsics for when wip actions (make hostings, challenges) report back to chain =>
//     storageChallengeID
// if (DB.active.storageChallenges[id] ) const challenge = getStorageChallengeByID(storageChallengeID)

    const queue = priorityQueue(function compare (a, b) { return a.id < b.id ? -1 : 1 })
    // queue.size()
    // queue.add(item) // add item at correct position into queue
    // queue.take(index=0) // get front item and remove it from the queue
    // queue.peek(index=0) // check front item
    // queue.drop(function keep (x) { return item.contract !== id })


    contract.activeHosters.forEach((hosterID, i) => {
      removeJobForRolesXXXX({ providers: { hosters: [hosterID] }, jobID: contractID }, log)
      const { feed: feedID } = getContractByID(contractID)
      // TODO ACTION find new provider for the contract (makeAmendment(reuse))
      // emit event to notify hoster(s) to stop hosting
      emitEvent('DropHosting', [feedID, hosterID], log)
    })
    contract.activeHosters = []
    // remove from jobs queue
    for (var j = 0; j < DB.queues.pendingAmendments; j++) {
      const id = DB.queues.pendingAmendments[j]
      const amendment = getAmendmentByID(id)
      if (contractID === amendment.contract) DB.queues.pendingAmendments.splice(j, 1)
    }
  }
}
async function scheduleChallenges (opts) {
  const { plan, hosterID, contractID, meta: [user, name, nonce, status], log } = opts
  console.log(`-----Starting Challenge Phase for contract: ${contractID}, hoster: ${hosterID}`)
  log({ type: 'chain', data: [`Starting Challenge Phase for contract: ${contractID}, hoster: ${hosterID}`] })
  const schedulingChallenges = async () => {
    // schedule new challenges ONLY while the contract is active (plan.duration.until > new Date())
    const until = plan.duration.until
    const blockNow = header.number
    if (!(until > blockNow)) return
    // TODO if (!plan.schedules.length) {}
    // else {} // plan schedules based on plan.schedules
    const planID = plan.id
    const from = plan.duration.from
    // TODO sort challenge request jobs based on priority (RATIO!) of the sponsors
    _requestStorageChallenge({ contractID, hosterID, meta: { user, name, nonce, status }, log })
    // _requestPerformanceChallenge({ user, signingData: { name, nonce }, status, args: [contractID, hosterID] })
    scheduleAction({ action: schedulingChallenges, delay: 5, name: 'schedulingChallenges' })
  }
  const { scheduleAction, cancelAction } = await scheduler
  console.log(scheduleAction)
  scheduleAction({ action: schedulingChallenges, delay: 1, name: 'schedulingChallenges' })
}

async function scheduleAmendmentFollowUp (id, log) {
  const scheduling = () => {
    console.log('This is a scheduled amendment follow up for amendment ', id)
    // TODO get all necessary data to call this exstrinsic from the chain
    // const { providers: { attestors } } = getAmendmentByID(id)
    // const report = [id, attestors]
    // const [attestorID] = attestors
    // const user = getUserByID(attestorID)
    // amendmentReport(user, { name, nonce }, status, [report])

    // console.log('scheduleAmendmentFollowUp', sid)
    // const contract = getContractByID(contractID)
    // // if (contract.activeHosters.length >= 3) return
    //
    // removeJobForRolesXXXX({ failedHosters: [], amendment, doneJob: `NewAmendment${id}` }, log)
    // // TODO update reuse
    // // const reuse = { attestors: [], encoders, hosters }
    // const reuse = { attestors: [], encoders: [], hosters: [] }
    // const newID = init_amendment(contractID, reuse, log)
    // add_to_pending(newID, log)
    // return id
  }
  const { scheduleAction, cancelAction } = await scheduler
  var sid = scheduleAction({ action: scheduling, delay: 10, name: 'scheduleAmendmentFollowUp' })
  return sid
}

async function planValid ({ plan }) {
  const blockNow = header.number
  const { duration: { from, until } } = plan
  if ((until > from) && ( until > blockNow)) return true
}

async function retryAmendment (opts) {
  const { failed, providers, contractID, plan, meta, log } = opts
  var reuse
  const [peerID] = failed
  const { hosters, attestors, encoders } = providers

  if (attestors.includes(peerID)) {
    // if failed is attestor (report was automatically triggered by amendmentFollowUp)
    const successfulAttestors = attestors.filter(id => !failed.includes(id))
    reuse = { hosters, encoders, attestors: successfulAttestors }
  }
  else if (hosters.includes(peerID)) {
    // else if any of the failed users is a hoster, we know all others did their job and can be reused
    const successfulHosters = hosters.filter(id => !failed.includes(id))
    contract.activeHosters = [...contract.activeHosters, ...successfulHosters]
    for (var i = 0, len = successfulHosters.length; i < len; i++) {
      console.log(`Hosting started: contract: ${contractID}, amendment: ${amendmentID}, hoster: ${successfulHosters[i]}`)
      const data = { plan, hosterID: successfulHosters[i], contractID, meta, log }
      scheduleChallenges(data)
    }
    reuse = { hosters: successfulHosters, encoders, attestors }
  } else if (encoders.includes(peerID)) {
    // if any of the encoders failed, we know attestor couldn't compare the encoded chunks and couldn't send them to hosters
    // we know all hosters are good, they can be reused
    const successfulEncoders = encoders.filter(id => !failed.includes(id))
    reuse = { hosters, encoders: successfulEncoders, attestors }

  }
  // remove jobs from providers
  removeJobForRolesXXXX({ providers: amendment.providers, jobID: amendmentID }, log)
  // TODO: ... who should drop jobs when??? ...
  // => emit Event to STOP JOB for EVERYONE who FAILED
  emitEvent('DropJob', [amendmentID, failed], log)
  // TODO: add new amendment to contract only after it is taken from the queue
  // TODO: make amendments small (diffs) and show latest summary of all amendments under contract.activeHosters
  
  // make new amendment
  console.log({reuse})
  const newID = await init_amendment(contractID, reuse, log)
  // TODO ACTION find new provider for the contract (makeAmendment(reuse))
  add_to_pending(newID, log)
  try_next_amendment(log)
}

async function makeStorageChallenge({ contract, hosterID, plan }, log) {

}
async function makePerformanceChallenge ({ contractID, hosterID, plan }, log) {
  // const id = DB.performanceChallenge.length
  const performanceChallenge = { contract: contractID, hoster: hosterID }
  // DB.performanceChallenges.push(performanceChallenge) // @NOTE: set id
  const id = await addItem(performanceChallenge)
  DB.active.performanceChallenges[id] = true
  // select attestors
  const avoid = makeAvoid(plan)
  avoid[hosterID] = true

  const newJob = performanceChallenge.id
  const type = 'NewPerformanceChallenge'
  const attestors = select({ idleProviders: DB.status.idleAttestors, role: 'attestor', newJob, amount: 5, avoid, plan, log })
  if (!attestors.length) return DB.queues.attestorsJobQueue.push({ fnName: 'NewPerformanceChallenge', opts: { performanceChallenge } })
  performanceChallenge.attestors = attestors
  giveJobToRoles({
    type,
    selectedProviders: attestors,
    idleProviders: DB.status.idleAttestors,
    role: 'attestor',
    newJob
  }, log)
  // emit event
  log({ type: 'chain', data: [type, newJob] })
  emitEvent(type, [newJob], log)
}

function isValidHoster ({ hosters, failedHosters, hosterID }) {
  // is hoster listed in the amendment for hosting and is hoster not listed as failed (by the attestor)
  if (!hosters.includes(hosterID) || failedHosters.includes(hosterID)) return log({ type: 'chain', data: [`Error: this user can not call this function`] })
  return true
}

function emitEvent (method, data, log) {
  const message = [{ event: { data, method } }]
  handlers.forEach(([name, handler]) => handler(message))
  log({ type: 'chain', data: [`emit chain event ${JSON.stringify(message)}`] })
}

function get_datasets (plan) {
  const dataset_ids = plan.program.map(item => item.dataset).flat()
  return dataset_ids.map(id => getDatasetByID(id))
}