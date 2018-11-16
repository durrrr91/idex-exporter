const env = require('dotenv').config()
const Web3 = require('web3')
// const ETH_NODE = process.env.PARITY_URL || process.env.INFURA_URL
// const web3 = new Web3(new Web3.providers.HttpProvider(ETH_NODE))
const ETH_SOCKET = process.env.PARITY_WS || process.env.INFURA_WS
const web3 = new Web3(new Web3.providers.WebsocketProvider(ETH_SOCKET))
const DEX_CONTRACT = new web3.eth.Contract(JSON.parse(process.env.DEX_ABI), process.env.DEX_ADDRESS) 
const BIRTH_BLOCK = parseInt(process.env.BIRTH_BLOCK) || 1
const BATCH_SIZE = parseInt(process.env.BATCH_SIZE) || 500
let FROM_BLOCK, TO_BLOCK; 

const pkg = require('./package.json')
const { Exporter } = require('san-exporter')
const exporter = new Exporter(pkg.name)

async function pushData() {
    // Calculate the block range to check
    // Uses batching of BATCH_SIZE blocks by default so we don't load too many events at once
    await calculateBlockRange()
    const events = await getEvents();
    await asyncForEach(events, async (event) => {
        console.log(event.event + " - " + event.transactionHash)
        // event.returnValues contains user address, ERC20 token address and amount exchanged
        await exporter.sendData({
            iso_date: new Date().toISOString(),
            dex_address: event.address,
            blockHash: event.blockHash,
            blockNumber: event.blockNumber,
            transactionHash: event.transactionHash,
            event: event.event,
            returnValues: event.returnValues
        })
    })

    FROM_BLOCK = TO_BLOCK + 1;
    await exporter.savePosition(FROM_BLOCK)
    setTimeout(pushData, 3000)
}

async function getEvents() {
    if(FROM_BLOCK > TO_BLOCK) {
        // This only happens when we have checked the current chain tip already
        // FROM_BLOCK is set to chain tip + 1 as to avoid recheking the latest block multiple times
        console.log("No need to check yet, waiting for new block");
        return [];
    }
    console.log("Getting events from block " + FROM_BLOCK + " to block " + TO_BLOCK)
    return DEX_CONTRACT.getPastEvents('allEvents',{
        fromBlock: FROM_BLOCK,
        toBlock: TO_BLOCK
    });
}

async function getBlockNumber() {
    return web3.eth.getBlockNumber()
}

async function calculateBlockRange() {
    console.log("Calculating block range");
    // BIRTH_BLOCK is the block height when the DEX was created (no need to check prior blocks)
    if(FROM_BLOCK === null || FROM_BLOCK < BIRTH_BLOCK) {
        FROM_BLOCK = BIRTH_BLOCK
        TO_BLOCK = FROM_BLOCK + BATCH_SIZE
    }else {
        let blockNumber = await getBlockNumber()
        if(blockNumber - FROM_BLOCK >= BATCH_SIZE) {
            TO_BLOCK = FROM_BLOCK + BATCH_SIZE
        }else {
            TO_BLOCK = blockNumber
        }
    }
    console.log("From: "+FROM_BLOCK)
    console.log("To: "+TO_BLOCK)
}

async function asyncForEach(array, callback) {
    for (let index = 0; index < array.length; index++) {
        await callback(array[index], index, array);
    }
}

process.on('uncaughtException', function(err) {
    console.log('Caught exception: ' + err)
});

web3._provider.on('end', function(eventObj) { 
    console.log('Web socket disconnected. Trying to reconnect? ')
});

async function work() {
    // Start here
    // Get the latest block we checked for events from the exporter 
    // (null if we run for the first time)
    // This runs only once on start: then we keep latest block in memory
    await exporter.connect()
    FROM_BLOCK = await exporter.getLastPosition()
    // FROM_BLOCK = 6715149 <-- Start from a recent block to test the chain tip case without syncing
    await pushData()
}

work()