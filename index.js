const env = require('dotenv').config()
const Web3 = require('web3')
const ETH_NODE = process.env.PARITY_URL || process.env.INFURA_URL
const web3 = new Web3(new Web3.providers.HttpProvider(ETH_NODE))
const DEX_CONTRACT = new web3.eth.Contract(JSON.parse(process.env.DEX_ABI), process.env.DEX_ADDRESS) 
const BIRTH_BLOCK = process.env.BIRTH_BLOCK || 1
const BATCH_SIZE = process.env.BATCH_SIZE || 100
let FROM_BLOCK, TO_BLOCK; 

const pkg = require('./package.json')
const { Exporter } = require('san-exporter')
const exporter = new Exporter(pkg.name)

async function pushData() {
    // Calculate the block range to check
    // Uses batching of 100 blocks by default so we don't load too many events at once
    await calculateEndingBlock()
    const events = await getEvents();
    await asyncForEach(events, async (event) => {
        //console.log(event)
        await exporter.sendData({
            iso_date: new Date().toISOString(),
            dex_address: event.address,
            blockHash: event.blockHash,
            blockNumber: event.blockNumber,
            transactionHash: event.transactionHash,
            event: event.event,
            token: event.returnValues.token,
            user: event.returnValues.user,
            amount: event.returnValues.amount,
            balance: event.returnValues.balance
        })
    })

    FROM_BLOCK = TO_BLOCK;
    await exporter.savePosition(TO_BLOCK)
    setTimeout(pushData, 60000)
}

async function getEvents() {
    return DEX_CONTRACT.getPastEvents('allEvents',{
        fromBlock: FROM_BLOCK,
        toBlock: TO_BLOCK
    });
}

async function getBlockNumber() {
    return web3.eth.getBlockNumber()
}

async function calculateEndingBlock() {
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
}

async function asyncForEach(array, callback) {
    for (let index = 0; index < array.length; index++) {
        await callback(array[index], index, array);
    }
}

process.on('uncaughtException', function(err) {
    console.log('Caught exception: ' + err);
});

async function work() {
    // Start here
    // Get the latest block we checked for events from the exporter 
    // (null if we run for the first time)
    // This runs only once on start: then we keep latest block in memory
    await exporter.connect()
    FROM_BLOCK = await exporter.getLastPosition()
    await pushData()
}

work()