import fs from "fs";
import Web3 from "web3";

import abiDecoder from "abi-decoder";
import IUniswapV2Pair from "./ABIs/IUniswapV2Pair.js";
import IUniswapV2PairAbi from "./ABIs/IUniswapV2PairAbi.js";

import { createObjectCsvWriter } from 'csv-writer';

abiDecoder.addABI(IUniswapV2PairAbi);

const web3 = new Web3('https://arb1.arbitrum.io/rpc');

const start = '2023-09-01'
const end = '2023-10-01'

const csvFilePath = `txs-${start}.csv`;
const isCSVExist = fs.existsSync(csvFilePath);

const txsWriter = createObjectCsvWriter({
    path: csvFilePath,
    header: [
        { id: 'txhash', title: 'TX Hash' },
        { id: 'txindex', title: 'TX Index' },
        { id: 'block', title: 'Block Number' },
        { id: 'gasused', title: 'Gas Used' },
        { id: 'timestamp', title: 'Timestamp' },
        { id: 'pool', title: 'Pool address' },
        { id: 'token0', title: 'Token 0' },
        { id: 'token1', title: 'Token 1' }
    ],
	append: isCSVExist
});

async function init() {
    try {
        console.log("Init searching...");

        const startDate = new Date(start);
        startDate.setUTCHours(0, 0, 0, 0);
        const startTimestamp = Math.floor(startDate.getTime() / 1000);

        const endDate = new Date(end);
        endDate.setUTCHours(23, 59, 59, 999);
        const endTimestamp = Math.floor(endDate.getTime() / 1000);

        const startBlockNumber = parseInt(await get_blockNumber(startTimestamp));
        const endBlockNumber = parseInt(await get_blockNumber(endTimestamp));

        console.log("From", startBlockNumber, "to", endBlockNumber);

        let fromBlock = startBlockNumber;
		let id = 0;

        while (fromBlock < endBlockNumber) {
            let toBlock = fromBlock + 200000;
            if (toBlock > endBlockNumber) {
                toBlock = endBlockNumber;
            }

            fromBlock = await processRange(id++, fromBlock, toBlock) + 1;
        }
    } catch(error) {
        console.log(error);
    }
}

async function processRange(id, from, to) {
    try {
        const params = { topics: ['0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822'], fromBlock: from, toBlock: to, }
        const respOfLogs = await web3.eth.getPastLogs(params);
		
		let promises = [];
		let ticks = 0;
		
		console.log(id, "Searching from", from, "to", to, "Amount:", respOfLogs.length);

        for(let l = 0; l < respOfLogs.length;) {
			const txHash = respOfLogs[l].transactionHash;
			
			if(promises.length < 10) {
				l++;
				
				promises.push(processTx(txHash));
			} else {
				try {
					const datas = await Promise.all(promises);

					const filteredDatas = datas.filter(data => data !== null && data.txhash !== null);
					if (filteredDatas.length > 0) await txsWriter.writeRecords(filteredDatas);
									
					if(ticks++ > 5) {
						const c = l + 1;
						const progress = (c / respOfLogs.length) * 100;
						console.log(id, "Checkeds:", c, "/", respOfLogs.length, "Progress:", progress.toFixed(2), "%");
						
						ticks = 0;
					}
					
					promises = [];
					
					await paused(500);
				} catch(error) {
					continue;
				}
			}
        }

        return to;
    } catch(error) {
        await paused(3000);

        return await processRange(id, from, to - 10000);
    }
}

async function processTx(txHash) {
	try {
		const txReceipt = await web3.eth.getTransactionReceipt(txHash);
		if (!txReceipt || txReceipt.status == 0) return null;

		let decodedLogs = [];
		try {
			decodedLogs = abiDecoder.decodeLogs(txReceipt.logs);
		} catch (e) {
			return null;
		}

		if(!decodedLogs) return null;

		const txIndex = txReceipt.transactionIndex
		const block = txReceipt.blockNumber;
		const gasused = txReceipt.gasUsed;
		const timestamp = "0";
		const [pool, token0, token1] = await get_decoded(decodedLogs);

		if(pool == null || token0 == null || token1 == null) return null;

		return { txhash: txHash, txindex: txIndex, block: block, gasused: gasused, timestamp: timestamp, pool: pool, token0: token0, token1: token1 };
	} catch(error) {
        console.log(error);

		await paused(1000);
		
		return processTx(txHash);
	}
}

async function get_decoded(decodedLogs) {
    let pool, token0, token1;

    for (let decodedLogIt = 0; decodedLogIt < decodedLogs.length; ++decodedLogIt) {
        const decodedLog = decodedLogs[decodedLogIt];

        if (decodedLog.name == "Swap") {
            pool = decodedLog.address;

            try {
                const pairContract = new web3.eth.Contract(IUniswapV2Pair, pool);
                token0 = await pairContract.methods.token0().call();
                token1 = await pairContract.methods.token1().call();
            } catch (error) {
                if(error.innerError && error.innerError.message === 'Too Many Requests') {
					await paused(1000)
					
					return await get_decoded(decodedLogs);
				};
                return [null, null, null];
            }
            break;
        }
    }

    return [pool, token0, token1];
}

async function get_blockNumber(timestamp) {
    const url = `https://api.arbiscan.io/api?module=block&action=getblocknobytime&timestamp=${timestamp}&closest=before&apikey=K1XETFG5ZMKT8WS7A9QS5XVASVJC1V4KQ4`;

    try {
        const response = await fetch(url, { timeout: 10000 });
        const data = await response.json();

        if (data.status === 0) return null;

        return data.result;
    } catch (error) {
        console.error("Error getting data from block:", error);
        return null;
    }
}

function paused(time) {
    return new Promise(resolve => setTimeout(resolve, time));
}

init();