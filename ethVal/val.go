package main

import (
	"fmt"
	"os"
	"time"

	"github.com/ethereum/go-ethereum/consensus/ethash"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/ethLogger"
)

var (
	upNum       = 18997155
	endNum      = upNum + 5000
	dbPath      = "/home/ubuntu/eth_data/geth/chaindata"
	ancientPath = dbPath + "/ancient/chain"
	// upNum       = 17034770
	// endNum      = 17034770
	// dbPath      = "/home/user/eth/eth_data/geth/chaindata"
	// ancientPath = dbPath + "/ancient/chain"
	//upNum       = 16098649
	//endNum      = 16098650
	//dbPath      = "/home/rfxiong/data/ethdata/geth/chaindata"
	//ancientPath = dbPath + "/ancient"
)

func main() {
	ancientDb, err := rawdb.NewLevelDBDatabaseWithFreezer(dbPath, 16, 1, ancientPath, "", true)
	if err != nil {
		panic(err)
	}

	logger := ethLogger.NewZeroLogger()
	// db := rawdb.NewMemoryDatabase()
	logger.Infof("the state of the blockNumber %v", upNum-1)
	bc, _ := core.NewBlockChain(ancientDb, nil, core.DefaultGenesisBlock().Config, ethash.NewFaker(), vm.Config{}, nil, nil)
	logger.Infof("the state of the blockNumber %v", upNum-1)

	// for i := upNum; i >= endNum; i-- {
	// 	bh := rawdb.ReadCanonicalHash(ancientDb, uint64(i))
	// 	b := rawdb.ReadBlock(ancientDb, bh, uint64(i))
	// 	db := state.NewDatabase(ancientDb)
	// 	_, err := state.New(b.Root(), db, nil)
	// 	if err != nil {
	// 		continue
	// 	} else {
	// 		fmt.Println(b.Number())
	// 		return
	// 	}
	// }
	// fmt.Println("-------------------- not find state --------------------")
	file, err := os.OpenFile("execution.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND|os.O_TRUNC, 0666)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	preblkHash := rawdb.ReadCanonicalHash(ancientDb, uint64(upNum-1))
	preblock := rawdb.ReadBlock(ancientDb, preblkHash, uint64(upNum-1))
	sdb := state.NewDatabase(ancientDb)
	statedb, err := state.New(preblock.Root(), sdb, nil)
	if err != nil {
		panic(err)
	}

	var sum *time.Duration = new(time.Duration)
	var analyzeSum *time.Duration = new(time.Duration)
	//var chainSum int

	// logger.Infof("the statedb OriginRoot is %+v", statedb.OriginalRoot)
	// logger.Infof("the statedb is %+v", statedb)
	// for i := upNum; i <= endNum; i++ {
	// 	blkHash := rawdb.ReadCanonicalHash(ancientDb, uint64(i))
	// 	block := rawdb.ReadBlock(ancientDb, blkHash, uint64(i))
	// 	blkBody := rawdb.ReadBody(ancientDb, blkHash, uint64(i))
	// 	logger.Infof("etherscan url: https://etherscan.io/block/%v", i)
	// 	logger.Infof("the block.transactions is %d", len(blkBody.Transactions))
	// 	// if len(block.Transactions()) > 50 {
	// 	//bc.Processor().Process(block, statedb, vm.Config{})
	// 	txSum += len(block.Transactions())
	// }
	// logger.Infof("the txsum : %d", txSum)
	// blkHash := rawdb.ReadCanonicalHash(ancientDb, uint64(endNum))
	// block := rawdb.ReadBlock(ancientDb, blkHash, uint64(endNum))
	// logger.Infof("warm up block: %v", block)
	// return

	// warm up
	txSum := 0
	//maxSum := 0
	loadTime := new(time.Duration)
	blockList := make([]*types.Block, endNum-upNum)
	for i := upNum; i < endNum; i++ {
		blkHash := rawdb.ReadCanonicalHash(ancientDb, uint64(i))
		block := rawdb.ReadBlock(ancientDb, blkHash, uint64(i))
		blockList[i-upNum] = block
		//blkBody := rawdb.ReadBody(ancientDB, blkHash, uint64(i))
		if len(block.Transactions()) > 0 {
			logger.Infof("warm up block number: %v", i)
			bc.Processor().ProcessSerial(block, statedb, vm.Config{}, loadTime)
			// if maxSum < len(block.Transactions()) {
			// 	maxSum = len(block.Transactions())
			// }
			txSum += len(block.Transactions())
		}
	}
	copyStateDB := make([]*state.StateDB, 16)
	currState := statedb.Copy()

	for j := 0; j < 100; j++ {
		for i := 0; i < 16; i++ {
			copyStateDB[i] = currState.Copy()
		}
		for i := upNum; i < endNum; i++ {
			block := blockList[i-upNum]
			//blkBody := rawdb.ReadBody(ancientDb, blkHash, uint64(i))
			logger.Infof("etherscan url: https://etherscan.io/block/%v", i)
			//logger.Infof("the block.transactions is %d", len(blkBody.Transactions))
			// if len(block.Transactions()) > 50 {
			//bc.Processor().Process(block, statedb, vm.Config{})
			// bc.Processor().ProcesswithDag(block, statedb, vm.Config{}, sum2)
			//bc.Processor().ReplayAndReexecute(block, copyStateDB, statedb, vm.Config{}, sum)
			bc.Processor().ReplayImproved(block, statedb, copyStateDB, vm.Config{}, analyzeSum, sum)
			// txChainLen, _, _ := bc.Processor().ProcessWithDeps(block, statedb, vm.Config{}, sum3, analyzeSum)
			//bc.Processor().Replay(block, statedb, copyStateDB, vm.Config{}, sum)
			//bc.Processor().ProcessOcc(block, statedb, vm.Config{}, sum4)
			txSum += len(block.Transactions())
			// chainSum += txChainLen
		}
		//logger.Infof("%d trie root is %+v", j, statedb.OriginalRoot)
		file.Write([]byte(fmt.Sprintf("%d trie root is %+v\n", j, statedb.OriginalRoot)))
	}

	// logger.Infof("load time is %+v", sum)
	// logger.Infof("the serial time is %+v", sum1)
	// logger.Infof("the parallel without reorder time is %+v", sum2)
	// logger.Infof("the analyze time is %+v, the DAG execution time is %+v", analyzeSum, sum3)
	// logger.Infof("the DeOCC execution time is %+v", sum4)
	// logger.Infof("the txsum : %d, the chainsum : %d", txSum, chainSum)
	//logger.Infof("the txsum : %d, the DAG execution time with reexecution is %+v", txSum, sum4)
	//logger.Infof("the total re-execution rate is %v\n", rate)

}
