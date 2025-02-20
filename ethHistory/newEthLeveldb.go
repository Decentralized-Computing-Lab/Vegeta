package main

import (
	//"fmt"

	"flag"
	"fmt"
	"os"
	"time"

	"github.com/ethereum/go-ethereum/consensus/ethash"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/ethLogger"
)

var (
	upNum       = 11735922 + 1
	dbPath      = "/home/ubuntu/ethdata/geth/chaindata"
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
	Gap := flag.Int("gap", 0, "the gap of state used to speculate and execute")
	blockNum := flag.Int("blockNum", 200, "number of blocks to run")
	flag.Parse()
	endNum := upNum + *blockNum
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
	spe_statedb, err := state.New(preblock.Root(), sdb, nil)
	if err != nil {
		panic(err)
	}
	var sum *time.Duration = new(time.Duration)
	var sum_0 *time.Duration = new(time.Duration)
	var sum1 *time.Duration = new(time.Duration)
	var sum2 *time.Duration = new(time.Duration)
	var sum_serial *time.Duration = new(time.Duration)
	// var analyzeSum *time.Duration = new(time.Duration)

	var txSum int
	// var chainSum int
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
	// return
	var s float64 = 0
	var rate float64 = 0
	exe_statedb := spe_statedb.Copy()
	gap := *Gap
	for i := 0; i < gap; i++ {
		blkHash := rawdb.ReadCanonicalHash(ancientDb, uint64(i+upNum))
		block := rawdb.ReadBlock(ancientDb, blkHash, uint64(i+upNum))
		blkBody := rawdb.ReadBody(ancientDb, blkHash, uint64(i+upNum))
		logger.Infof("etherscan url: https://etherscan.io/block/%v", i+upNum)
		logger.Infof("the block.transactions is %d", len(blkBody.Transactions))
		bc.Processor().ProcessSerial(block, exe_statedb, vm.Config{}, sum_0)
	}
	file.Write([]byte(fmt.Sprintf("pre_lod time %+v\n\n", sum_0)))

	for i := upNum; i <= endNum; i++ {
		spe_blkHash := rawdb.ReadCanonicalHash(ancientDb, uint64(i))
		spe_block := rawdb.ReadBlock(ancientDb, spe_blkHash, uint64(i))

		exe_blkHash := rawdb.ReadCanonicalHash(ancientDb, uint64(i+gap))
		exe_block := rawdb.ReadBlock(ancientDb, exe_blkHash, uint64(i+gap))
		logger.Infof("etherscan url: https://etherscan.io/block/%v", i)
		bc.Processor().ProcessSerialWithoutCommit(exe_block, exe_statedb, vm.Config{}, sum)

		r, _ := bc.Processor().ReplayAndReexecute(exe_block, exe_statedb, spe_statedb, vm.Config{}, sum1)
		s += r

		txSum += exe_block.Transactions().Len()
		bc.Processor().ProcessSerial(spe_block, spe_statedb, vm.Config{}, sum2)
		bc.Processor().ProcessSerial(exe_block, exe_statedb, vm.Config{}, sum_serial)
	}
	rate = s / float64(endNum-upNum+1)
	//var s float64
	//for i := upNum; i <= endNum; i++ {
	//	blkHash := rawdb.ReadCanonicalHash(ancientDb, uint64(i))
	//	block := rawdb.ReadBlock(ancientDb, blkHash, uint64(i))
	//	blkBody := rawdb.ReadBody(ancientDb, blkHash, uint64(i))
	//	logger.Infof("etherscan url: https://etherscan.io/block/%v", i)
	//	logger.Infof("the block.transactions is %d", len(blkBody.Transactions))
	//	if len(block.Transactions()) > 50 {
	//		r, _ := bc.Processor().ReplayAndReexecute(block, statedb, vm.Config{}, sum4)
	//      s += r
	//	}
	//}
	//rate := s / float64(endNum - upNum + 1)

	logger.Infof("total txs: %v, sum %+v, sum replay %+v, sum_serial %+v, sum2 %+v", txSum, sum, sum1, sum_serial, sum2)
	logger.Infof("the total re-execution rate is %v\n", rate)
}
