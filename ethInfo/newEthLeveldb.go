package main

import (
	//"fmt"
	"os"
	"time"

	"github.com/ethereum/go-ethereum/consensus/ethash"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/state"

	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/ethLogger"
	"github.com/ethereum/go-ethereum/ethdb"
)

var (
	upNum       = 11735923 //3710581
	endNum      = upNum + 100
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
	ancientDb, err := rawdb.NewLevelDBDatabaseWithFreezer(dbPath, 16, 1, ancientPath, "", true)
	if err != nil {
		panic(err)
	}

	logger := ethLogger.NewZeroLogger()
	txN := getTxSum(upNum, 5000, ancientDb)
	logger.Infof("tx num: %v", txN)
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
	var sum1 *time.Duration = new(time.Duration)
	var sum3 *time.Duration = new(time.Duration)
	//var sum4 *time.Duration = new(time.Duration)
	var analyzeSum *time.Duration = new(time.Duration)
	var sum5 *time.Duration = new(time.Duration)
	var executeTime *time.Duration = new(time.Duration)

	var txSum int
	var chainSum int
	reRxecuteNum := 0
	fbRxecuteNum := 0

	reExecuteCh := make(chan *types.Transaction, 20000)

	for i := upNum; i < endNum; i++ {
		blkHash := rawdb.ReadCanonicalHash(ancientDb, uint64(i))
		block := rawdb.ReadBlock(ancientDb, blkHash, uint64(i))
		blkBody := rawdb.ReadBody(ancientDb, blkHash, uint64(i))
		logger.Infof("etherscan url: https://etherscan.io/block/%v", i)
		logger.Infof("the block.transactions is %d", len(blkBody.Transactions))
		if len(block.Transactions()) > 0 {
			//bc.Processor().Process(block, statedb, vm.Config{})
			bc.Processor().ProcessSerial(block, statedb, vm.Config{}, sum)
			bc.Processor().ProcessSerial(block, statedb, vm.Config{}, sum1)
			//bc.Processor().ProcesswithDag(block, statedb, vm.Config{}, sum4)
			//txChainLen, _, _ := bc.Processor().ProcessWithDeps(block, statedb, vm.Config{}, sum3, analyzeSum)
			//bc.Processor().ProcessOcc(block, statedb, vm.Config{}, sum4)
			//bc.Processor().ParallelBlind(block, statedb, vm.Config{}, sum5)
			bc.Processor().ProcessAriaReorderFB(block, statedb, vm.Config{}, sum5, executeTime, reExecuteCh, &fbRxecuteNum, &reRxecuteNum)
			txSum += len(block.Transactions())
			//chainSum += txChainLen
		}

	}

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

	logger.Infof("load time is %+v", sum)
	logger.Infof("the execution time is %+v", sum1)
	logger.Infof("the analyze time is %+v, the DAG execution time is %+v", analyzeSum, sum3)
	//logger.Infof("the DeOCC execution time is %+v", sum4)
	//logger.Infof("the blindly parallel execution time is %+v", sum5)
	logger.Infof("the AriaFB all time is %+v, execution time is %+v, fbre: %d, re: %d", sum5, executeTime, fbRxecuteNum, reRxecuteNum)
	logger.Infof("the txsum : %d", txSum)
	logger.Infof("the txsum : %d, the chainsum : %d", txSum, chainSum)
	//logger.Infof("the DAG execution time with reexecution is %+v", sum4)
	//logger.Infof("the total re-execution rate is %v\n", rate)

}

func getTxSum(upNum int, lenOfBlocks int, ancientDb ethdb.Database) int {
	chainSum := 0
	for i := upNum; i < upNum+lenOfBlocks; i++ {
		blkHash := rawdb.ReadCanonicalHash(ancientDb, uint64(i))
		block := rawdb.ReadBlock(ancientDb, blkHash, uint64(i))
		chainSum += len(block.Transactions())
	}
	return chainSum
}
