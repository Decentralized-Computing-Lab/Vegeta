// Copyright 2015 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package core

import (
	"container/list"
	"fmt"
	"os"
	"sort"

	//"github.com/docker/docker/daemon/logger"
	"github.com/ethereum/go-ethereum/ethLogger"

	// "github.com/ethereum/go-ethereum/logger"
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/panjf2000/ants/v2"

	//"github.com/ethereum/go-ethereum/logger"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum/bitmap"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/consensus"
	"github.com/ethereum/go-ethereum/consensus/misc"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/params"
)

// StateProcessor is a basic Processor, which takes care of transitioning
// state from one point to another.
//
// StateProcessor implements Processor.
type StateProcessor struct {
	config *params.ChainConfig // Chain configuration options
	bc     *BlockChain         // Canonical block chain
	engine consensus.Engine    // Consensus engine used for block rewards
}

// NewStateProcessor initialises a new StateProcessor.
func NewStateProcessor(config *params.ChainConfig, bc *BlockChain, engine consensus.Engine) *StateProcessor {
	return &StateProcessor{
		config: config,
		bc:     bc,
		engine: engine,
	}
}

type DAG_Neighbor struct {
	Neighbors []uint32
}

type DAG struct {
	Vertexes []*DAG_Neighbor
}

// Transaction dependency in adjacency table representation
type dagNeighbors map[int]bool

type ConsensusContent struct {
	DagDeps      map[int]map[int]int `json:"DAG,omitempty"`
	ReadBitmaps  []*bitmap.Bitmap    `json:"read_set,omitempty"`
	WriteBitmaps []*bitmap.Bitmap    `json:"write_set,omitempty"`
	KeyDict      map[string]int      `json:"key_dict,omitempty"`
	TxChain      []int               `json:"tx_chain,omitempty"`
	BlockNum     *big.Int            `json:"block_number"`
	BytesOfBlock []byte              `json:"encoded_block"`

	// used for fabric execute-order-validate process
	BytesOfPreBlock [][]byte         `json:"encoded_preblock,omitempty"`
	TxValidateMask  []*bitmap.Bitmap `json:"tx_validate,omitempty"`
	FReadBitmaps    []*bitmap.Bitmap `json:"fread_set,omitempty"`
	FWriteBitmaps   []*bitmap.Bitmap `json:"wread_set,omitempty"`
}

// Process processes the state changes according to the Ethereum rules by running
// the transaction messages using the statedb and applying any rewards to both
// the processor (coinbase) and any included uncles.
//
// Process returns the receipts and logs accumulated during the process and
// returns the amount of gas that was used in the process. If any of the
// transactions failed to execute due to insufficient gas it will return an error.
func (p *StateProcessor) Process(block *types.Block, statedb *state.StateDB, cfg vm.Config) (types.Receipts, []*types.Log, uint64, error) {
	var (
		receipts    types.Receipts
		usedGas     = new(uint64)
		header      = block.Header()
		blockHash   = block.Hash()
		blockNumber = block.Number()
		allLogs     []*types.Log
		gp          = new(GasPool).AddGas(block.GasLimit())
	)

	// Mutate the block and state according to any hard-fork specs
	if p.config.DAOForkSupport && p.config.DAOForkBlock != nil && p.config.DAOForkBlock.Cmp(block.Number()) == 0 {
		misc.ApplyDAOHardFork(statedb)
	}

	blockContext := NewEVMBlockContext(header, p.bc, nil)
	vmenv := vm.NewEVM(blockContext, vm.TxContext{}, statedb, p.config, cfg)

	// Iterate over and process the individual transactions
	for i, tx := range block.Transactions() {
		msg, err := tx.AsMessage(types.MakeSigner(p.config, header.Number), header.BaseFee)
		if err != nil {
			return nil, nil, 0, fmt.Errorf("could not apply tx %d [%v]: %w", i, tx.Hash().Hex(), err)
		}
		statedb.Prepare(tx.Hash(), i)
		receipt, err := applyTransaction(msg, p.config, p.bc, nil, gp, statedb, blockNumber, blockHash, tx, usedGas, vmenv)
		if err != nil {
			return nil, nil, 0, fmt.Errorf("could not apply tx %d [%v]: %w", i, tx.Hash().Hex(), err)
		}
		receipts = append(receipts, receipt)
		allLogs = append(allLogs, receipt.Logs...)
	}

	// Finalize the block, applying any consensus engine specific extras (e.g. block rewards)
	p.engine.Finalize(p.bc, header, statedb, block.Transactions(), block.Uncles())
	return receipts, allLogs, *usedGas, nil
}

func (p *StateProcessor) ProcessSerial(block *types.Block, statedb *state.StateDB, cfg vm.Config, sum *time.Duration) (uint64, error) {
	var (
		usedGas     = new(uint64)
		header      = block.Header()
		blockNumber = block.Number()
		gp          = new(GasPool).AddGas(block.GasLimit())
	)

	if p.config.DAOForkSupport && p.config.DAOForkBlock != nil && p.config.DAOForkBlock.Cmp(block.Number()) == 0 {
		misc.ApplyDAOHardFork(statedb)
	}

	// logger := ethLogger.NewZeroLogger()

	startSerial := time.Now()
	blockContext := NewEVMBlockContext(header, p.bc, nil)
	vmenv := vm.NewEVM(blockContext, vm.TxContext{}, statedb, p.config, cfg)

	for i, tx := range block.Transactions() {
		msg, _ := tx.AsMessage(types.MakeSigner(p.config, header.Number), header.BaseFee)
		statedb.Prepare(tx.Hash(), i)
		// start := time.Now()
		//applyTransactionSerial(msg, p.config, gp, statedb, blockNumber, usedGas, vmenv, sum)
		applyTransaction2(msg, p.config, p.bc, nil, gp, statedb, blockNumber, block.Hash(), tx, usedGas, vmenv)
		// end := time.Since(start)
		// logger.Infof("the tx_%d, the applyTransactionSerial time is %+v", i, end)
	}

	statedb.IntermediateRoot3(p.config.IsEIP158(blockNumber))
	endSerial := time.Since(startSerial)
	*sum += endSerial
	// logger.Infof("the sum txs of block_%d is %d, the ProcessSerial time is %+v", block.Number(), len(block.Transactions()), *sum)
	p.engine.Finalize(p.bc, header, statedb, block.Transactions(), block.Uncles())
	return *usedGas, nil
}

func (p *StateProcessor) ProcessSerialWithoutCommit(block *types.Block, statedb *state.StateDB, cfg vm.Config, sum *time.Duration) (uint64, error) {
	var (
		usedGas     = new(uint64)
		header      = block.Header()
		blockNumber = block.Number()
		gp          = new(GasPool).AddGas(block.GasLimit())
	)

	if p.config.DAOForkSupport && p.config.DAOForkBlock != nil && p.config.DAOForkBlock.Cmp(block.Number()) == 0 {
		misc.ApplyDAOHardFork(statedb)
	}

	// logger := ethLogger.NewZeroLogger()

	startSerial := time.Now()
	blockContext := NewEVMBlockContext(header, p.bc, nil)
	vmenv := vm.NewEVM(blockContext, vm.TxContext{}, statedb, p.config, cfg)

	for i, tx := range block.Transactions() {
		msg, _ := tx.AsMessage(types.MakeSigner(p.config, header.Number), header.BaseFee)
		statedb.Prepare(tx.Hash(), i)
		// start := time.Now()
		//applyTransactionSerial(msg, p.config, gp, statedb, blockNumber, usedGas, vmenv, sum)
		applyTransaction2(msg, p.config, p.bc, nil, gp, statedb, blockNumber, block.Hash(), tx, usedGas, vmenv)
		// end := time.Since(start)
		// logger.Infof("the tx_%d, the applyTransactionSerial time is %+v", i, end)
	}

	// statedb.IntermediateRoot3(p.config.IsEIP158(blockNumber))
	endSerial := time.Since(startSerial)
	*sum += endSerial
	// logger.Infof("the sum txs of block_%d is %d, the ProcessSerial time is %+v", block.Number(), len(block.Transactions()), *sum)
	// p.engine.Finalize(p.bc, header, statedb, block.Transactions(), block.Uncles())
	return *usedGas, nil
}

func applyTransaction(msg types.Message, config *params.ChainConfig, bc ChainContext, author *common.Address, gp *GasPool, statedb *state.StateDB, blockNumber *big.Int, blockHash common.Hash, tx *types.Transaction, usedGas *uint64, evm *vm.EVM) (*types.Receipt, error) {
	// Create a new context to be used in the EVM environment.
	txContext := NewEVMTxContext(msg)
	evm.Reset(txContext, statedb)

	// Apply the transaction to the current state (included in the env).
	// Message, GasPool, EVM
	result, err := ApplyMessage(evm, msg, gp)
	if err != nil {
		return nil, err
	}

	// Update the state with pending changes.
	var root []byte
	if config.IsByzantium(blockNumber) {
		statedb.Finalise(true)
	} else {
		root = statedb.IntermediateRoot(config.IsEIP158(blockNumber)).Bytes()
	}
	*usedGas += result.UsedGas

	// Create a new receipt for the transaction, storing the intermediate root and gas used
	// by the tx.
	receipt := &types.Receipt{Type: tx.Type(), PostState: root, CumulativeGasUsed: *usedGas}
	if result.Failed() {
		receipt.Status = types.ReceiptStatusFailed
	} else {
		receipt.Status = types.ReceiptStatusSuccessful
	}
	receipt.TxHash = tx.Hash()
	receipt.GasUsed = result.UsedGas

	// If the transaction created a contract, store the creation address in the receipt.
	if msg.To() == nil {
		receipt.ContractAddress = crypto.CreateAddress(evm.TxContext.Origin, tx.Nonce())
	}

	// Set the receipt logs and create the bloom filter.
	receipt.Logs = statedb.GetLogs(tx.Hash(), blockHash)
	receipt.Bloom = types.CreateBloom(types.Receipts{receipt})
	receipt.BlockHash = blockHash
	receipt.BlockNumber = blockNumber
	receipt.TransactionIndex = uint(statedb.TxIndex())
	return receipt, err
}

func applyTransactionSerial(msg types.Message, config *params.ChainConfig, gp *GasPool, statedb *state.StateDB, blockNumber *big.Int, usedGas *uint64, evm *vm.EVM, applyTime *time.Duration) /*types.Receipt,*/ error {
	txContext := NewEVMTxContext(msg)
	evm.Reset(txContext, statedb)
	result, err := ApplyMessage(evm, msg, gp)

	if err != nil {
		return err
	}
	//if config.IsByzantium(blockNumber) {
	//	statedb.Finalise(true)
	//} else {
	statedb.IntermediateRoot3(config.IsEIP158(blockNumber)).Bytes()
	//}
	*usedGas += result.UsedGas
	return err
}

// Build txs' read bitmap and write bitmap, so we can use AND to simplify read/write set conflict process.
// keyDict: key string -> key index in bitmap, e.g., key1 -> 0, key2 -> 1, key3 -> 2
// read/write Table:  tx1: {key1, key3}; tx2: {key2, key3}
// read/write bitmap:         key1      key2     key3
//
//	tx1      1        0        1
//	tx2      0        1        1
func buildRWBitmaps(ReadSets map[int]map[common.Address]struct{},
	WriteSets map[int]map[common.Address]struct{}) ([]*bitmap.Bitmap, []*bitmap.Bitmap, map[string]int) {
	dictIndex := 0
	txCount := len(ReadSets)
	readBitmap := make([]*bitmap.Bitmap, txCount)
	writeBitmap := make([]*bitmap.Bitmap, txCount)
	keyDict := make(map[string]int, 1024)
	for i := 0; i < txCount; i++ {
		readTableItemForI := make([]common.Address, 0, len(ReadSets[i]))
		for k := range ReadSets[i] {
			readTableItemForI = append(readTableItemForI, k)
		}
		writeTableItemForI := make([]common.Address, 0, len(WriteSets[i]))
		for k := range WriteSets[i] {
			writeTableItemForI = append(writeTableItemForI, k)
		}

		readBitmap[i] = &bitmap.Bitmap{}
		for _, keyForI := range readTableItemForI {
			if existIndex, ok := keyDict[string(keyForI[:])]; !ok {
				keyDict[string(keyForI[:])] = dictIndex
				readBitmap[i].Set(dictIndex)
				dictIndex++
			} else {
				readBitmap[i].Set(existIndex)
			}
		}

		writeBitmap[i] = &bitmap.Bitmap{}
		for _, keyForI := range writeTableItemForI {
			if existIndex, ok := keyDict[string(keyForI[:])]; !ok {
				keyDict[string(keyForI[:])] = dictIndex
				writeBitmap[i].Set(dictIndex)
				dictIndex++
			} else {
				writeBitmap[i].Set(existIndex)
			}
		}
	}
	return readBitmap, writeBitmap, keyDict
}

func buildCumulativeBitmap(readBitmap []*bitmap.Bitmap,
	writeBitmap []*bitmap.Bitmap) ([]*bitmap.Bitmap, []*bitmap.Bitmap) {
	cumulativeReadBitmap := make([]*bitmap.Bitmap, len(readBitmap))
	cumulativeWriteBitmap := make([]*bitmap.Bitmap, len(writeBitmap))
	for i, b := range readBitmap {
		cumulativeReadBitmap[i] = b.Clone()
		if i > 0 {
			cumulativeReadBitmap[i].Or(cumulativeReadBitmap[i-1])
		}
	}
	for i, b := range writeBitmap {
		cumulativeWriteBitmap[i] = b.Clone()
		if i > 0 {
			cumulativeWriteBitmap[i].Or(cumulativeWriteBitmap[i-1])
		}
	}
	return cumulativeReadBitmap, cumulativeWriteBitmap
}

// Conflict cases: I read & J write; I write & J read; I write & J write
func conflicted(readBitmapForI, writeBitmapForI, readBitmapForJ, writeBitmapForJ *bitmap.Bitmap) bool {
	if readBitmapForI.InterExist(writeBitmapForJ) ||
		writeBitmapForI.InterExist(writeBitmapForJ) ||
		writeBitmapForI.InterExist(readBitmapForJ) {
		return true
	}
	return false
}

// fast conflict cases: I read & J write; I write & J read; I write & J write
func fastConflicted(readBitmapForI, writeBitmapForI, cumulativeReadBitmap,
	cumulativeWriteBitmap *bitmap.Bitmap) bool {
	if readBitmapForI.InterExist(cumulativeWriteBitmap) ||
		writeBitmapForI.InterExist(cumulativeWriteBitmap) ||
		writeBitmapForI.InterExist(cumulativeReadBitmap) {
		return true
	}
	return false
}

func buildReach(i int, reachFromI *bitmap.Bitmap,
	readBitmaps []*bitmap.Bitmap, writeBitmaps []*bitmap.Bitmap,
	readBitmapForI *bitmap.Bitmap, writeBitmapForI *bitmap.Bitmap,
	directReachFromI *bitmap.Bitmap, reachMap []*bitmap.Bitmap) {
	for j := i - 1; j >= 0; j-- {
		if reachFromI.Has(j) {
			continue
		}
		readBitmapForJ := readBitmaps[j]
		writeBitmapForJ := writeBitmaps[j]
		if conflicted(readBitmapForI, writeBitmapForI, readBitmapForJ, writeBitmapForJ) {
			directReachFromI.Set(j)
			reachFromI.Or(reachMap[j])
		}
	}
}

func BuildDAG(ReadSets map[int]map[common.Address]struct{}, WriteSets map[int]map[common.Address]struct{}, blockNumber *big.Int) (*DAG, []*bitmap.Bitmap, []*bitmap.Bitmap, map[string]int, []*bitmap.Bitmap) {
	txCount := len(ReadSets)
	// build read-write bitmap for all transactions
	readBitmaps, writeBitmaps, keyDict := buildRWBitmaps(ReadSets, WriteSets)
	cumulativeReadBitmap, cumulativeWriteBitmap := buildCumulativeBitmap(readBitmaps, writeBitmaps)

	dag := &DAG{}
	if txCount == 0 {
		return dag, nil, nil, nil, nil
	}
	dag.Vertexes = make([]*DAG_Neighbor, txCount)

	// build DAG base on read and write bitmaps
	// reachMap describes reachability from tx i to tx j in DAG.
	// For example, if the DAG is tx3 -> tx2 -> tx1 -> begin, the reachMap is
	// 		tx1		tx2		tx3
	// tx1	0		0		0
	// tx2	1		0		0
	// tx3	1		1		0
	reachMap := make([]*bitmap.Bitmap, txCount)

	for i := 0; i < txCount; i++ {
		// 1. get read and write bitmap for tx i
		readBitmapForI := readBitmaps[i]
		writeBitmapForI := writeBitmaps[i]
		// directReachFromI is used to build DAG, it's the direct neighbors of the ith tx
		directReachFromI := &bitmap.Bitmap{}
		// reachFromI is used to save reachability we have already known, it's the all neighbors of the ith tx
		reachFromI := &bitmap.Bitmap{}
		reachFromI.Set(i)

		if i > 0 && fastConflicted(
			readBitmapForI, writeBitmapForI, cumulativeReadBitmap[i-1], cumulativeWriteBitmap[i-1]) {
			buildReach(i, reachFromI, readBitmaps, writeBitmaps, readBitmapForI, writeBitmapForI, directReachFromI, reachMap)
		}
		reachMap[i] = reachFromI

		// build DAG based on directReach bitmap
		dag.Vertexes[i] = &DAG_Neighbor{
			Neighbors: make([]uint32, 0, 16),
		}
		for _, j := range directReachFromI.Pos1() {
			dag.Vertexes[i].Neighbors = append(dag.Vertexes[i].Neighbors, uint32(j))
		}
	}
	return dag, readBitmaps, writeBitmaps, keyDict, reachMap
}

// 不考虑冲突分析，并行执行生成读写集合
func (p *StateProcessor) ProcessWithoutConflict(block *types.Block, statedb *state.StateDB,
	cfg vm.Config) (int64, map[int]dagNeighbors, []*bitmap.Bitmap, []*bitmap.Bitmap, map[string]int, []*bitmap.Bitmap, time.Duration, error) {

	var (
		// logger      = ethLogger.NewZeroLogger()
		header = block.Header()
		gp     = new(GasPool).AddGas(block.GasLimit())
		wg     sync.WaitGroup
		lock   sync.RWMutex
	)

	// Mutate the block and state according to any hard-fork specs
	if p.config.DAOForkSupport && p.config.DAOForkBlock != nil && p.config.DAOForkBlock.Cmp(block.Number()) == 0 {
		misc.ApplyDAOHardFork(statedb)
	}

	var (
		copyStateDB = make([]*state.StateDB, block.Transactions().Len())
		Readsets    = make(map[int]map[common.Address]struct{})
		Writesets   = make(map[int]map[common.Address]struct{})
	)

	for i := 0; i < block.Transactions().Len(); i++ {
		copyStateDB[i] = statedb.Copy()
		copyStateDB[i].StateCopy(statedb)
	}

	var goRoutinePool *ants.Pool
	var poolCapacity = runtime.NumCPU() * 10
	goRoutinePool, _ = ants.NewPool(poolCapacity, ants.WithPreAlloc(true))
	wg.Add(block.Transactions().Len())
	for i, tx := range block.Transactions() {
		tx := tx
		i := i
		goRoutinePool.Submit(func() {
			msg, _ := tx.AsMessage(types.MakeSigner(p.config, header.Number), header.BaseFee)
			blockContext := NewEVMBlockContext(header, p.bc, nil)
			lock.RLock()
			copydb := copyStateDB[i]
			lock.RUnlock()
			vmenv := vm.NewEVM(blockContext, vm.TxContext{}, copydb, p.config, cfg)
			copydb.Prepare(tx.Hash(), i)
			applyTransactionWithoutConflict(msg, gp, copydb, vmenv)

			lock.Lock()
			if Readsets[i] == nil {
				Readsets[i] = make(map[common.Address]struct{})
				Readsets[i] = copydb.Readset
			}
			if Writesets[i] == nil {
				Writesets[i] = make(map[common.Address]struct{})
				Writesets[i] = copydb.Writeset
			}
			lock.Unlock()
			wg.Done()
		})
	}
	wg.Wait()

	start := time.Now()
	dag, readBitmaps, writeBitmaps, keyDict, reachMap := BuildDAG(Readsets, Writesets, block.Number())
	// Construct the adjacency list of dag, which describes the subsequent adjacency transactions of all transactions
	dagRemain := make(map[int]dagNeighbors)
	for txIndex, neighbors := range dag.Vertexes {
		dn := make(dagNeighbors)
		for _, neighbor := range neighbors.Neighbors {
			dn[int(neighbor)] = true
		}
		dagRemain[txIndex] = dn
	}
	dagtime := time.Since(start)
	// logger.Infof("buildDag time is: %+v", dagtime)
	return 0, dagRemain, readBitmaps, writeBitmaps, keyDict, reachMap, dagtime, nil
}

// func (bp *BlocksPackage) Empty() bool {
// 	return bp.len == 0
// }

func (bp *BlocksPackage) Empty() bool {
	return bp.list.Len() == 0
}

func (bp *BlocksPackage) Done() bool {
	return bp.blockNum == 0
}

func (bp *BlocksPackage) Free() {
	bp.list = list.New()
}

func (bp *BlocksPackage) AddBlock(block *types.Block) {
	block.Validate = &bitmap.Bitmap{}
	block.Validate = block.Validate.Fill1(block.Transactions().Len())
	bp.list.PushBack(block)
	bp.blockNum++
}

func (bp *BlocksPackage) AddValidateResult(bp2 *BlocksPackage) {
	t := bp2.list.Back()
	for t != nil {
		block := t.Value.(*types.Block)
		bp.list.PushFront(block)
		t = t.Prev()
	}
	bp.blockNum -= bp2.blockNum // 剪去所有tx都被验证完的块的数量
}

// func (bp *BlocksPackage) AddBlock(block *types.Block) {
// 	bf := &BlockFlow{block: block, next: nil} // 插入到链表尾部
// 	bp.len = bp.len + 1
// 	if bp.head == nil {
// 		bp.head = bf
// 	} else {
// 		bp.tail.next = bf
// 	}
// 	bp.tail = bf
// 	bf.block.Validate = &bitmap.Bitmap{}
// 	bf.block.Validate = bf.block.Validate.Fill1(block.Transactions().Len())
// }

func (bp *BlocksPackage) Log(file *os.File) {
	txNums := 0
	h := bp.list.Front()
	for h != nil {
		block := h.Value.(*types.Block)
		txNums = txNums + block.Validate.Pos1Nums()
		txs := block.Validate.Pos1()
		file.Write([]byte(fmt.Sprintf("\tblock %d: abort txs: %d\n", block.Number(), len(txs))))
		h = h.Next()
	}
	// fmt.Printf("bp has len: %v, txs num: %v\n", bp.len, txNums)
	file.Write([]byte(fmt.Sprintf(">>txs in execute: %v, txs abort: %v,total accept %v, total abort %v\n\n", bp.txsize, bp.abortsize, bp.totalexecute, bp.totalabort)))
	// fmt.Sprintf(">>txs in execute: %v, txs abort: %v,total accept %v, total abort %v\n\n", bp.txsize, bp.abortsize, bp.totalexecute, bp.totalabort)
}

// 不考虑冲突分析，并行执行生成读写集合
func (p *StateProcessor) ProcessExecute(bp *BlocksPackage, statedb *state.StateDB,
	cfg vm.Config) ([]*bitmap.Bitmap, []*bitmap.Bitmap, map[string]int, time.Duration, error) {
	txNums := 0
	h := bp.list.Front()
	for h != nil {
		block := h.Value.(*types.Block)
		txNums += block.Validate.Pos1Nums()
		h = h.Next()
	}
	bp.txsize = txNums
	var (
		// logger      = ethLogger.NewZeroLogger()
		headers = make([]*types.Header, 0)
		// gp      = new(GasPool).AddGas(bp.tail.block.GasLimit())
		gp   = new(GasPool)
		wg   sync.WaitGroup
		lock sync.RWMutex
		txs  types.Transactions
	)

	h = bp.list.Front()
	for h != nil {
		block := h.Value.(*types.Block)
		v := block.Validate.Pos1()
		for tid := range v {
			txs = append(txs, block.Transactions()[tid])
			headers = append(headers, block.Header())
		}
		gp.AddGas(block.GasLimit())
		h = h.Next()
	}
	// Mutate the block and state according to any hard-fork specs
	if p.config.DAOForkSupport && p.config.DAOForkBlock != nil && p.config.DAOForkBlock.Cmp(bp.list.Back().Value.(*types.Block).Number()) == 0 {
		misc.ApplyDAOHardFork(statedb)
	}

	var (
		copyStateDB = make([]*state.StateDB, txNums)
		Readsets    = make(map[int]map[common.Address]struct{})
		Writesets   = make(map[int]map[common.Address]struct{})
	)

	for i := 0; i < txNums; i++ {
		copyStateDB[i] = statedb.Copy()
		copyStateDB[i].StateCopy(statedb)
	}

	var goRoutinePool *ants.Pool
	var poolCapacity = runtime.NumCPU() * 10
	goRoutinePool, _ = ants.NewPool(poolCapacity, ants.WithPreAlloc(true))
	wg.Add(txNums)
	start := time.Now()
	for i, tx := range txs {
		tx := tx
		i := i
		goRoutinePool.Submit(func() {
			msg, _ := tx.AsMessage(types.MakeSigner(p.config, headers[i].Number), headers[i].BaseFee)
			blockContext := NewEVMBlockContext(headers[i], p.bc, nil)
			lock.RLock()
			copydb := copyStateDB[i]
			lock.RUnlock()
			vmenv := vm.NewEVM(blockContext, vm.TxContext{}, copydb, p.config, cfg)
			copydb.Prepare(tx.Hash(), i)
			applyTransactionWithoutConflict(msg, gp, copydb, vmenv)

			lock.Lock()
			if Readsets[i] == nil {
				Readsets[i] = make(map[common.Address]struct{})
				Readsets[i] = copydb.Readset
			}
			if Writesets[i] == nil {
				Writesets[i] = make(map[common.Address]struct{})
				Writesets[i] = copydb.Writeset
			}
			lock.Unlock()
			wg.Done()
		})
	}
	wg.Wait()

	// dag, readBitmaps, writeBitmaps, keyDict, reachMap := BuildDAG(Readsets, Writesets, block.Number())
	readBitmaps, writeBitmaps, keyDict := buildRWBitmaps(Readsets, Writesets)
	// Construct the adjacency list of dag, which describes the subsequent adjacency transactions of all transactions
	dagtime := time.Since(start)
	// logger.Infof("buildDag time is: %+v", dagtime)
	return readBitmaps, writeBitmaps, keyDict, dagtime, nil
}

func (p *StateProcessor) ProcessValidate(bp *BlocksPackage, RSet []*bitmap.Bitmap, WSet []*bitmap.Bitmap) time.Duration {
	wbit := &bitmap.Bitmap{}
	i := 0
	abortn := 0
	start := time.Now()
	h := bp.list.Front()
	for h != nil {
		block := h.Value.(*types.Block)
		v := block.Validate.Pos1()
		for _, tid := range v {
			if !wbit.Conflict(RSet[i]) { // 如果没有冲突则这个交易可以提交
				// bf.block.Validate.Set(tid)
				// fmt.Printf(">>> set bit %d for block %d: %x\n", tid, bf.block.Number(), bf.block.Validate.Words)
				block.Validate.SetZero(tid)
				// fmt.Printf(">>> the bit %d for block %d: %x\n", tid, bf.block.Number(), bf.block.Validate.Words)
				wbit.Or(WSet[i])
				// n = n + 1
			} else {
				abortn = abortn + 1
			}
			i = i + 1
		}
		if block.Validate.EqualZero() { // 从bp中去掉这个block
			// fmt.Printf("block %v empty\n", bf.block.Number())
			bp.blockNum++
			ht := h.Next()
			bp.list.Remove(h)
			h = ht
		} else {
			h = h.Next()
		}
	}
	t := time.Since(start)
	bp.abortsize = abortn
	bp.totalabort += abortn
	bp.totalexecute += bp.txsize - abortn
	return t
}

func (p *StateProcessor) ProcessEOV(bp *BlocksPackage, statedb *state.StateDB, cfg vm.Config, sum *time.Duration, executeSum *time.Duration) (time.Duration, error) {
	readBitmaps, writeBitmaps, _, time, _ := p.ProcessExecute(bp, statedb, cfg)
	t := p.ProcessValidate(bp, readBitmaps, writeBitmaps)
	*sum = *sum + t // 并行执行时间加上验证的时间
	*executeSum = *executeSum + time
	return time, nil
}

func (p *StateProcessor) Execute(bp *BlocksPackage, statedb *state.StateDB, cfg vm.Config) (time.Duration, *ConsensusContent) {
	RBitmp, WBitmp, _, time, _ := p.ProcessExecute(bp, statedb, cfg)
	tail_block := bp.list.Back().Value.(*types.Block)
	b, _ := tail_block.EncodeToBytes()
	content := &ConsensusContent{
		FReadBitmaps:  RBitmp,
		FWriteBitmaps: WBitmp,
		BlockNum:      tail_block.Number(),
		BytesOfBlock:  b,
	}

	h := bp.list.Front()
	for h != bp.list.Back() {
		block := h.Value.(*types.Block)
		d, _ := block.EncodeToBytes()
		content.BytesOfPreBlock = append(content.BytesOfPreBlock, d)
		h = h.Next()
	}

	return time, content
}

func (p *StateProcessor) Validate(bp *BlocksPackage, cc *ConsensusContent, statedb *state.StateDB, cfg vm.Config, sum *time.Duration) error {
	var (
		readBitmaps  = cc.FReadBitmaps
		writeBitmaps = cc.FWriteBitmaps
	)
	t := p.ProcessValidate(bp, readBitmaps, writeBitmaps)
	*sum = *sum + t

	return nil
}

func hasconflicts(txIndex int, readTableItemForI, writeTableItemForI []common.Address, writes map[string]int) bool {
	for _, k := range readTableItemForI {
		if tid, ok := writes[string(k[:])]; ok {
			if tid < txIndex {
				return true
			}
		}
	}
	for _, k := range writeTableItemForI {
		if tid, ok := writes[string(k[:])]; ok {
			if tid < txIndex {
				return true
			}
		}
	}
	return false
}

func (p *StateProcessor) ProcessOcc(block *types.Block, statedb *state.StateDB, cfg vm.Config, sum *time.Duration) (time.Duration, error) {
	var (
		header = block.Header()
		gp     = new(GasPool).AddGas(block.GasLimit())
		//logger = ethLogger.NewZeroLogger()
		// txCount     = block.Transactions().Len()
	)
	if p.config.DAOForkSupport && p.config.DAOForkBlock != nil && p.config.DAOForkBlock.Cmp(block.Number()) == 0 {
		misc.ApplyDAOHardFork(statedb)
	}
	var (
		txMapping        = make(map[int]*types.Transaction)
		txBatchSize      = len(block.Transactions())
		runningTxC       = make(chan int, txBatchSize)
		rerunningTxC     = make(chan int, txBatchSize)
		finishC          = make(chan bool)
		scheduleFinishC  = make(chan bool)
		poolCapacity     = 16
		copyObjectsTimes = make([]time.Duration, block.Transactions().Len())
		writes           = make(map[string]int, 1024)
	)

	for index, tx := range block.Transactions() {
		txMapping[index] = tx
	}
	var applySize = 0
	var (
		err          error
		lockapply    sync.RWMutex
		lockdb       sync.RWMutex
		lockconflict sync.RWMutex
		lockwrite    sync.RWMutex
	)

	var goRoutinePool *ants.Pool
	goRoutinePool, _ = ants.NewPool(poolCapacity, ants.WithPreAlloc(true))
	defer goRoutinePool.Release()
	executeStart := time.Now()
	go func() {
		for {
			select {
			case txIndex := <-runningTxC:
				tx := txMapping[txIndex]
				err = goRoutinePool.Submit(func() {
					msg, _ := tx.AsMessage(types.MakeSigner(p.config, header.Number), header.BaseFee)
					blockContext := NewEVMBlockContext(header, p.bc, nil)

					start1 := time.Now()
					// dictIndex := 0
					lockdb.RLock()
					copydb := statedb.Copy()
					lockdb.RUnlock()
					end1 := time.Since(start1)

					vmenv := vm.NewEVM(blockContext, vm.TxContext{}, copydb, p.config, cfg)
					copydb.Prepare(tx.Hash(), txIndex)
					applyTransactionWithoutConflict(msg, gp, copydb, vmenv)

					//start2 := time.Now()
					lockapply.Lock()
					// num := applySize
					//end2 := time.Since(start2)
					copyObjectsTimes[txIndex] = end1 //+ end2
					lockapply.Unlock()

					reExecute := false
					readTableItemForI := make([]common.Address, 0, len(copydb.Readset))
					for k := range copydb.Readset {
						readTableItemForI = append(readTableItemForI, k)
					}
					writeTableItemForI := make([]common.Address, 0, len(copydb.Writeset))
					lockwrite.Lock()
					for k := range copydb.Writeset {
						writeTableItemForI = append(writeTableItemForI, k)
						if tid, ok := writes[string(k[:])]; ok { // 存在
							if txIndex < tid {
								writes[string(k[:])] = txIndex
							}
						} else {
							writes[string(k[:])] = txIndex // 不存在
						}
					}
					lockwrite.Unlock()

					lockconflict.Lock()

					lockwrite.RLock()
					if hasconflicts(txIndex, readTableItemForI, writeTableItemForI, writes) {
						reExecute = true
					}
					lockwrite.RUnlock()
					if reExecute {
						rerunningTxC <- txIndex
						//logger.Debugf("the re-execute tx is %+v", txIndex)
					}
					applySize++
					//logger.Debugf("applySizeis %+v", applySize)
					lockconflict.Unlock()
					if applySize >= txBatchSize {
						finishC <- true
					}
				})
			case <-finishC:
				//logger.Debugf("schedule finish")
				scheduleFinishC <- true
				return
			}
		}
	}()

	// Put the pending transaction into the running queue
	go func() {
		if len(block.Transactions()) > 0 {
			for i, _ := range block.Transactions() {
				runningTxC <- i
			}
		} else {
			finishC <- true
		}
	}()
	<-scheduleFinishC
	close(rerunningTxC)
	for txid := range rerunningTxC {
		//logger.Debugf("the re-execute tx %+v", txid)
		tx := txMapping[txid]
		msg, _ := tx.AsMessage(types.MakeSigner(p.config, header.Number), header.BaseFee)
		blockContext := NewEVMBlockContext(header, p.bc, nil)
		copydb := statedb.Copy()
		vmenv := vm.NewEVM(blockContext, vm.TxContext{}, copydb, p.config, cfg)
		copydb.Prepare(tx.Hash(), txid)
		applyTransactionWithoutConflict(msg, gp, copydb, vmenv)
	}
	//logger.Debugf("all executed")
	executeTime := time.Since(executeStart)

	var copytimes time.Duration
	for i := 0; i < txBatchSize/poolCapacity; i++ {
		copytimes += copyObjectsTimes[i]
	}
	*sum += executeTime - copytimes
	///logger.Infof("simulate time with occ, size %d, execute time used %+v", len(block.Transactions()), executeTime-copytimes)
	return copytimes, err
}

func (p *StateProcessor) ProcesswithDag(block *types.Block, statedb *state.StateDB, cfg vm.Config, sum *time.Duration) (time.Duration, error) {
	var (
		usedGas     = new(uint64)
		header      = block.Header()
		blockHash   = block.Hash()
		blockNumber = block.Number()
		gp          = new(GasPool).AddGas(block.GasLimit())
		logger      = ethLogger.NewZeroLogger()
	)
	if p.config.DAOForkSupport && p.config.DAOForkBlock != nil && p.config.DAOForkBlock.Cmp(block.Number()) == 0 {
		misc.ApplyDAOHardFork(statedb)
	}
	var (
		copyStateDB = make([]*state.StateDB, block.Transactions().Len())
		txMapping   = make(map[int]*types.Transaction)
		txBatchSize = len(block.Transactions())
		// runningTxC       = make(chan int, txBatchSize)
		runningTxC       = make(chan int, txBatchSize+2)
		doneTxC          = make(chan int, txBatchSize)
		finishC          = make(chan bool)
		scheduleFinishC  = make(chan bool)
		poolCapacity     = runtime.NumCPU()
		copyObjectsTimes = make([]time.Duration, block.Transactions().Len())
		lastFinished     int
	)

	// ProcessWithoutConflict
	_, dagRemain, _, _, _, reachMap, _, _ := p.ProcessWithoutConflict(block, statedb, cfg)
	// logger.Infof("the dagRemain is %+v", dagRemain)
	var unconflictNum uint64 = 0
	for _, neighbors := range dagRemain {
		if len(neighbors) == 0 {
			unconflictNum++
		}
	}
	// logger.Infof("the unconflictNum is %+v", unconflictNum)

	for index, tx := range block.Transactions() {
		txMapping[index] = tx
	}
	for i := 0; i < block.Transactions().Len(); i++ {
		copyStateDB[i] = statedb.Copy()
		copyStateDB[i].StateCopy(statedb)
	}
	var (
		err       error
		applySize uint32
		lock      sync.RWMutex
	)
	var goRoutinePool *ants.Pool
	goRoutinePool, _ = ants.NewPool(poolCapacity, ants.WithPreAlloc(true))
	defer goRoutinePool.Release()
	executeStart := time.Now()
	go func() {
		for {
			select {
			case txIndex := <-runningTxC:
				tx := txMapping[txIndex]
				err = goRoutinePool.Submit(func() {
					msg, _ := tx.AsMessage(types.MakeSigner(p.config, header.Number), header.BaseFee)
					blockContext := NewEVMBlockContext(header, p.bc, nil)
					start1 := time.Now()
					lock.RLock()
					copydb := copyStateDB[txIndex]
					lock.RUnlock()
					end1 := time.Since(start1)
					vmenv := vm.NewEVM(blockContext, vm.TxContext{}, copydb, p.config, cfg)
					copydb.Prepare(tx.Hash(), txIndex)
					applyTransaction2(msg, p.config, p.bc, nil, gp, copydb, blockNumber, blockHash, tx, usedGas, vmenv)
					atomic.AddUint32(&applySize, 1)
					copyObjectsTimes[txIndex] = end1
					doneTxC <- txIndex
					if applySize >= (uint32)(len(block.Transactions())) {
						lastFinished = txIndex
						finishC <- true
					}
				})
			case doneTxIndex := <-doneTxC:
				shrinkDag(doneTxIndex, dagRemain)
				txIndexBatch := popNextTxBatchFromDag(dagRemain)
				//logger.Debugf("block [%d] schedule with dag, pop next tx index batch size:%d", block.Number(), len(txIndexBatch))
				for _, tx := range txIndexBatch {
					runningTxC <- tx
				}
				//logger.Debugf("shrinkDag and pop next tx batch size:%d, dagRemain size:%d", len(txIndexBatch), len(dagRemain))
			case <-finishC:
				logger.Debugf("block [%d] schedule with dag finish", block.Number())
				scheduleFinishC <- true
				return
			}
		}
	}()

	txIndexBatch := popNextTxBatchFromDag(dagRemain)
	logger.Debugf("simulate with dag first batch size:%d, total batch size:%d", len(txIndexBatch), txBatchSize)
	for _, tx := range txIndexBatch {
		runningTxC <- tx
	}
	<-scheduleFinishC

	// 计算时间的时候，根据最后 finished 的txindx 找出一条路径，将路径上的复制时间减去
	executeTime := time.Since(executeStart)
	// logger.Infof("the execute time is %+v", executeTime)
	var copytimes time.Duration
	for _, v := range reachMap[lastFinished].Pos1() {
		copytimes += copyObjectsTimes[v]
		// logger.Infof("the copy time is %+v", copytimes)
	}
	*sum += executeTime - copytimes
	// logger.Infof("simulate time with dag finished, size %d, execute time used %+v", len(block.Transactions()), executeTime - copytimes)
	return copytimes, err
}

func shrinkDag(txIndex int, dagRemain map[int]dagNeighbors) {
	for _, neighbors := range dagRemain {
		delete(neighbors, txIndex)
	}
}

func popNextTxBatchFromDag(dagRemain map[int]dagNeighbors) []int {
	var txIndexBatch []int
	for checkIndex, neighbors := range dagRemain {
		if len(neighbors) == 0 {
			txIndexBatch = append(txIndexBatch, checkIndex)
			delete(dagRemain, checkIndex)
		}
	}
	return txIndexBatch
}

func applyTransactionWithoutConflict(msg types.Message, gp *GasPool, statedb *state.StateDB, evm *vm.EVM) {
	txContext := NewEVMTxContext(msg)
	evm.Reset(txContext, statedb)
	ApplyMessage(evm, msg, gp)
}

func applyTransaction2(msg types.Message, config *params.ChainConfig, bc ChainContext, author *common.Address, gp *GasPool, db2 *state.StateDB, blockNumber *big.Int, blockHash common.Hash, tx *types.Transaction, usedGas *uint64, evm *vm.EVM) (map[common.Address]struct{}, *ExecutionResult, time.Duration, error) {
	// start := time.Now()
	txContext := NewEVMTxContext(msg)
	evm.Reset(txContext, db2)
	// reTime := time.Since(start)
	result, err := ApplyMessage(evm, msg, gp /*, db2*/)
	return db2.GetStateObjectsPending(), result, 0, err
}

func applyTransactionP(msg types.Message, config *params.ChainConfig, bc ChainContext, author *common.Address, gp *GasPool, db2 *state.StateDB, blockNumber *big.Int, blockHash common.Hash, tx *types.Transaction, usedGas *uint64, evm *vm.EVM) {
	// txContext := NewEVMTxContext(msg)
	// evm.Reset(txContext, db2)
	// ApplyMessage(evm, msg, gp)
	//time.Sleep(time.Duration(100) * time.Nanosecond)
}

// ApplyTransaction attempts to apply a transaction to the given state database
// and uses the input parameters for its environment. It returns the receipt
// for the transaction, gas used and an error if the transaction failed,
// indicating the block was invalid.
func ApplyTransaction(config *params.ChainConfig, bc ChainContext, author *common.Address, gp *GasPool, statedb *state.StateDB, header *types.Header, tx *types.Transaction, usedGas *uint64, cfg vm.Config) (*types.Receipt, error) {
	msg, err := tx.AsMessage(types.MakeSigner(config, header.Number), header.BaseFee)
	if err != nil {
		return nil, err
	}
	// Create a new context to be used in the EVM environment
	blockContext := NewEVMBlockContext(header, bc, author)
	vmenv := vm.NewEVM(blockContext, vm.TxContext{}, statedb, config, cfg)
	return applyTransaction(msg, config, bc, author, gp, statedb, header.Number, header.Hash(), tx, usedGas, vmenv)
}

func (p *StateProcessor) ReplayAndReexecute(block *types.Block, statedb *state.StateDB, spe_statedb *state.StateDB, cfg vm.Config, sum *time.Duration) (float64, error) {
	var (
		usedGas     = new(uint64)
		header      = block.Header()
		blockHash   = block.Hash()
		blockNumber = block.Number()
		gp          = new(GasPool).AddGas(block.GasLimit())
		logger      = ethLogger.NewZeroLogger()
	)
	if p.config.DAOForkSupport && p.config.DAOForkBlock != nil && p.config.DAOForkBlock.Cmp(block.Number()) == 0 {
		misc.ApplyDAOHardFork(statedb)
	}
	var (
		copyStateDB = make([]*state.StateDB, block.Transactions().Len())
		txMapping   = make(map[int]*types.Transaction)
		txBatchSize = len(block.Transactions())
		runningTxC  = make(chan int, txBatchSize)
		// runningTxC       = make(chan int, runtime.NumCPU())
		doneTxC          = make(chan int, txBatchSize)
		finishC          = make(chan bool)
		scheduleFinishC  = make(chan bool)
		poolCapacity     = runtime.NumCPU()
		copyObjectsTimes = make([]time.Duration, block.Transactions().Len())
		lastFinished     int
		needReexecute    = make(chan int, txBatchSize)
		newSet           = make(map[common.Address]map[int]bool)
		// newSet      sync.Map
		finishFlagC = make([]chan bool, block.Transactions().Len())
	)
	for i := 0; i < block.Transactions().Len(); i++ {
		finishFlagC[i] = make(chan bool, 1)
	}

	// ProcessWithoutConflict (Preexecute)
	_, dagRemain, readBitmaps, writeBitmaps, keyDict, reachMap, _, _ := p.ProcessWithoutConflict(block, spe_statedb, cfg)
	// logger.Infof("the dagRemain is %+v", dagRemain)
	var unconflictNum uint64 = 0
	for _, neighbors := range dagRemain {
		if len(neighbors) == 0 {
			unconflictNum++
		}
	}
	// logger.Infof("the unconflictNum is %+v", unconflictNum)

	// Replay
	// if unconflictNum > 3 {
	for index, tx := range block.Transactions() {
		txMapping[index] = tx
	}
	for i := 0; i < block.Transactions().Len(); i++ {
		copyStateDB[i] = statedb.Copy()
		copyStateDB[i].StateCopy(statedb)
	}
	var (
		err        error
		applySize  uint32
		lock       sync.RWMutex
		newSetlock sync.RWMutex
	)
	var goRoutinePool *ants.Pool
	goRoutinePool, _ = ants.NewPool(poolCapacity, ants.WithPreAlloc(true))
	defer goRoutinePool.Release()
	executeStart := time.Now()

	go func() {
		for {
			select {
			case txIndex := <-runningTxC:
				tx := txMapping[txIndex]
				err = goRoutinePool.Submit(func() {
					msg, _ := tx.AsMessage(types.MakeSigner(p.config, header.Number), header.BaseFee)
					blockContext := NewEVMBlockContext(header, p.bc, nil)
					start1 := time.Now()
					lock.RLock()
					copydb := copyStateDB[txIndex]
					lock.RUnlock()
					end1 := time.Since(start1)
					vmenv := vm.NewEVM(blockContext, vm.TxContext{}, copydb, p.config, cfg)
					copydb.Prepare(tx.Hash(), txIndex)
					applyTransaction2(msg, p.config, p.bc, nil, gp, copydb, blockNumber, blockHash, tx, usedGas, vmenv)

					readChanged, readNew, newReadAddr := isRChanged(readBitmaps, writeBitmaps, keyDict, copydb.Readset, txIndex)
					writeChanged, writeNew, newWriteAddr := isWChanged(writeBitmaps, keyDict, copydb.Writeset, txIndex)
					if readChanged || writeChanged {
						needReexecute <- txIndex
					} else if readNew || writeNew {
						if txIndex > 0 {
							<-finishFlagC[txIndex-1]
						}
						newSetlock.Lock()
						for _, addr := range newWriteAddr {
							if _, ok := newSet[addr]; !ok {
								newSet[addr] = make(map[int]bool)
							}
							newSet[addr][txIndex] = true
						}
						newSetlock.Unlock()
						reFlag := false
						for i := 0; i < txIndex; i++ {
							for _, addr := range newReadAddr {
								newSetlock.RLock()
								_, ok := newSet[addr][i]
								newSetlock.RUnlock()
								if ok {
									lock.RLock()
									copydb := copyStateDB[txIndex]
									lock.RUnlock()
									vmenv := vm.NewEVM(blockContext, vm.TxContext{}, copydb, p.config, cfg)
									copydb.Prepare(tx.Hash(), txIndex)
									applyTransaction2(msg, p.config, p.bc, nil, gp, copydb, blockNumber, blockHash, tx, usedGas, vmenv)
									copydb.IntermediateRoot_Re(p.config.IsEIP158(blockNumber))
									reFlag = true
									break
								}
							}
							if reFlag {
								break
							}
						}
						if !reFlag {
							copydb.IntermediateRoot_Re(p.config.IsEIP158(blockNumber))
						}
					} else {
						copydb.IntermediateRoot_Re(p.config.IsEIP158(blockNumber))
					}

					atomic.AddUint32(&applySize, 1)
					copyObjectsTimes[txIndex] = end1
					doneTxC <- txIndex
					close(finishFlagC[txIndex])
					if applySize >= (uint32)(len(block.Transactions())) {
						lastFinished = txIndex
						finishC <- true
					}
				})
			case doneTxIndex := <-doneTxC:
				shrinkDag(doneTxIndex, dagRemain)
				txIndexBatch := popNextTxBatchFromDag(dagRemain)
				logger.Debugf("block [%d] schedule with dag, pop next tx index batch size:%d", block.Number(), len(txIndexBatch))
				for _, tx := range txIndexBatch {
					runningTxC <- tx
				}
				logger.Debugf("shrinkDag and pop next tx batch size:%d, dagRemain size:%d", len(txIndexBatch), len(dagRemain))
			case <-finishC:
				logger.Debugf("block [%d] schedule with dag finish", block.Number())
				scheduleFinishC <- true
				return
			}
		}
	}()

	txIndexBatch := popNextTxBatchFromDag(dagRemain)
	logger.Debugf("simulate with dag first batch size:%d, total batch size:%d", len(txIndexBatch), txBatchSize)
	for _, tx := range txIndexBatch {
		runningTxC <- tx
	}
	<-scheduleFinishC

	// 计算时间的时候，根据最后 finished 的txindx 找出一条路径，将路径上的复制时间减去
	executeTime := time.Since(executeStart)
	// logger.Infof("the execute time is %+v", executeTime)
	var copytimes time.Duration
	for _, v := range reachMap[lastFinished].Pos1() {
		copytimes += copyObjectsTimes[v]
		// logger.Infof("the copy time is %+v", copytimes)
	}
	*sum += executeTime - copytimes
	logger.Infof("simulate time with dag finished, size %d, execute time used %+v", len(block.Transactions()), executeTime-copytimes)

	// Reexecute
	lock.RLock()
	copydb := copyStateDB[lastFinished]
	lock.RUnlock()
	// logger := ethLogger.NewZeroLogger()
	reExecuteStart := time.Now()
	reExecuteNum := len(needReexecute)
	for i := 0; i < reExecuteNum; i++ {
		reIndex := <-needReexecute
		tx := txMapping[reIndex]
		//logger.Infof("block: %v, transaction need to be re-executed: %+v\n", blockNumber, tx.Hash())
		msg, _ := tx.AsMessage(types.MakeSigner(p.config, header.Number), header.BaseFee)
		blockContext := NewEVMBlockContext(header, p.bc, nil)
		vmenv := vm.NewEVM(blockContext, vm.TxContext{}, copydb, p.config, cfg)
		copydb.Prepare(tx.Hash(), reIndex)
		applyTransaction_Re(msg, p.config, p.bc, nil, gp, copydb, blockNumber, blockHash, tx, usedGas, vmenv)
	}
	reExecuteTime := time.Since(reExecuteStart)
	*sum += reExecuteTime
	rate := float64(reExecuteNum) / float64(len(block.Transactions()))
	logger.Infof("the rate of transactions need re-execution is %.4f, time of parallel is %+v time of reexecute is %+v\n", rate, executeTime-copytimes, reExecuteTime)

	root := (*copydb.Trie).Hash()
	logger.Infof("the trie root is %v\n", root)
	//if root == header.Root {
	//	logger.Infof("the execution result is the same as Ethereum\n")
	//} else {
	//	logger.Infof("the execution result is not the same as Ethereum\n")
	//}

	return rate, err
	// } else {
	// 	// start := time.Now()
	// 	p.ProcessSerial(block, statedb, cfg, sum)
	// 	return 0, nil
	// }
}

func isWChanged(preWBitmaps []*bitmap.Bitmap, keyDict map[string]int,
	newWSet map[common.Address]struct{}, txIndex int) (bool, bool, []common.Address) {
	var (
		preWTableDict = make(map[int]bool)
		newWTableDict = make(map[int]bool)
		writeChanged  bool
		writeNew      bool
		newWriteAdd   []common.Address
	)

	for _, v := range preWBitmaps[txIndex].Pos1() {
		preWTableDict[v] = true
	}

	for k := range newWSet {
		if addrIndex, ok := keyDict[string(k[:])]; ok {
			newWTableDict[addrIndex] = true
		} else {
			writeNew = true
			newWriteAdd = append(newWriteAdd, k)
		}
	}

	for dict := range newWTableDict {
		if _, ok := preWTableDict[dict]; !ok {
			writeChanged = true
			break
		}
	}

	return writeChanged, writeNew, newWriteAdd
}

func isRChanged(preRBitmaps []*bitmap.Bitmap, preWBitmaps []*bitmap.Bitmap, keyDict map[string]int,
	newRSet map[common.Address]struct{}, txIndex int) (bool, bool, []common.Address) {
	var (
		preRTableDict = make(map[int]bool)
		newRTableDict = make(map[int]bool)
		wDict         = make(map[int]bool)
		readChanged   bool
		readNew       bool
		newReadAdd    []common.Address
	)

	for _, v := range preRBitmaps[txIndex].Pos1() {
		preRTableDict[v] = true
	}

	for k := range newRSet {
		if addrIndex, ok := keyDict[string(k[:])]; ok {
			newRTableDict[addrIndex] = true
		} else {
			readNew = true
			newReadAdd = append(newReadAdd, k)
		}
	}

	for _, wBitmap := range preWBitmaps {
		for _, v := range wBitmap.Pos1() {
			wDict[v] = true
		}
	}

	for dict := range newRTableDict {
		if _, ok := preRTableDict[dict]; !ok {
			if _, ok := wDict[dict]; !ok {
				continue
			}
			readChanged = true
		}
	}

	return readChanged, readNew, newReadAdd
}

func applyTransaction_Re(msg types.Message, config *params.ChainConfig, bc ChainContext, author *common.Address, gp *GasPool, db2 *state.StateDB, blockNumber *big.Int, blockHash common.Hash, tx *types.Transaction, usedGas *uint64, evm *vm.EVM) (map[common.Address]struct{}, *ExecutionResult, time.Duration, error) {
	// start := time.Now()
	txContext := NewEVMTxContext(msg)
	evm.Reset(txContext, db2)
	// reTime := time.Since(start)
	result, err := ApplyMessage(evm, msg, gp /*, db2*/)
	stateObjectsPending := db2.GetStateObjectsPending()

	if config.IsByzantium(blockNumber) {
		db2.Finalise(true)
	} else {
		db2.IntermediateRoot_Re(config.IsEIP158(blockNumber))
	}

	return stateObjectsPending, result, 0, err
}

func logRWSet(preRBitmaps []*bitmap.Bitmap, preWBitmaps []*bitmap.Bitmap, keyDict map[string]int,
	newRSet map[common.Address]struct{}, newWSet map[common.Address]struct{}, txIndex int) {
	logger := ethLogger.NewZeroLogger()
	dict := make(map[int]string)
	for k, v := range keyDict {
		dict[v] = k
	}
	for _, i := range preRBitmaps[txIndex].Pos1() {
		logger.Infof("speculate read set: %v\n", dict[i])
	}
	for _, i := range preWBitmaps[txIndex].Pos1() {
		logger.Infof("speculate write set: %v\n", dict[i])
	}

	for addr := range newRSet {
		logger.Infof("replay read set: %v\n", addr)
	}
	for addr := range newWSet {
		logger.Infof("replay write set: %v\n", addr)
	}
}

func (p *StateProcessor) Speculate(block *types.Block, statedb *state.StateDB, cfg vm.Config) *ConsensusContent {
	dagDeps, txChain, readBitmaps, writeBitmaps, keyDict, _, _, _ := p.PreExecute(block, statedb, cfg)
	b, _ := block.EncodeToBytes()
	content := &ConsensusContent{
		DagDeps:      dagDeps,
		ReadBitmaps:  readBitmaps,
		WriteBitmaps: writeBitmaps,
		KeyDict:      keyDict,
		TxChain:      txChain,
		BlockNum:     block.Number(),
		BytesOfBlock: b,
	}
	return content
}

func (p *StateProcessor) Parallel(cc *ConsensusContent, statedb *state.StateDB, cfg vm.Config, sum *time.Duration) error {
	var (
		dagDeps = cc.DagDeps
		txChain = cc.TxChain
		block   = new(types.Block)
	)
	block.DecodeBytes(cc.BytesOfBlock)
	var (
		usedGas     = new(uint64)
		header      = block.Header()
		blockHash   = block.Hash()
		blockNumber = block.Number()
		gp          = new(GasPool).AddGas(block.GasLimit())
		//logger      = ethLogger.NewZeroLogger()
	)
	if p.config.DAOForkSupport && p.config.DAOForkBlock != nil && p.config.DAOForkBlock.Cmp(block.Number()) == 0 {
		misc.ApplyDAOHardFork(statedb)
	}
	var (
		copyStateDB      = make([]*state.StateDB, block.Transactions().Len())
		txMapping        = make(map[int]*types.Transaction)
		txBatchSize      = len(block.Transactions())
		runningTxC       = make(chan int, txBatchSize+2)
		doneTxC          = make(chan int, txBatchSize)
		finishC          = make(chan bool)
		scheduleFinishC  = make(chan bool)
		poolCapacity     = 16 //runtime.NumCPU()
		copyObjectsTimes = make([]time.Duration, block.Transactions().Len())
	)

	for index, tx := range block.Transactions() {
		txMapping[index] = tx
	}
	//copyStart := time.Now()
	for i := 0; i < block.Transactions().Len(); i++ {
		copyStateDB[i] = statedb.Copy()
		copyStateDB[i].StateCopy(statedb)
	}
	//copyTime := time.Since(copyStart)

	var (
		err       error
		applySize uint32
		lock      sync.RWMutex
	)

	var goRoutinePool *ants.Pool
	goRoutinePool, _ = ants.NewPool(poolCapacity, ants.WithPreAlloc(true))
	defer goRoutinePool.Release()
	totalNum := (uint32)(len(block.Transactions()))
	txIndexBatch := popNextBatch(dagDeps)
	sort.Ints(txIndexBatch)
	for _, tx := range txIndexBatch {
		runningTxC <- tx
	}
	executeStart := time.Now()
	go func() {
		for _, txIndex := range txChain {
			tx := txMapping[txIndex]
			start1 := time.Now()
			lock.RLock()
			copydb := copyStateDB[txIndex]
			lock.RUnlock()
			end1 := time.Since(start1)
			copydb.Prepare(tx.Hash(), txIndex)
			msg, _ := tx.AsMessage(types.MakeSigner(p.config, header.Number), header.BaseFee)
			blockContext := NewEVMBlockContext(header, p.bc, nil)
			vmenv := vm.NewEVM(blockContext, vm.TxContext{}, copydb, p.config, cfg)
			applyTransaction2(msg, p.config, p.bc, nil, gp, copydb, blockNumber, blockHash, tx, usedGas, vmenv)
			atomic.AddUint32(&applySize, 1)
			copyObjectsTimes[txIndex] = end1
			doneTxC <- txIndex
			if applySize >= totalNum {
				finishC <- true
			}
		}
	}()

	go func() {
		for {
			select {
			case txIndex := <-runningTxC:
				tx := txMapping[txIndex]
				err = goRoutinePool.Submit(func() {
					start1 := time.Now()
					lock.RLock()
					copydb := copyStateDB[txIndex]
					lock.RUnlock()
					end1 := time.Since(start1)
					copydb.Prepare(tx.Hash(), txIndex)
					msg, _ := tx.AsMessage(types.MakeSigner(p.config, header.Number), header.BaseFee)
					blockContext := NewEVMBlockContext(header, p.bc, nil)
					vmenv := vm.NewEVM(blockContext, vm.TxContext{}, copydb, p.config, cfg)
					applyTransactionP(msg, p.config, p.bc, nil, gp, copydb, blockNumber, blockHash, tx, usedGas, vmenv)
					atomic.AddUint32(&applySize, 1)
					copyObjectsTimes[txIndex] = end1
					doneTxC <- txIndex
					if applySize >= totalNum {
						finishC <- true
					}
				})
			case doneTxIndex := <-doneTxC:
				shrinkDAGDeps(doneTxIndex, dagDeps)
				txIndexBatch := popNextBatch(dagDeps)
				for _, tx := range txIndexBatch {
					runningTxC <- tx
				}
			case <-finishC:
				//logger.Debugf("block [%d] schedule with dag finish", block.Number())
				scheduleFinishC <- true
				return
			}
		}
	}()
	<-scheduleFinishC

	executeTime := time.Since(executeStart)

	var copytimes time.Duration
	for _, v := range txChain {
		copytimes += copyObjectsTimes[v]
		// logger.Infof("the copy time is %+v", copytimes)
	}
	*sum += executeTime - copytimes
	return err
}

func buildBitmapsPerTx(ReadkeysSet map[int]map[string]struct{}, WritekeysSet map[int]map[string]struct{}) ([]*bitmap.Bitmap, []*bitmap.Bitmap, map[string]int) {
	dictIndex := 0
	txCount := len(ReadkeysSet)
	readBitmap := make([]*bitmap.Bitmap, txCount)
	writeBitmap := make([]*bitmap.Bitmap, txCount)
	keyDict := make(map[string]int, 1024)
	for i := 0; i < txCount; i++ {
		readTableItemForI := make([]string, 0, len(ReadkeysSet[i]))
		for k := range ReadkeysSet[i] {
			readTableItemForI = append(readTableItemForI, k)
		}
		writeTableItemForI := make([]string, 0, len(WritekeysSet[i]))
		for k := range WritekeysSet[i] {
			writeTableItemForI = append(writeTableItemForI, k)
		}

		readBitmap[i] = &bitmap.Bitmap{}
		for _, keyForI := range readTableItemForI {
			if existIndex, ok := keyDict[keyForI[:]]; !ok {
				keyDict[keyForI[:]] = dictIndex
				readBitmap[i].Set(dictIndex)
				dictIndex++
			} else {
				readBitmap[i].Set(existIndex)
			}
		}

		writeBitmap[i] = &bitmap.Bitmap{}
		for _, keyForI := range writeTableItemForI {
			if existIndex, ok := keyDict[keyForI[:]]; !ok {
				keyDict[keyForI[:]] = dictIndex
				writeBitmap[i].Set(dictIndex)
				dictIndex++
			} else {
				writeBitmap[i].Set(existIndex)
			}
		}
	}
	return readBitmap, writeBitmap, keyDict
}

func BuildDAGWithKeys(ReadkeysSet map[int]map[string]struct{}, WritekeysSet map[int]map[string]struct{}, blockNumber *big.Int) (*DAG, []*bitmap.Bitmap, []*bitmap.Bitmap, map[string]int, []*bitmap.Bitmap) {
	txCount := len(ReadkeysSet)
	// build read-write bitmap for all transactions
	readBitmaps, writeBitmaps, keyDict := buildBitmapsPerTx(ReadkeysSet, WritekeysSet)
	cumulativeReadBitmap, cumulativeWriteBitmap := buildCumulativeBitmap(readBitmaps, writeBitmaps)

	dag := &DAG{}
	if txCount == 0 {
		return dag, nil, nil, nil, nil
	}
	dag.Vertexes = make([]*DAG_Neighbor, txCount)

	// build DAG base on read and write bitmaps
	// reachMap describes reachability from tx i to tx j in DAG.
	// For example, if the DAG is tx3 -> tx2 -> tx1 -> begin, the reachMap is
	// 		tx1		tx2		tx3
	// tx1	0		0		0
	// tx2	1		0		0
	// tx3	1		1		0
	reachMap := make([]*bitmap.Bitmap, txCount)

	for i := 0; i < txCount; i++ {
		// 1. get read and write bitmap for tx i
		readBitmapForI := readBitmaps[i]
		writeBitmapForI := writeBitmaps[i]
		// directReachFromI is used to build DAG, it's the direct neighbors of the ith tx
		directReachFromI := &bitmap.Bitmap{}
		// reachFromI is used to save reachability we have already known, it's the all neighbors of the ith tx
		reachFromI := &bitmap.Bitmap{}
		reachFromI.Set(i)

		if i > 0 && fastConflicted(
			readBitmapForI, writeBitmapForI, cumulativeReadBitmap[i-1], cumulativeWriteBitmap[i-1]) {
			buildReach(i, reachFromI, readBitmaps, writeBitmaps, readBitmapForI, writeBitmapForI, directReachFromI, reachMap)
		}
		reachMap[i] = reachFromI

		// build DAG based on directReach bitmap
		dag.Vertexes[i] = &DAG_Neighbor{
			Neighbors: make([]uint32, 0, 16),
		}
		for _, j := range directReachFromI.Pos1() {
			dag.Vertexes[i].Neighbors = append(dag.Vertexes[i].Neighbors, uint32(j))
		}
	}
	return dag, readBitmaps, writeBitmaps, keyDict, reachMap
}

func findMostFrequentKey(ReadkeysSet map[int]map[string]struct{}, WritekeysSet map[int]map[string]struct{}) (string, []int) {
	keysAccessCount := make(map[string][]int)
	for index, readKeys := range ReadkeysSet {
		for key := range readKeys {
			keysAccessCount[key] = append(keysAccessCount[key], index)
		}
	}
	for index, writeKeys := range WritekeysSet {
		for key := range writeKeys {
			if _, ok := ReadkeysSet[index][key]; !ok {
				keysAccessCount[key] = append(keysAccessCount[key], index)
			}
		}
	}

	var mostFrequentKey string
	max := 0
	for key, txList := range keysAccessCount {
		if len(txList) > max {
			mostFrequentKey = key
			max = len(txList)
		}
	}
	return mostFrequentKey, keysAccessCount[mostFrequentKey]
}

func (p *StateProcessor) PreExecute(block *types.Block, statedb *state.StateDB,
	cfg vm.Config) (map[int]map[int]int, []int, []*bitmap.Bitmap, []*bitmap.Bitmap, map[string]int, []*bitmap.Bitmap, time.Duration, error) {

	var (
		// logger = ethLogger.NewZeroLogger()
		header = block.Header()
		gp     = new(GasPool).AddGas(block.GasLimit())
		wg     sync.WaitGroup
		lock   sync.RWMutex
	)

	// Mutate the block and state according to any hard-fork specs
	if p.config.DAOForkSupport && p.config.DAOForkBlock != nil && p.config.DAOForkBlock.Cmp(block.Number()) == 0 {
		misc.ApplyDAOHardFork(statedb)
	}

	var (
		copyStateDB  = make([]*state.StateDB, block.Transactions().Len())
		ReadkeysSet  = make(map[int]map[string]struct{})
		WritekeysSet = make(map[int]map[string]struct{})
	)
	for i := 0; i < block.Transactions().Len(); i++ {
		copyStateDB[i] = statedb.Copy()
		//copyStateDB[i].StateCopy(statedb)
	}

	var goRoutinePool *ants.Pool
	var poolCapacity = runtime.NumCPU()
	goRoutinePool, _ = ants.NewPool(poolCapacity, ants.WithPreAlloc(true))
	wg.Add(block.Transactions().Len())
	start := time.Now()
	for i, tx := range block.Transactions() {
		tx := tx
		i := i
		goRoutinePool.Submit(func() {
			msg, _ := tx.AsMessage(types.MakeSigner(p.config, header.Number), header.BaseFee)
			blockContext := NewEVMBlockContext(header, p.bc, nil)
			lock.RLock()
			copydb := copyStateDB[i]
			lock.RUnlock()
			vmenv := vm.NewEVM(blockContext, vm.TxContext{}, copydb, p.config, cfg)
			copydb.Prepare(tx.Hash(), i)
			applyTransactionWithoutConflict(msg, gp, copydb, vmenv)

			lock.Lock()
			if ReadkeysSet[i] == nil {
				ReadkeysSet[i] = make(map[string]struct{})
				ReadkeysSet[i] = copydb.Readkeys
			}
			if WritekeysSet[i] == nil {
				WritekeysSet[i] = make(map[string]struct{})
				WritekeysSet[i] = copydb.Writekeys
			}
			lock.Unlock()
			wg.Done()
		})
	}
	wg.Wait()
	time := time.Since(start)
	_, readBitmaps, writeBitmaps, keyDict, reachMap := BuildDAGWithKeys(ReadkeysSet, WritekeysSet, block.Number())
	// Construct the adjacency list of dag, which describes the subsequent adjacency transactions of all transactions
	// dagRemain := make(map[int]dagNeighbors)
	// for txIndex, neighbors := range dag.Vertexes {
	// 	dn := make(dagNeighbors)
	// 	for _, neighbor := range neighbors.Neighbors {
	// 		dn[int(neighbor)] = true
	// 	}
	// 	dagRemain[txIndex] = dn
	// }
	dagDeps := BuildDAGShowDependencies(readBitmaps, writeBitmaps)
	_, txChain := findMostFrequentKey(ReadkeysSet, WritekeysSet)
	for _, index := range txChain {
		for neighbor, dep := range dagDeps[index] {
			if dep != 1 {
				dep = 5 - dep
			}
			if dagDeps[neighbor] != nil {
				dagDeps[neighbor][index] = dep
			} else {
				dn := make(map[int]int)
				dn[index] = dep
				dagDeps[neighbor] = dn
			}
		}
	}
	for _, index := range txChain {
		delete(dagDeps, index)
	}

	// time := time.Since(start)
	// logger.Infof("analysis time is: %+v", time)

	// txBatchSize := len(block.Transactions())
	// txIndexBatch := popNextTxBatchFromDag(dagRemain)
	// logger.Debugf("simulate with dag first batch size:%d, total batch size:%d", len(txIndexBatch), txBatchSize)

	// longestPath := 0
	// for i := 0; i < len(reachMap); i++ {
	// 	if len(reachMap[i].Pos1()) > longestPath {
	// 		longestPath = len(reachMap[i].Pos1())
	// 	}
	// }
	// logger.Debugf("the longest reach path is: %d", longestPath)

	return dagDeps, txChain, readBitmaps, writeBitmaps, keyDict, reachMap, time, nil
}

func (p *StateProcessor) ProcessWithDeps(block *types.Block, statedb *state.StateDB, cfg vm.Config, sum *time.Duration, analyzeSum *time.Duration) (int, time.Duration, error) {
	var (
		usedGas     = new(uint64)
		header      = block.Header()
		blockHash   = block.Hash()
		blockNumber = block.Number()
		gp          = new(GasPool).AddGas(block.GasLimit())
		//logger      = ethLogger.NewZeroLogger()
	)
	if p.config.DAOForkSupport && p.config.DAOForkBlock != nil && p.config.DAOForkBlock.Cmp(block.Number()) == 0 {
		misc.ApplyDAOHardFork(statedb)
	}
	var (
		copyStateDB = make([]*state.StateDB, block.Transactions().Len())
		txMapping   = make(map[int]*types.Transaction)
		txBatchSize = len(block.Transactions())
		// runningTxC       = make(chan int, txBatchSize)
		runningTxC       = make(chan int, txBatchSize+2)
		doneTxC          = make(chan int, txBatchSize+2)
		finishC          = make(chan bool)
		scheduleFinishC  = make(chan bool)
		poolCapacity     = runtime.NumCPU()
		copyObjectsTimes = make([]time.Duration, block.Transactions().Len())
		//lastFinished     int
	)

	// ProcessWithoutConflict
	dagDeps, txChain, _, _, _, _, analyzeTime, _ := p.PreExecute(block, statedb, cfg)
	*analyzeSum += analyzeTime
	sort.Ints(txChain)

	for index, tx := range block.Transactions() {
		txMapping[index] = tx
	}
	for i := 0; i < block.Transactions().Len(); i++ {
		copyStateDB[i] = statedb.Copy()
		copyStateDB[i].StateCopy(statedb)
	}
	var (
		err       error
		applySize uint32
		lock      sync.RWMutex
	)
	var goRoutinePool *ants.Pool
	goRoutinePool, _ = ants.NewPool(poolCapacity, ants.WithPreAlloc(true))
	defer goRoutinePool.Release()
	totalNum := (uint32)(len(block.Transactions()))
	txIndexBatch := popNextBatch(dagDeps)
	sort.Ints(txIndexBatch)
	//logger.Debugf("txBatch: %v", txIndexBatch)
	// logger.Debugf("simulate with dag first batch size:%d, total batch size:%d", len(txIndexBatch), txBatchSize)
	for _, tx := range txIndexBatch {
		runningTxC <- tx
	}

	executeStart := time.Now()
	go func() {
		//logger.Debugf("txChain: %v", txChain)
		for _, txIndex := range txChain {
			tx := txMapping[txIndex]
			start1 := time.Now()
			lock.RLock()
			copydb := copyStateDB[txIndex]
			lock.RUnlock()
			end1 := time.Since(start1)
			copydb.Prepare(tx.Hash(), txIndex)
			msg, _ := tx.AsMessage(types.MakeSigner(p.config, header.Number), header.BaseFee)
			blockContext := NewEVMBlockContext(header, p.bc, nil)
			vmenv := vm.NewEVM(blockContext, vm.TxContext{}, copydb, p.config, cfg)
			applyTransaction2(msg, p.config, p.bc, nil, gp, copydb, blockNumber, blockHash, tx, usedGas, vmenv)
			atomic.AddUint32(&applySize, 1)
			copyObjectsTimes[txIndex] = end1
			doneTxC <- txIndex
			if applySize >= totalNum {
				//lastFinished = txIndex
				finishC <- true
			}
		}
	}()

	go func() {
		for {
			select {
			case txIndex := <-runningTxC:
				tx := txMapping[txIndex]
				err = goRoutinePool.Submit(func() {
					//start1 := time.Now()
					lock.RLock()
					copydb := copyStateDB[txIndex]
					lock.RUnlock()
					//end1 := time.Since(start1)
					copydb.Prepare(tx.Hash(), txIndex)
					msg, _ := tx.AsMessage(types.MakeSigner(p.config, header.Number), header.BaseFee)
					blockContext := NewEVMBlockContext(header, p.bc, nil)
					vmenv := vm.NewEVM(blockContext, vm.TxContext{}, copydb, p.config, cfg)
					applyTransactionP(msg, p.config, p.bc, nil, gp, copydb, blockNumber, blockHash, tx, usedGas, vmenv)
					atomic.AddUint32(&applySize, 1)
					//copyObjectsTimes[txIndex] = end1
					doneTxC <- txIndex
					if applySize >= totalNum {
						//lastFinished = txIndex
						finishC <- true
					}
				})
			case doneTxIndex := <-doneTxC:
				shrinkDAGDeps(doneTxIndex, dagDeps)
				txIndexBatch := popNextBatch(dagDeps)
				//sort.Ints(txIndexBatch)
				//logger.Debugf("len of dagDeps: %d", len(dagDeps))
				//logger.Debugf("block [%d] schedule with dag, pop next tx index batch size:%d", block.Number(), len(txIndexBatch))
				for _, tx := range txIndexBatch {
					runningTxC <- tx
				}
				//logger.Debugf("shrinkDag and pop next tx batch size:%d, dagRemain size:%d", len(txIndexBatch), len(dagRemain))
			case <-finishC:
				//logger.Debugf("block [%d] schedule with dag finish", block.Number())
				scheduleFinishC <- true
				return
			}
		}
	}()

	<-scheduleFinishC

	// 计算时间的时候，根据最后 finished 的txindx 找出一条路径，将路径上的复制时间减去
	executeTime := time.Since(executeStart)
	// logger.Infof("the execute time is %+v", executeTime)
	var copytimes time.Duration
	for _, v := range txChain {
		copytimes += copyObjectsTimes[v]
		// logger.Infof("the copy time is %+v", copytimes)
	}
	*sum += executeTime - copytimes
	// logger.Infof("simulate time with dag finished, size %d, execute time used %+v", len(block.Transactions()), executeTime - copytimes)
	return len(txChain), copytimes, err
}

func (p *StateProcessor) ProcessAriaReorderFB(block *types.Block, statedb *state.StateDB, cfg vm.Config, sum *time.Duration, executeTime *time.Duration, reExecuteCh chan *types.Transaction, fbexecute *int, reexecute *int) error {
	var (
		header = block.Header()
		gp     = new(GasPool).AddGas(block.GasLimit())
	)
	if p.config.DAOForkSupport && p.config.DAOForkBlock != nil && p.config.DAOForkBlock.Cmp(block.Number()) == 0 {
		misc.ApplyDAOHardFork(statedb)
	}

	var (
		logger       = ethLogger.NewZeroLogger()
		txMapping    = make(map[int]*types.Transaction)
		txBatchSize  = len(block.Transactions()) + len(reExecuteCh)
		rerunningTxC = make(chan int, txBatchSize)
		copyStateDB  = make([]*state.StateDB, txBatchSize)
		writes       = make(map[common.Address]int, 1024)
		reads        = make(map[common.Address]int, 1024)
		poolCapacity = runtime.NumCPU() * 2
		lockwrite    sync.RWMutex
		lockdb       sync.RWMutex
		lockmap      sync.RWMutex
		wg           sync.WaitGroup
		wgConflicts  sync.WaitGroup
		err          error
	)

	// 提前读取 reExecuteCh 中所有的交易
	initialReExecTxs := []*types.Transaction{}

	done := false // 标志是否退出循环

	for {
		select {
		case tx, ok := <-reExecuteCh:
			if !ok {
				done = true
			} else {
				initialReExecTxs = append(initialReExecTxs, tx)
			}
		default:
			done = true
		}
		if done {
			break
		}
	}

	// 构建 txMapping，reExecuteCh 的交易编号在前
	txIndex := 0
	for _, tx := range initialReExecTxs {
		txMapping[txIndex] = tx
		txIndex++
	}
	for _, tx := range block.Transactions() {
		txMapping[txIndex] = tx
		txIndex++
	}

	type TxReadInfo struct {
		ReadTableItems []common.Address
	}
	type TxWriteInfo struct {
		WriteTableItems []common.Address
	}
	var txReadInfoMap = make(map[int]TxReadInfo)
	var txWriteInfoMap = make(map[int]TxWriteInfo)

	for i := 0; i < txBatchSize; i++ {
		copyStateDB[i] = statedb.Copy()
	}

	logger.Debugln("start The Execution Phase")

	wg.Add(poolCapacity)
	executeStart1 := time.Now()
	for i := 0; i < poolCapacity; i++ {
		idx := i
		go func() {
			//copydb := copyStateDB[idx]
			for txIndex := idx; txIndex < txBatchSize; txIndex += poolCapacity {
				tx := txMapping[txIndex]
				lockdb.RLock()
				copydb := copyStateDB[txIndex]
				lockdb.RUnlock()
				copydb.Readset = make(map[common.Address]struct{})
				copydb.Writeset = make(map[common.Address]struct{})
				copydb.Prepare(tx.Hash(), txIndex)
				msg, _ := tx.AsMessage(types.MakeSigner(p.config, header.Number), header.BaseFee)
				blockContext := NewEVMBlockContext(header, p.bc, nil)
				vmenv := vm.NewEVM(blockContext, vm.TxContext{}, copydb, p.config, cfg)
				applyTransactionWithoutConflict(msg, gp, copydb, vmenv)

				readTableItemForI := make([]common.Address, 0, len(copydb.Readset))
				writeTableItemForI := make([]common.Address, 0, len(copydb.Writeset))

				lockwrite.Lock()
				for k := range copydb.Readset {
					readTableItemForI = append(readTableItemForI, k)
					if tid, ok := reads[k]; ok { // 存在
						if txIndex < tid {
							reads[k] = txIndex
						}
					} else {
						reads[k] = txIndex // 不存在
					}
				}

				for k := range copydb.Writeset {
					writeTableItemForI = append(writeTableItemForI, k)
					if tid, ok := writes[k]; ok { // 存在
						if txIndex < tid {
							writes[k] = txIndex
						}
					} else {
						writes[k] = txIndex // 不存在
					}
				}
				lockwrite.Unlock()

				lockmap.Lock()
				txReadInfoMap[txIndex] = TxReadInfo{
					ReadTableItems: readTableItemForI,
				}

				txWriteInfoMap[txIndex] = TxWriteInfo{
					WriteTableItems: writeTableItemForI,
				}
				lockmap.Unlock()
			}
			wg.Done()
		}()
	}
	wg.Wait()
	executeTime1 := time.Since(executeStart1)

	logger.Debugln("start The Commit Phase")
	var goRoutinePool *ants.Pool
	goRoutinePool, _ = ants.NewPool(160, ants.WithPreAlloc(true))
	defer goRoutinePool.Release()
	wgConflicts.Add(txBatchSize)
	//executeStart2 := time.Now()
	for i := 0; i < txBatchSize; i++ {
		txIndex := i
		goRoutinePool.Submit(func() {
			defer wgConflicts.Done()
			reExecute := false
			lockwrite.RLock()
			readTableItemForI := txReadInfoMap[txIndex].ReadTableItems
			writeTableItemForI := txWriteInfoMap[txIndex].WriteTableItems
			if hasWAW(txIndex, writeTableItemForI, writes) {
				reExecute = true
			}
			if hasRAW(txIndex, readTableItemForI, writes) && hasWAR(txIndex, writeTableItemForI, reads) {
				reExecute = true
			}
			lockwrite.RUnlock()

			if reExecute {
				select {
				case rerunningTxC <- txIndex: // **非阻塞写入**
				default:
				}
			}
		})
	}
	// 等待所有冲突检测完成
	wgConflicts.Wait()
	// //executeTime2 := time.Since(executeStart2)

	close(rerunningTxC)
	reRunningTxLen := len(rerunningTxC)
	logger.Debugf("the reRunningTxLen is :%d", reRunningTxLen)
	*fbexecute = *fbexecute + reRunningTxLen

	// Replay
	var replayTime time.Duration
	//var reTime time.Duration
	if reRunningTxLen != 0 {
		// 从 channel 中获取所有 txIndex
		txIndices := []int{}
		for txIndex := range rerunningTxC {
			txIndices = append(txIndices, txIndex)
		}

		// 对 txIndex 进行排序
		sort.Ints(txIndices)

		// 重新编号并更新关联数据
		newTxIndex := 0

		// 新建 map 存储新编号的内容
		newTxReadInfoMap := make(map[int]TxReadInfo)
		newTxWriteInfoMap := make(map[int]TxWriteInfo)
		newTxMapping := make(map[int]*types.Transaction)

		// 创建新的 map[int]map[common.Address]struct{}
		var ReadSets = make(map[int]map[common.Address]struct{})
		var WriteSets = make(map[int]map[common.Address]struct{})

		for _, oldTxIndex := range txIndices {
			readInfo := txReadInfoMap[oldTxIndex]
			writeInfo := txWriteInfoMap[oldTxIndex]
			tx := txMapping[oldTxIndex]

			newTxReadInfoMap[newTxIndex] = readInfo
			newTxWriteInfoMap[newTxIndex] = writeInfo
			newTxMapping[newTxIndex] = tx

			newTxIndex++
		}

		// 转换 txReadInfoMap 为 map[int]map[common.Address]struct{}
		for txIndex, readInfo := range newTxReadInfoMap {
			readTableItems := make(map[common.Address]struct{})
			for _, addr := range readInfo.ReadTableItems {
				readTableItems[addr] = struct{}{}
			}
			ReadSets[txIndex] = readTableItems
		}

		// 转换 txWriteInfoMap 为 map[int]map[common.Address]struct{}
		for txIndex, writeInfo := range newTxWriteInfoMap {
			writeTableItems := make(map[common.Address]struct{})
			for _, addr := range writeInfo.WriteTableItems {
				writeTableItems[addr] = struct{}{}
			}
			WriteSets[txIndex] = writeTableItems
		}

		_, reExecuteNum, _ := reexcuteAria(newTxMapping, ReadSets, WriteSets, reRunningTxLen, block, statedb, cfg, p, reExecuteCh)
		//reTime = exeTime
		*reexecute = *reexecute + reExecuteNum

		_, replayTime = replayAria(newTxMapping, ReadSets, WriteSets, reRunningTxLen, block, statedb, cfg, p)
	}
	*executeTime += executeTime1
	*sum += executeTime1 + replayTime
	//*sum += executeTime1 + reTime
	return err
}

// WAW(T, writes) = HasConflicts(T.TID, T.WS, writes)
func hasWAW(txIndex int, writeTableItemForI []common.Address, writes map[common.Address]int) bool {
	for _, k := range writeTableItemForI {
		if tid, ok := writes[k]; ok {
			if tid < txIndex {
				return true
			}
		}
	}
	return false
}

// RAW(T, writes) = HasConflicts(T.TID, T.RS, writes)
func hasRAW(txIndex int, readTableItemForI []common.Address, writes map[common.Address]int) bool {
	for _, k := range readTableItemForI {
		if tid, ok := writes[k]; ok {
			if tid < txIndex {
				return true
			}
		}
	}
	return false
}

// WAR(T, reads) = HasConflicts(T.TID, T.WS, reads)
func hasWAR(txIndex int, writeTableItemForI []common.Address, reads map[common.Address]int) bool {
	for _, k := range writeTableItemForI {
		if tid, ok := reads[k]; ok {
			if tid < txIndex {
				return true
			}
		}
	}
	return false
}

func replayAria(newTxMapping map[int]*types.Transaction, ReadSets map[int]map[common.Address]struct{}, WriteSets map[int]map[common.Address]struct{},
	reRunningTxLen int, block *types.Block, statedb *state.StateDB, cfg vm.Config, p *StateProcessor) (error, time.Duration) {
	var (
		usedGas     = new(uint64)
		header      = block.Header()
		blockHash   = block.Hash()
		blockNumber = block.Number()
		gp          = new(GasPool).AddGas(block.GasLimit())
		//logger      = ethLogger.NewZeroLogger()
	)
	var (
		copyStateDB = make([]*state.StateDB, reRunningTxLen)
		//txMapping   = make(map[int]*types.Transaction)
		//txBatchSize = len(block.Transactions())
		// runningTxC       = make(chan int, txBatchSize)
		runningTxC      = make(chan int, reRunningTxLen+2)
		doneTxC         = make(chan int, reRunningTxLen+2)
		finishC         = make(chan bool)
		scheduleFinishC = make(chan bool)
		poolCapacity    = runtime.NumCPU()
		//copyObjectsTimes = make([]time.Duration, block.Transactions().Len())
		//lastFinished     int
	)

	for i := 0; i < reRunningTxLen; i++ {
		copyStateDB[i] = statedb.Copy()
	}
	var (
		err       error
		applySize uint32
		//lock      sync.RWMutex
	)

	dag, _, _, _, _ := BuildDAG(ReadSets, WriteSets, block.Number())
	dagRemain := make(map[int]dagNeighbors)
	for txIndex, neighbors := range dag.Vertexes {
		dn := make(dagNeighbors)
		for _, neighbor := range neighbors.Neighbors {
			dn[int(neighbor)] = true
		}
		dagRemain[txIndex] = dn
	}

	var goRoutinePool *ants.Pool
	goRoutinePool, _ = ants.NewPool(poolCapacity, ants.WithPreAlloc(true))
	defer goRoutinePool.Release()
	totalNum := (uint32)(reRunningTxLen)

	_, txChain := findLongestChain(ReadSets, WriteSets)
	for _, index := range txChain {
		for neighbor, dep := range dagRemain[index] {
			if dagRemain[neighbor] != nil {
				dagRemain[neighbor][index] = dep
			} else {
				dn := make(dagNeighbors)
				dn[index] = dep
				dagRemain[neighbor] = dn
			}
		}
	}
	for _, index := range txChain {
		delete(dagRemain, index)
	}
	sort.Ints(txChain)

	txIndexBatch := popNextTxBatchFromDag(dagRemain)
	sort.Ints(txIndexBatch)
	for _, tx := range txIndexBatch {
		runningTxC <- tx
	}

	executeStart := time.Now()
	go func() {
		//logger.Debugf("txChain: %v", txChain)
		for _, txIndex := range txChain {
			tx := newTxMapping[txIndex]
			//start1 := time.Now()
			//lock.RLock()
			copydb := copyStateDB[txIndex]
			//lock.RUnlock()
			//end1 := time.Since(start1)
			copydb.Prepare(tx.Hash(), txIndex)
			msg, _ := tx.AsMessage(types.MakeSigner(p.config, header.Number), header.BaseFee)
			blockContext := NewEVMBlockContext(header, p.bc, nil)
			vmenv := vm.NewEVM(blockContext, vm.TxContext{}, copydb, p.config, cfg)
			applyTransaction2(msg, p.config, p.bc, nil, gp, copydb, blockNumber, blockHash, tx, usedGas, vmenv)
			atomic.AddUint32(&applySize, 1)
			//copyObjectsTimes[txIndex] = end1
			doneTxC <- txIndex
			if applySize >= totalNum {
				//lastFinished = txIndex
				finishC <- true
			}
		}
	}()

	go func() {
		for {
			select {
			case txIndex := <-runningTxC:
				tx := newTxMapping[txIndex]
				err = goRoutinePool.Submit(func() {
					//start1 := time.Now()
					//lock.RLock()
					copydb := copyStateDB[txIndex]
					//lock.RUnlock()
					//end1 := time.Since(start1)
					copydb.Prepare(tx.Hash(), txIndex)
					msg, _ := tx.AsMessage(types.MakeSigner(p.config, header.Number), header.BaseFee)
					blockContext := NewEVMBlockContext(header, p.bc, nil)
					vmenv := vm.NewEVM(blockContext, vm.TxContext{}, copydb, p.config, cfg)
					applyTransactionP(msg, p.config, p.bc, nil, gp, copydb, blockNumber, blockHash, tx, usedGas, vmenv)
					atomic.AddUint32(&applySize, 1)
					//copyObjectsTimes[txIndex] = end1
					doneTxC <- txIndex
					if applySize >= totalNum {
						//lastFinished = txIndex
						finishC <- true
					}
				})
			case doneTxIndex := <-doneTxC:
				shrinkDag(doneTxIndex, dagRemain)
				txIndexBatch := popNextTxBatchFromDag(dagRemain)
				sort.Ints(txIndexBatch)
				//logger.Debugf("len of dagDeps: %d", len(dagDeps))
				//logger.Debugf("block [%d] schedule with dag, pop next tx index batch size:%d", block.Number(), len(txIndexBatch))
				for _, tx := range txIndexBatch {
					runningTxC <- tx
				}
				//logger.Debugf("shrinkDag and pop next tx batch size:%d, dagRemain size:%d", len(txIndexBatch), len(dagRemain))
			case <-finishC:
				//logger.Debugf("block [%d] schedule with dag finish", block.Number())
				scheduleFinishC <- true
				return
			}
		}
	}()

	<-scheduleFinishC

	executeTime := time.Since(executeStart)

	return err, executeTime
}

func reexcuteAria(newTxMapping map[int]*types.Transaction, ReadSets map[int]map[common.Address]struct{}, WriteSets map[int]map[common.Address]struct{},
	reRunningTxLen int, block *types.Block, statedb *state.StateDB, cfg vm.Config, p *StateProcessor, reExecuteCh chan *types.Transaction) (error, int, time.Duration) {
	var (
		copyStateDB = make([]*state.StateDB, reRunningTxLen)
		//usedGas     = new(uint64)
		header = block.Header()
		//blockHash   = block.Hash()
		//blockNumber = block.Number()
		gp     = new(GasPool).AddGas(block.GasLimit())
		logger = ethLogger.NewZeroLogger()
	)

	var (
		//finishFlagC     = make([]chan bool, reRunningTxLen)
		//newSet          = make(map[common.Address]map[int]bool)
		needReexecute   = make(chan int, reRunningTxLen)
		runningTxC      = make(chan int, reRunningTxLen)
		doneTxC         = make(chan int, reRunningTxLen)
		finishC         = make(chan bool)
		scheduleFinishC = make(chan bool)
		err             error
	)

	var (
		applySize uint32
		lock      sync.RWMutex
		//newSetlock  sync.RWMutex
		usedGas     = new(uint64)
		blockHash   = block.Hash()
		blockNumber = block.Number()
	)
	// for i := range finishFlagC {
	// 	finishFlagC[i] = make(chan bool, 1) // 带缓冲的通道
	// }
	for i := 0; i < reRunningTxLen; i++ {
		copyStateDB[i] = statedb.Copy()
		//copyStateDB[i].StateCopy(statedb)
	}

	var goRoutinePool1 *ants.Pool
	goRoutinePool1, _ = ants.NewPool(runtime.NumCPU()*10, ants.WithPreAlloc(true))
	defer goRoutinePool1.Release()

	//executeStart3 := time.Now()
	dag, readBitmaps, writeBitmaps, keyDict, _ := BuildDAG(ReadSets, WriteSets, block.Number())
	//dag, _, _, _, _ := BuildDAG(ReadSets, WriteSets, block.Number())
	//executeTime3 := time.Since(executeStart3)

	dagRemain := make(map[int]dagNeighbors)
	for txIndex, neighbors := range dag.Vertexes {
		dn := make(dagNeighbors)
		for _, neighbor := range neighbors.Neighbors {
			dn[int(neighbor)] = true
		}
		dagRemain[txIndex] = dn
	}

	executeStart4 := time.Now()
	go func() {
		for {
			select {
			case txIndex := <-runningTxC:
				//logger.Debugf("the tx is :%d", txIndex)
				tx := newTxMapping[txIndex]
				err = goRoutinePool1.Submit(func() {
					msg, _ := tx.AsMessage(types.MakeSigner(p.config, header.Number), header.BaseFee)
					blockContext := NewEVMBlockContext(header, p.bc, nil)
					lock.RLock()
					copydb := copyStateDB[txIndex]
					lock.RUnlock()
					vmenv := vm.NewEVM(blockContext, vm.TxContext{}, copydb, p.config, cfg)
					copydb.Prepare(tx.Hash(), txIndex)
					applyTransaction2(msg, p.config, p.bc, nil, gp, copydb, blockNumber, blockHash, tx, usedGas, vmenv)
					// readChanged, readNew, newReadAddr := isRChanged(readBitmaps, writeBitmaps, keyDict, copydb.Readset, txIndex)
					// writeChanged, writeNew, newWriteAddr := isWChanged(writeBitmaps, keyDict, copydb.Writeset, txIndex)
					readChanged, readNew, _ := isRChanged(readBitmaps, writeBitmaps, keyDict, copydb.Readset, txIndex)
					writeChanged, writeNew, _ := isWChanged(writeBitmaps, keyDict, copydb.Writeset, txIndex)
					if readChanged || writeChanged {
						needReexecute <- txIndex
						logger.Debugf("the re-execute tx %+v", txIndex)
					} else if readNew || writeNew {
						needReexecute <- txIndex
						logger.Debugf("the re-execute tx %+v", txIndex)
						// if txIndex > 0 {
						// 	<-finishFlagC[txIndex-1]
						// }
						// newSetlock.Lock()
						// for _, addr := range newWriteAddr {
						// 	if _, ok := newSet[addr]; !ok {
						// 		newSet[addr] = make(map[int]bool)
						// 	}
						// 	newSet[addr][txIndex] = true
						// }
						// newSetlock.Unlock()
						// reFlag := false
						// for i := 0; i < txIndex; i++ {
						// 	for _, addr := range newReadAddr {
						// 		newSetlock.RLock()
						// 		_, ok := newSet[addr][i]
						// 		newSetlock.RUnlock()
						// 		if ok {
						// 			lock.RLock()
						// 			copydb := copyStateDB[txIndex]
						// 			lock.RUnlock()
						// 			vmenv := vm.NewEVM(blockContext, vm.TxContext{}, copydb, p.config, cfg)
						// 			copydb.Prepare(tx.Hash(), txIndex)
						// 			applyTransaction2(msg, p.config, p.bc, nil, gp, copydb, blockNumber, blockHash, tx, usedGas, vmenv)
						// 			copydb.IntermediateRoot_Re(p.config.IsEIP158(blockNumber))
						// 			reFlag = true
						// 			break
						// 		}
						// 	}
						// 	if reFlag {
						// 		break
						// 	}
						// }
						// if !reFlag {
						// 	copydb.IntermediateRoot_Re(p.config.IsEIP158(blockNumber))
						// }
					} else {
						copydb.IntermediateRoot_Re(p.config.IsEIP158(blockNumber))
					}

					atomic.AddUint32(&applySize, 1)
					doneTxC <- txIndex
					//close(finishFlagC[txIndex])
					if applySize >= (uint32)(reRunningTxLen) {
						finishC <- true
					}
				})
			case doneTxIndex := <-doneTxC:
				shrinkDag(doneTxIndex, dagRemain)
				txIndexBatch := popNextTxBatchFromDag(dagRemain)
				//logger.Debugf("block [%d] schedule with dag, pop next tx index batch size:%d", block.Number(), len(txIndexBatch))
				for _, tx := range txIndexBatch {
					//logger.Infof("the runningTxC111 tx is %+v", tx)
					runningTxC <- tx
				}
				//logger.Debugf("shrinkDag and pop next tx batch size:%d, dagRemain size:%d", len(txIndexBatch), len(dagRemain))
			case <-finishC:
				//logger.Debugf("block [%d] schedule with dag finish", block.Number())
				scheduleFinishC <- true
				return
			}
		}
	}()

	txIndexBatch := popNextTxBatchFromDag(dagRemain)
	//logger.Debugf("simulate with dag first batch size:%d, total batch size:%d", len(txIndexBatch), reRunningTxLen)
	for _, tx := range txIndexBatch {
		//logger.Debugf("the txIndexBatch tx is :%d", tx)
		runningTxC <- tx
	}
	<-scheduleFinishC
	executeTime4 := time.Since(executeStart4)

	close(needReexecute)
	reExecuteNum := len(needReexecute)
	logger.Debugf("the re-execute num %+v", reExecuteNum)

	for reIndex := range needReexecute {
		//logger.Debugf("the re-execute tx %+v", reIndex)
		tx := newTxMapping[reIndex]
		reExecuteCh <- tx
	}
	logger.Debugln("all executed")
	//*sum += executeTime3
	//*reexecute = *reexecute + reExecuteNum
	//*sum += executeTime4

	return err, reExecuteNum, executeTime4
}

func findLongestChain(ReadSets map[int]map[common.Address]struct{}, WriteSets map[int]map[common.Address]struct{}) (common.Address, []int) {
	keysAccessCount := make(map[common.Address][]int)
	for index, readSet := range ReadSets {
		for key := range readSet {
			keysAccessCount[key] = append(keysAccessCount[key], index)
		}
	}
	for index, writeSet := range WriteSets {
		for key := range writeSet {
			if _, ok := ReadSets[index][key]; !ok {
				keysAccessCount[key] = append(keysAccessCount[key], index)
			}
		}
	}

	var mostFrequentKey common.Address
	max := 0
	for key, txList := range keysAccessCount {
		if len(txList) > max {
			mostFrequentKey = key
			max = len(txList)
		}
	}
	return mostFrequentKey, keysAccessCount[mostFrequentKey]
}

func (p *StateProcessor) Serial(cc *ConsensusContent, statedb *state.StateDB, cfg vm.Config, sum *time.Duration) error {
	block := new(types.Block)
	block.DecodeBytes(cc.BytesOfBlock)
	var (
		usedGas     = new(uint64)
		header      = block.Header()
		blockNumber = block.Number()
		blockHash   = block.Hash()
		gp          = new(GasPool).AddGas(block.GasLimit())
	)

	if p.config.DAOForkSupport && p.config.DAOForkBlock != nil && p.config.DAOForkBlock.Cmp(block.Number()) == 0 {
		misc.ApplyDAOHardFork(statedb)
	}

	blockContext := NewEVMBlockContext(header, p.bc, nil)
	vmenv := vm.NewEVM(blockContext, vm.TxContext{}, statedb, p.config, cfg)
	startSerial := time.Now()
	for i, tx := range block.Transactions() {
		msg, _ := tx.AsMessage(types.MakeSigner(p.config, header.Number), header.BaseFee)
		statedb.Prepare(tx.Hash(), i)
		applyTransaction2(msg, p.config, p.bc, nil, gp, statedb, blockNumber, blockHash, tx, usedGas, vmenv)
	}
	statedb.IntermediateRoot3(p.config.IsEIP158(blockNumber))
	endSerial := time.Since(startSerial)
	*sum += endSerial
	p.engine.Finalize(p.bc, header, statedb, block.Transactions(), block.Uncles())
	return nil
}

func (p *StateProcessor) DeOcc(cc *ConsensusContent, statedb *state.StateDB, cfg vm.Config, sum *time.Duration) error {
	block := new(types.Block)
	block.DecodeBytes(cc.BytesOfBlock)
	var (
		header = block.Header()
		gp     = new(GasPool).AddGas(block.GasLimit())
	)
	if p.config.DAOForkSupport && p.config.DAOForkBlock != nil && p.config.DAOForkBlock.Cmp(block.Number()) == 0 {
		misc.ApplyDAOHardFork(statedb)
	}
	var (
		//copyStateDB      = make([]*state.StateDB, block.Transactions().Len())
		txMapping        = make(map[int]*types.Transaction)
		txBatchSize      = len(block.Transactions())
		runningTxC       = make(chan int, txBatchSize)
		rerunningTxC     = make(chan int, txBatchSize)
		finishC          = make(chan bool)
		scheduleFinishC  = make(chan bool)
		poolCapacity     = 16
		copyObjectsTimes = make([]time.Duration, block.Transactions().Len())
		writes           = make(map[string]int, 1024)
	)

	for index, tx := range block.Transactions() {
		txMapping[index] = tx
	}
	// for i := 0; i < block.Transactions().Len(); i++ {
	// 	copyStateDB[i] = statedb.Copy()
	// 	copyStateDB[i].StateCopy(statedb)
	// }
	var applySize = 0
	var (
		err          error
		lockapply    sync.RWMutex
		lockdb       sync.RWMutex
		lockconflict sync.RWMutex
		lockwrite    sync.RWMutex
	)
	var goRoutinePool *ants.Pool
	goRoutinePool, _ = ants.NewPool(poolCapacity, ants.WithPreAlloc(true))
	defer goRoutinePool.Release()
	executeStart := time.Now()
	go func() {
		for {
			select {
			case txIndex := <-runningTxC:
				tx := txMapping[txIndex]
				err = goRoutinePool.Submit(func() {
					msg, _ := tx.AsMessage(types.MakeSigner(p.config, header.Number), header.BaseFee)
					blockContext := NewEVMBlockContext(header, p.bc, nil)
					start1 := time.Now()
					lockdb.RLock()
					copydb := statedb.Copy()
					lockdb.RUnlock()
					end1 := time.Since(start1)

					vmenv := vm.NewEVM(blockContext, vm.TxContext{}, copydb, p.config, cfg)
					copydb.Prepare(tx.Hash(), txIndex)
					applyTransactionWithoutConflict(msg, gp, copydb, vmenv)

					lockapply.Lock()
					copyObjectsTimes[txIndex] = end1 //+ end2
					lockapply.Unlock()

					reExecute := false
					readTableItemForI := make([]common.Address, 0, len(copydb.Readset))
					for k := range copydb.Readset {
						readTableItemForI = append(readTableItemForI, k)
					}
					writeTableItemForI := make([]common.Address, 0, len(copydb.Writeset))
					lockwrite.Lock()
					for k := range copydb.Writeset {
						writeTableItemForI = append(writeTableItemForI, k)
						if tid, ok := writes[string(k[:])]; ok { // 存在
							if txIndex < tid {
								writes[string(k[:])] = txIndex
							}
						} else {
							writes[string(k[:])] = txIndex // 不存在
						}
					}
					lockwrite.Unlock()
					lockconflict.Lock()
					lockwrite.RLock()
					if hasconflicts(txIndex, readTableItemForI, writeTableItemForI, writes) {
						reExecute = true
					}
					lockwrite.RUnlock()
					if reExecute {
						rerunningTxC <- txIndex
					}
					applySize++
					lockconflict.Unlock()
					if applySize >= txBatchSize {
						finishC <- true
					}
				})
			case <-finishC:
				scheduleFinishC <- true
				return
			}
		}
	}()

	// Put the pending transaction into the running queue
	go func() {
		if len(block.Transactions()) > 0 {
			for i, _ := range block.Transactions() {
				runningTxC <- i
			}
		} else {
			finishC <- true
		}
	}()
	<-scheduleFinishC
	close(rerunningTxC)
	for txid := range rerunningTxC {
		tx := txMapping[txid]
		msg, _ := tx.AsMessage(types.MakeSigner(p.config, header.Number), header.BaseFee)
		blockContext := NewEVMBlockContext(header, p.bc, nil)
		copydb := statedb.Copy()
		vmenv := vm.NewEVM(blockContext, vm.TxContext{}, copydb, p.config, cfg)
		copydb.Prepare(tx.Hash(), txid)
		applyTransactionWithoutConflict(msg, gp, copydb, vmenv)
	}
	executeTime := time.Since(executeStart)

	var copytimes time.Duration
	for i := 0; i < txBatchSize/poolCapacity; i++ {
		copytimes += copyObjectsTimes[i]
	}
	*sum += executeTime - copytimes
	return err
}

func BuildDAGShowDependencies(readBitmaps []*bitmap.Bitmap, writeBitmaps []*bitmap.Bitmap) map[int]map[int]int {
	num := len(readBitmaps)
	dagDeps := make(map[int]map[int]int)
	for i := 0; i < num; i++ {
		depsForI := make(map[int]int)
		readBitmapForI, writeBitmapForI := readBitmaps[i], writeBitmaps[i]
		for j := i - 1; j >= 0; j-- {
			readBitmapForJ, writeBitmapForJ := readBitmaps[j], writeBitmaps[j]
			if writeBitmapForI.InterExist(writeBitmapForJ) {
				depsForI[j] = 1
				continue
			} else if readBitmapForI.InterExist(writeBitmapForJ) {
				depsForI[j] = 2
				continue
			} else if writeBitmapForI.InterExist(readBitmapForJ) {
				depsForI[j] = 3
				continue
			}
		}
		dagDeps[i] = make(map[int]int)
		dagDeps[i] = depsForI
	}

	return dagDeps
}

func shrinkDAGDeps(txIndex int, dagDeps map[int]map[int]int) {
	for _, depsForI := range dagDeps {
		delete(depsForI, txIndex)
	}
}

func popNextBatch(dagDeps map[int]map[int]int) []int {
	var txIndexBatch []int
	for checkIndex, deps := range dagDeps {
		if len(deps) == 0 {
			txIndexBatch = append(txIndexBatch, checkIndex)
			delete(dagDeps, checkIndex)
		} else if noWAW(deps) {
			if noWAR(deps) || noRAW(deps) {
				txIndexBatch = append(txIndexBatch, checkIndex)
				delete(dagDeps, checkIndex)
			}
		}
	}
	return txIndexBatch
}

func noWAR(deps map[int]int) bool {
	for _, dep := range deps {
		if dep == 3 {
			return false
		}
	}
	return true
}

func noRAW(deps map[int]int) bool {
	for _, dep := range deps {
		if dep == 2 {
			return false
		}
	}
	return true
}

func noWAW(deps map[int]int) bool {
	for _, dep := range deps {
		if dep == 1 {
			return false
		}
	}
	return true
}
