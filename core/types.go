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
	"time"

	"github.com/ethereum/go-ethereum/bitmap"

	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
)

// Validator is an interface which defines the standard for block validation. It
// is only responsible for validating block contents, as the header validation is
// done by the specific consensus engines.
type Validator interface {
	// ValidateBody validates the given block's content.
	ValidateBody(block *types.Block) error

	// ValidateState validates the given statedb and optionally the receipts and
	// gas used.
	ValidateState(block *types.Block, state *state.StateDB, receipts types.Receipts, usedGas uint64) error
}

// Prefetcher is an interface for pre-caching transaction signatures and state.
type Prefetcher interface {
	// Prefetch processes the state changes according to the Ethereum rules by running
	// the transaction messages using the statedb, but any changes are discarded. The
	// only goal is to pre-cache transaction signatures and state trie nodes.
	Prefetch(block *types.Block, statedb *state.StateDB, cfg vm.Config, interrupt *uint32)
}

// type BlockFlow struct {
// 	block *types.Block
// 	next  *BlockFlow
// }

type BlocksPackage struct {
	list *list.List
	// head         *BlockFlow
	// tail         *BlockFlow
	// len          int
	blockNum     int
	txsize       int
	abortsize    int
	totalabort   int
	totalexecute int
}

func NewBlockFlow() *BlocksPackage {
	ret := &BlocksPackage{}
	ret.list = list.New()
	return ret
}

// Processor is an interface for processing blocks using a given initial state.
type Processor interface {
	// Process processes the state changes according to the Ethereum rules by running
	// the transaction messages using the statedb and applying any rewards to both
	// the processor (coinbase) and any included uncles.
	Process(block *types.Block, statedb *state.StateDB, cfg vm.Config) (types.Receipts, []*types.Log, uint64, error)
	ProcessWithoutConflict(block *types.Block, statedb *state.StateDB, scfg vm.Config) (int64, map[int]dagNeighbors, []*bitmap.Bitmap, []*bitmap.Bitmap, map[string]int, []*bitmap.Bitmap, time.Duration, error)
	ProcessSerial(block *types.Block, statedb *state.StateDB, cfg vm.Config, duration *time.Duration) (uint64, error)
	ProcessSerialWithoutCommit(block *types.Block, statedb *state.StateDB, cfg vm.Config, duration *time.Duration) (uint64, error)
	ProcessOcc(block *types.Block, statedb *state.StateDB, cfg vm.Config, sum *time.Duration) (time.Duration, error)
	// fabric interfaces
	ProcessEOV(bf *BlocksPackage, statedb *state.StateDB, cfg vm.Config, sum *time.Duration, executeSum *time.Duration) (time.Duration, error) //  单节点
	Execute(bp *BlocksPackage, statedb *state.StateDB, cfg vm.Config) (time.Duration, *ConsensusContent)                                       // 多节点
	Validate(bp *BlocksPackage, cc *ConsensusContent, statedb *state.StateDB, cfg vm.Config, sum *time.Duration) error                         // 多节点
	//
	ProcesswithDag(block *types.Block, statedb *state.StateDB, cfg vm.Config, sum *time.Duration) (time.Duration, error)
	ReplayAndReexecute(block *types.Block, statedb *state.StateDB, spe_statedb *state.StateDB, cfg vm.Config, sum *time.Duration) (float64, error)
	Speculate(block *types.Block, statedb *state.StateDB, cfg vm.Config) *ConsensusContent
	Parallel(cc *ConsensusContent, statedb *state.StateDB, cfg vm.Config, sum *time.Duration) error
	PreExecute(block *types.Block, statedb *state.StateDB, cfg vm.Config) (map[int]map[int]int, []int, []*bitmap.Bitmap, []*bitmap.Bitmap, map[string]int, []*bitmap.Bitmap, time.Duration, error)
	ProcessWithDeps(block *types.Block, statedb *state.StateDB, cfg vm.Config, sum *time.Duration, analyzeSum *time.Duration) (int, time.Duration, error)
	Serial(cc *ConsensusContent, statedb *state.StateDB, cfg vm.Config, sum *time.Duration) error
	DeOcc(cc *ConsensusContent, statedb *state.StateDB, cfg vm.Config, sum *time.Duration) error
	ProcessAriaReorderFB(block *types.Block, statedb *state.StateDB, cfg vm.Config, sum *time.Duration, executeTime *time.Duration, reExecuteCh chan *types.Transaction, fbexecute *int, reexecute *int) error
}
