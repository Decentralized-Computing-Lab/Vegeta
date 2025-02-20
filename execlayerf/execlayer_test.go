package main

import (
	"sync"
	"testing"

	"github.com/ethereum/go-ethereum/consensus/ethash"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/vm"
)

var (
	upNum       = 16774645
	endNum      = 16774745
	dbPath      = "/home/ubuntu/ethdata/geth/chaindata"
	ancientPath = dbPath + "/ancient/chain"
	// upNum       = 17034770
	// endNum      = 17034870
	// dbPath      = "/home/user/eth/eth_data/geth/chaindata"
	// ancientPath = dbPath + "/ancient/chain"
	currState *state.StateDB
	pendingN  int32
	localbp   *core.BlocksPackage
	bp_mutex  *sync.Mutex
)

func TestEOV(t *testing.T) {
	ancientDB, err := rawdb.NewLevelDBDatabaseWithFreezer(dbPath, 16, 1, ancientPath, "", true)
	if err != nil {
		panic(err)
	}
	bc, _ := core.NewBlockChain(ancientDB, nil, core.DefaultGenesisBlock().Config, ethash.NewFaker(), vm.Config{}, nil, nil)
	stateDB := buildStateDB(ancientDB, upNum-1)
	currState = stateDB.Copy()
}
