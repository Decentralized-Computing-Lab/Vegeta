package server

import (
	"sync"

	"go.themix.io/crypto/bls"
	"go.themix.io/transport"
	"go.themix.io/transport/info"
	"go.themix.io/transport/message"
	"go.uber.org/zap"
)

var maxbatchsize = 1

type state struct {
	lg        *zap.Logger
	tp        transport.Transport
	blsSig    *bls.BlsSig
	proposer  *Proposer
	id        info.IDType
	n         uint64
	collected uint64
	execs     map[uint64]*asyncCommSubset
	lock      sync.RWMutex
	reqc      chan *message.ConsMessage
	repc      chan []byte
}

func initState(lg *zap.Logger,
	tp transport.Transport,
	blsSig *bls.BlsSig,
	id info.IDType,
	proposer *Proposer,
	n uint64, repc chan []byte) *state {
	st := &state{
		lg:        lg,
		tp:        tp,
		blsSig:    blsSig,
		id:        id,
		proposer:  proposer,
		n:         n,
		collected: 0,
		execs:     make(map[uint64]*asyncCommSubset),
		lock:      sync.RWMutex{},
		reqc:      make(chan *message.ConsMessage, 2*int(n)*maxbatchsize),
		repc:      repc}
	go st.run()
	return st
}

func (st *state) insertMsg(msg *message.ConsMessage) {
	st.lock.RLock()

	if exec, ok := st.execs[msg.Sequence]; ok {
		st.lock.RUnlock()
		exec.insertMsg(msg)
	} else {
		if st.collected <= msg.Sequence {
			st.lock.RUnlock()
			exec := initACS(st, st.lg, st.tp, st.blsSig, st.proposer, msg.Sequence, st.n, st.reqc)
			st.lock.Lock()
			if e, ok := st.execs[msg.Sequence]; ok {
				exec = e
			} else {
				st.execs[msg.Sequence] = exec
			}
			st.lock.Unlock()
			exec.insertMsg(msg)
		}
	}
}

func (st *state) garbageCollect(seq uint64) {

}

// execute requests by a single thread
func (st *state) run() {
	var (
		resps string
		respN uint64
	)

	for {
		req := <-st.reqc
		if respN == 0 {
			resps = string(req.Content)
			respN++
		} else {
			resps = resps + "," + string(req.Content)
			respN++
		}
		if respN == st.n {
			resps = "[" + resps + "]"
			st.repc <- []byte(resps)
			respN = 0
		}
	}
}
