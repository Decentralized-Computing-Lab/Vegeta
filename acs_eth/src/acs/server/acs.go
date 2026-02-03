package server

import (
	"fmt"
	"sync"

	"go.themix.io/crypto/bls"
	"go.themix.io/transport"
	"go.themix.io/transport/info"
	"go.themix.io/transport/message"
	"go.uber.org/zap"
)

type asyncCommSubset struct {
	st            *state
	lg            *zap.Logger
	n             uint64
	thld          uint64
	sequence      uint64
	numDecided    uint64
	numFinished   uint64
	numDecidedOne uint64
	instances     []*instance
	proposer      *Proposer
	reqc          chan *message.ConsMessage
	lock          sync.Mutex
}

func initACS(st *state,
	lg *zap.Logger,
	tp transport.Transport,
	blsSig *bls.BlsSig,
	proposer *Proposer,
	seq uint64, n uint64,
	reqc chan *message.ConsMessage) *asyncCommSubset {
	re := &asyncCommSubset{
		st:        st,
		lg:        lg,
		proposer:  proposer,
		n:         n,
		sequence:  seq,
		instances: make([]*instance, n),
		reqc:      reqc,
		lock:      sync.Mutex{}}
	re.thld = 2*n/3 + 1
	for i := info.IDType(0); i < info.IDType(n); i++ {
		re.instances[i] = initInstance(lg, tp, blsSig, seq, n, re.thld, uint64(i))
	}
	return re
}

func (acs *asyncCommSubset) insertMsg(msg *message.ConsMessage) {
	isDecided, isFinished := acs.instances[msg.Proposer].insertMsg(msg)
	if isDecided {
		acs.lock.Lock()
		defer acs.lock.Unlock()

		if !acs.instances[msg.Proposer].decidedOne() && msg.Proposer == acs.proposer.id {
			fmt.Printf("ID %d decided zero at %d\n", msg.Proposer, msg.Sequence)
		}
		acs.numDecided++
		// if acs.numDecided == 1 {
		// 	acs.proposer.proceed(acs.sequence)
		// }
		if acs.instances[msg.Proposer].decidedOne() {
			acs.numDecidedOne++
		}
		// if acs.numDecidedOne == acs.thld {
		// 	for i, inst := range acs.instances {
		// 		inst.canVoteZero(info.IDType(i), acs.sequence)
		// 	}
		// }
		if acs.numDecided == acs.n {
			for _, inst := range acs.instances {
				proposal := inst.getProposal()
				if inst.decidedOne() && len(proposal.Content) != 0 {
					inst.lg.Info("executed",
						zap.Int("proposer", int(proposal.Proposer)),
						zap.Int("seq", int(msg.Sequence)),
						zap.Int("content", int(proposal.Content[0])))
					acs.reqc <- proposal
				} else if proposal.Proposer == acs.proposer.id && len(proposal.Content) != 0 {
					inst.lg.Info("repropose",
						zap.Int("proposer", int(proposal.Proposer)),
						zap.Int("seq", int(proposal.Sequence)),
						zap.Int("content", int(proposal.Content[0])))
					acs.proposer.propose(proposal.Content)
				} else if inst.decidedOne() {
					inst.lg.Info("empty",
						zap.Int("proposer", int(proposal.Proposer)),
						zap.Int("seq", int(proposal.Sequence)))
				}
			}
		}
	} else if isFinished {
		acs.lock.Lock()
		defer acs.lock.Unlock()

		acs.numFinished++
		if acs.numFinished == acs.n {
			acs.st.garbageCollect(acs.sequence)
		}
	}
}
