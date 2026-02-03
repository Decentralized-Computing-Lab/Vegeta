package server

import (
	"sync"

	"go.themix.io/transport"
	"go.themix.io/transport/info"
	"go.themix.io/transport/message"
	"go.uber.org/zap"
)

// Proposer is responsible for proposing requests
type Proposer struct {
	lg   *zap.Logger
	reqc chan []byte
	tp   transport.Transport
	seq  uint64
	id   info.IDType
	lock sync.Mutex
}

func initProposer(lg *zap.Logger, tp transport.Transport, id info.IDType, reqc chan []byte) *Proposer {
	proposer := &Proposer{lg: lg, tp: tp, id: id, reqc: reqc, lock: sync.Mutex{}}
	go proposer.run()
	return proposer
}

func (proposer *Proposer) proceed(seq uint64) {
	proposer.lock.Lock()
	defer proposer.lock.Unlock()

	if proposer.seq <= seq {
		proposer.reqc <- []byte{} // insert an empty reqeust
	}
}

func (proposer *Proposer) run() {
	var req []byte
	for {
		req = <-proposer.reqc
		proposer.propose(req)
	}
}

// Propose broadcast a propose message with the given request and the current sequence number
func (proposer *Proposer) propose(request []byte) {
	proposer.lock.Lock()

	msg := &message.ConsMessage{
		Type:     message.VAL,
		Proposer: proposer.id,
		From:     proposer.id,
		Sequence: proposer.seq,
		Content:  request}

	if len(request) > 0 {
		proposer.lg.Info("propose",
			zap.Int("proposer", int(msg.Proposer)),
			zap.Int("seq", int(msg.Sequence)),
			zap.Int("content", int(msg.Content[0])))
	}

	proposer.seq++

	proposer.lock.Unlock()

	proposer.tp.Broadcast(msg)
}
