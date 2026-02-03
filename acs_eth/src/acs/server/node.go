package server

import (
	"runtime"

	"go.themix.io/crypto/bls"
	"go.themix.io/transport"
	"go.themix.io/transport/info"
	"go.uber.org/zap"
)

// Node is a local process
type Node struct {
	reply     chan []byte
	proposer  *Proposer
	transport *transport.Transport
}

// InitNode initiate a node for processing messages
func InitNode(lg *zap.Logger, blsSig *bls.BlsSig, id info.IDType, n uint64, port int, addresses []string) {
	tp, msgc, reqc, repc := transport.InitTransport(lg, id, port, addresses)
	proposer := initProposer(lg, tp, id, reqc)
	state := initState(lg, tp, blsSig, id, proposer, n, repc)
	for i := 0; i < runtime.NumCPU()-1; i++ {
		go func() {
			for {
				msg := <-msgc
				state.insertMsg(msg)
			}
		}()
	}
	for {
		msg := <-msgc
		state.insertMsg(msg)
	}
}
