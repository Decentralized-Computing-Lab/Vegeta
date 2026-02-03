package server

import (
	"encoding/binary"
	"sync"

	"go.themix.io/crypto/bls"
	"go.themix.io/crypto/sha256"
	"go.themix.io/transport"
	"go.themix.io/transport/info"
	"go.themix.io/transport/message"
	"go.uber.org/zap"
)

// the maximum expected round that terminates consensus, P = 1 - pow(0.5, maxround)
var maxround = 30

type instance struct {
	tp           transport.Transport
	blsSig       *bls.BlsSig
	hasVotedZero bool
	hasVotedOne  bool
	hasSentAux   bool
	hasSentCoin  bool
	zeroEndorsed bool
	oneEndorsed  bool
	isDecided    bool
	isFinished   bool
	sequence     uint64
	n            uint64
	thld         uint64
	f            uint64
	round        uint8
	numEcho      uint64
	numReady     uint64
	binVals      uint8
	numBvalZero  []uint64
	numBvalOne   []uint64
	numAuxZero   []uint64
	numAuxOne    []uint64
	numCoin      []uint64
	proposal     *message.ConsMessage
	coinMsgs     [][]*message.ConsMessage
	lg           *zap.Logger
	lock         sync.Mutex
}

func initInstance(lg *zap.Logger, tp transport.Transport, blsSig *bls.BlsSig, sequence uint64, n uint64, thld uint64, i uint64) *instance {
	msg := &message.ConsMessage{
		Type:     message.VAL,
		Proposer: info.IDType(i)}
	inst := &instance{
		lg:          lg,
		tp:          tp,
		blsSig:      blsSig,
		sequence:    sequence,
		n:           n,
		thld:        thld,
		f:           n / 3,
		proposal:    msg,
		coinMsgs:    make([][]*message.ConsMessage, maxround),
		numBvalZero: make([]uint64, maxround),
		numBvalOne:  make([]uint64, maxround),
		numAuxZero:  make([]uint64, maxround),
		numAuxOne:   make([]uint64, maxround),
		numCoin:     make([]uint64, maxround),
		lock:        sync.Mutex{}}
	for i := 0; i < maxround; i++ {
		inst.coinMsgs[i] = make([]*message.ConsMessage, n)
	}
	return inst
}

// return true if the instance is decided or finished at the first time
func (inst *instance) insertMsg(msg *message.ConsMessage) (bool, bool) {
	inst.lock.Lock()
	defer inst.lock.Unlock()

	if inst.isFinished {
		return false, false
	}

	// if len(msg.Content) > 0 {
	// 	inst.lg.Info("receive msg",
	// 		zap.String("type", msg.Type.GetName()),
	// 		zap.Int("proposer", int(msg.Proposer)),
	// 		zap.Int("seq", int(msg.Sequence)),
	// 		zap.Int("round", int(msg.Round)),
	// 		zap.Int("from", int(msg.From)),
	// 		zap.Int("content", int(msg.Content[0])))
	// } else {
	// 	inst.lg.Info("receive msg",
	// 		zap.String("type", msg.Type.GetName()),
	// 		zap.Int("proposer", int(msg.Proposer)),
	// 		zap.Int("seq", int(msg.Sequence)),
	// 		zap.Int("round", int(msg.Round)),
	// 		zap.Int("from", int(msg.From)))
	// }

	switch msg.Type {
	case message.VAL:
		inst.proposal = msg
		hash, _ := sha256.ComputeHash(msg.Content)
		inst.tp.Broadcast(&message.ConsMessage{
			Type:     message.ECHO,
			Proposer: msg.Proposer,
			Sequence: msg.Sequence,
			Content:  hash})
		inst.isReadyToSendCoin()
		return inst.isReadyToEnterNewRound()
	case message.ECHO:
		inst.numEcho++
		if inst.numEcho == inst.thld {
			inst.tp.Broadcast(&message.ConsMessage{
				Type:     message.READY,
				Proposer: msg.Proposer,
				Sequence: msg.Sequence,
				Content:  msg.Content})
		}
	case message.READY:
		inst.numReady++
		if inst.numReady == inst.thld && inst.round == 0 {
			if !inst.hasVotedZero && !inst.hasVotedOne {
				inst.hasVotedOne = true
				inst.tp.Broadcast(&message.ConsMessage{
					Type:     message.BVAL,
					Proposer: msg.Proposer,
					Sequence: msg.Sequence,
					Content:  []byte{1}}) // vote 1
			}
			return inst.isReadyToEnterNewRound()
		}
	case message.BVAL:
		var b bool
		switch msg.Content[0] {
		case 0:
			inst.numBvalZero[msg.Round]++
		case 1:
			inst.numBvalOne[msg.Round]++
		}
		if inst.round == msg.Round && !inst.hasVotedZero && inst.numBvalZero[inst.round] > inst.f {
			inst.hasVotedZero = true
			inst.tp.Broadcast(&message.ConsMessage{
				Type:     message.BVAL,
				Proposer: msg.Proposer,
				Round:    inst.round,
				Sequence: msg.Sequence,
				Content:  []byte{0}}) // vote 0
		}
		if inst.round == msg.Round && !inst.zeroEndorsed && inst.numBvalZero[inst.round] >= inst.thld {
			inst.zeroEndorsed = true
			if !inst.hasSentAux {
				inst.hasSentAux = true
				inst.tp.Broadcast(&message.ConsMessage{
					Type:     message.AUX,
					Proposer: msg.Proposer,
					Round:    inst.round,
					Sequence: msg.Sequence,
					Content:  []byte{0}}) // aux 0
			}
			inst.isReadyToSendCoin()
			b = true
		}
		if inst.round == msg.Round && !inst.hasVotedOne && inst.numBvalOne[inst.round] > inst.f {
			inst.hasVotedOne = true
			inst.tp.Broadcast(&message.ConsMessage{
				Type:     message.BVAL,
				Proposer: msg.Proposer,
				Round:    inst.round,
				Sequence: msg.Sequence,
				Content:  []byte{1}}) // vote 1
		}
		if inst.round == msg.Round && !inst.oneEndorsed && inst.numBvalOne[inst.round] >= inst.thld {
			inst.oneEndorsed = true
			if !inst.hasSentAux {
				inst.hasSentAux = true
				inst.tp.Broadcast(&message.ConsMessage{
					Type:     message.AUX,
					Proposer: msg.Proposer,
					Round:    inst.round,
					Sequence: msg.Sequence,
					Content:  []byte{1}}) // aux 1
			}
			inst.isReadyToSendCoin()
			b = true
		}
		if b {
			return inst.isReadyToEnterNewRound()
		}
	case message.AUX:
		switch msg.Content[0] {
		case 0:
			inst.numAuxZero[msg.Round]++
		case 1:
			inst.numAuxOne[msg.Round]++
		}
		if inst.round == msg.Round {
			inst.isReadyToSendCoin()
			return inst.isReadyToEnterNewRound()
		}
	case message.COIN:
		inst.coinMsgs[msg.Round][msg.From] = msg
		inst.numCoin[msg.Round]++
		if inst.round == msg.Round {
			return inst.isReadyToEnterNewRound()
		}
	default:
		return false, false
	}
	return false, false
}

// must be executed within inst.lock
func (inst *instance) isReadyToSendCoin() {
	if !inst.hasSentCoin && inst.proposal != nil {
		if inst.oneEndorsed && inst.numAuxOne[inst.round] >= inst.thld {
			if !inst.isDecided {
				inst.binVals = 1
			}
		} else if inst.zeroEndorsed && inst.numAuxZero[inst.round] >= inst.thld {
			if !inst.isDecided {
				inst.binVals = 0
			}
		} else if !inst.isDecided && inst.oneEndorsed && inst.zeroEndorsed &&
			inst.numAuxOne[inst.round]+inst.numAuxZero[inst.round] >= inst.thld {
			if !inst.isDecided {
				inst.binVals = 2
			}
		} else {
			return
		}
		inst.hasSentCoin = true
		inst.tp.Broadcast(&message.ConsMessage{
			Type:     message.COIN,
			Proposer: inst.proposal.Proposer,
			Round:    inst.round,
			Sequence: inst.sequence,
			Content:  inst.blsSig.Sign(inst.getCoinInfo())}) // threshold bls sig share
	}
}

// must be executed within inst.lock
// return true if the instance is decided or finished at the first time
func (inst *instance) isReadyToEnterNewRound() (bool, bool) {
	if inst.hasSentCoin &&
		inst.numCoin[inst.round] > inst.f &&
		inst.numAuxZero[inst.round]+inst.numAuxOne[inst.round] >= inst.thld &&
		((inst.oneEndorsed && inst.numAuxOne[inst.round] >= inst.thld) ||
			(inst.zeroEndorsed && inst.numAuxZero[inst.round] >= inst.thld) ||
			(inst.oneEndorsed && inst.zeroEndorsed)) {
		sigShares := make([][]byte, 0)
		for _, m := range inst.coinMsgs[inst.round] {
			if m != nil {
				sigShares = append(sigShares, m.Content)
			}
		}
		coin := inst.blsSig.Recover(inst.getCoinInfo(), sigShares, int(inst.f+1), int(inst.n))

		inst.lg.Info("coin result",
			zap.Int("proposer", int(inst.proposal.Proposer)),
			zap.Int("seq", int(inst.sequence)),
			zap.Int("round", int(inst.round)),
			zap.Int("coin", int(coin[0]%2)))

		var nextVote byte
		if coin[0]%2 == inst.binVals {
			if inst.isDecided {
				inst.isFinished = true
				return false, true
			}

			inst.lg.Info("decided",
				zap.Int("proposer", int(inst.proposal.Proposer)),
				zap.Int("seq", int(inst.sequence)),
				zap.Int("round", int(inst.round)),
				zap.Int("result", int(coin[0]%2)))

			inst.isDecided = true
			nextVote = inst.binVals
		} else if inst.binVals != 2 { // nextVote should insist the single value
			nextVote = inst.binVals
		} else {
			nextVote = coin[0] % 2
		}

		if nextVote == 0 {
			inst.hasVotedZero = true
			inst.hasVotedOne = false
		} else {
			inst.hasVotedZero = false
			inst.hasVotedOne = true
		}
		inst.hasSentAux = false
		inst.hasSentCoin = false
		inst.zeroEndorsed = false
		inst.oneEndorsed = false
		inst.round++

		inst.tp.Broadcast(&message.ConsMessage{
			Type:     message.BVAL,
			Proposer: inst.proposal.Proposer,
			Round:    inst.round,
			Sequence: inst.sequence,
			Content:  []byte{nextVote}})

		if coin[0]%2 == inst.binVals && inst.isDecided {
			return true, false
		}
	}
	return false, false
}

func (inst *instance) getCoinInfo() []byte {
	bsender := make([]byte, 8)
	binary.LittleEndian.PutUint64(bsender, uint64(inst.proposal.Proposer))
	bseq := make([]byte, 8)
	binary.LittleEndian.PutUint64(bseq, inst.sequence)

	b := make([]byte, 17)
	b = append(b, bsender...)
	b = append(b, bseq...)
	b = append(b, inst.round)

	return b
}

func (inst *instance) canVoteZero(sender info.IDType, seq uint64) {
	inst.lock.Lock()
	defer inst.lock.Unlock()

	if inst.round == 0 && !inst.hasVotedZero && !inst.hasVotedOne {
		inst.hasVotedZero = true
		inst.tp.Broadcast(&message.ConsMessage{
			Type:     message.BVAL,
			Proposer: sender,
			Round:    inst.round,
			Sequence: seq,
			Content:  []byte{0}}) // vote 0
	}
}

func (inst *instance) decidedOne() bool {
	inst.lock.Lock()
	defer inst.lock.Unlock()

	return inst.isDecided && inst.binVals == 1
}

func (inst *instance) getProposal() *message.ConsMessage {
	inst.lock.Lock()
	defer inst.lock.Unlock()

	return inst.proposal
}
