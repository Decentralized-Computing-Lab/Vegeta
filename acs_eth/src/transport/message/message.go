package message

import "go.themix.io/transport/info"

// MessageType is type of consensus message
type MessageType uint8

// VAL is proposal message,
// ECHO is sent upon receiving VAL,
// READY is sent upon receiving 2f+1 ECHO
// BVAL is sent upon voting for a binary value (0 or 1)
// AUX is sent upon receiving f+1 matching BVAL message and timeout,
//	or in optimal case where f+1 BVAL matching BVAL messages are received from SG
// COIN is coin message
const (
	VAL   MessageType = 0
	ECHO  MessageType = 1
	READY MessageType = 2
	BVAL  MessageType = 3
	AUX   MessageType = 4
	COIN  MessageType = 5
)

// ConsMessage is the message type exchanged for achieving consensus
type ConsMessage struct {
	Type      MessageType
	Proposer  info.IDType
	From      info.IDType
	Round     uint8
	Sequence  uint64
	Signature []byte
	Content   []byte
}

// Request is the message sent from client to servers
type Request struct {
	From      info.IDType
	Sequence  uint64
	Signature []byte
	Content   []byte
}

// GetName return message type name
func (t MessageType) GetName() string {
	switch t {
	case 0:
		return "VAL"
	case 1:
		return "ECHO"
	case 2:
		return "READY"
	case 3:
		return "BVAL"
	case 4:
		return "AUX"
	case 5:
		return "COIN"
	}
	return "UNKNOWN"
}
