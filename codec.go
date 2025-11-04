// Package fsm contains codec implementations for serializing/deserializing
// FSM request and response messages for persistence.
package fsm

import (
	"encoding/json"
	"fmt"

	"google.golang.org/protobuf/proto"
)

// Codec is an interface for serializing and deserializing messages.
// The FSM library automatically selects an appropriate codec based on the
// message type, but custom codecs can be implemented for special cases.
type Codec interface {
	// Marshal serializes the given message to bytes for persistence.
	// The message type must be compatible with this codec's encoding format.
	Marshal(any) ([]byte, error)
	
	// Unmarshal deserializes bytes into the given message.
	// The message type must be compatible with this codec's encoding format.
	Unmarshal([]byte, any) error
}

type protoBinaryCodec struct{}

var _ Codec = (*protoBinaryCodec)(nil)

func (c *protoBinaryCodec) Marshal(message any) ([]byte, error) {
	protoMessage, ok := message.(proto.Message)
	if !ok {
		return nil, fmt.Errorf("%T doesn't implement proto.Message", message)
	}
	return proto.Marshal(protoMessage)
}

func (c *protoBinaryCodec) Unmarshal(data []byte, message any) error {
	protoMessage, ok := message.(proto.Message)
	if !ok {
		return fmt.Errorf("%T doesn't implement proto.Message", message)
	}

	err := proto.Unmarshal(data, protoMessage)
	if err != nil {
		return fmt.Errorf("unmarshal into %T: %w", message, err)
	}
	return nil
}

type jsonCodec struct{}

var _ Codec = (*jsonCodec)(nil)

func (c *jsonCodec) Marshal(message any) ([]byte, error) {
	return json.Marshal(message)
}

func (c *jsonCodec) Unmarshal(data []byte, message any) error {
	return json.Unmarshal(data, message)
}
