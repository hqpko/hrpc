package hrpc

import (
	"errors"
	"reflect"

	fast "github.com/gogo/protobuf/proto"
	"github.com/golang/protobuf/proto"
	"github.com/hqpko/hpool"
)

const (
	defChannelSize      = 1 << 6
	defReadChannelCount = 1 << 4
)

var errorType = reflect.TypeOf((*error)(nil)).Elem()
var ErrNotPbMessage = errors.New("args is not pb.message")

var (
	bufferPool = hpool.NewBufferPool(64, 1024*16)
	// callPool   = hpool.NewPool(func() interface{} { return &Call{done: make(chan *Call, 1)} }, 64)
)

type Translator interface {
	Marshal(value interface{}) ([]byte, error)
	Unmarshal(data []byte, value interface{}) error
}

type translatorProto struct {
}

func (tp *translatorProto) Marshal(value interface{}) ([]byte, error) {
	if pb, ok := value.(proto.Message); ok {
		return proto.Marshal(pb)
	}
	return nil, ErrNotPbMessage
}

func (tp *translatorProto) Unmarshal(data []byte, reply interface{}) error {
	if pb, ok := reply.(proto.Message); ok {
		return proto.Unmarshal(data, pb)
	}
	return ErrNotPbMessage
}

type translatorFastProto struct {
}

func (tp *translatorFastProto) Marshal(value interface{}) ([]byte, error) {
	if pb, ok := value.(proto.Message); ok {
		return fast.Marshal(pb)
	}
	return nil, ErrNotPbMessage
}

func (tp *translatorFastProto) Unmarshal(data []byte, reply interface{}) error {
	if pb, ok := reply.(proto.Message); ok {
		return fast.Unmarshal(data, pb)
	}
	return ErrNotPbMessage
}
