package hrpc

import (
	"errors"
	"reflect"

	"github.com/golang/protobuf/proto"
	"github.com/hqpko/hpool"
)

const (
	defChannelSize      = 1 << 6
	defReadChannelCount = 1 << 4
)

var errorType = reflect.TypeOf((*error)(nil)).Elem()

var (
	bufferPool = hpool.NewBufferPool(64, 1024*16)
	// callPool   = hpool.NewPool(func() interface{} { return &Call{done: make(chan *Call, 1)} }, 64)
)

type (
	HandlerEncode func(v interface{}) ([]byte, error)
	HandlerDecode func(b []byte, v interface{}) error
)

var (
	DefaultOption = Option{
		HandlerEncode: handlerPbEncode,
		HandlerDecode: handlerPbDecode,
	}

	ErrNotPbMessage = errors.New("args is not pb.message")
)

func handlerPbEncode(args interface{}) ([]byte, error) {
	if pb, ok := args.(proto.Message); ok {
		return proto.Marshal(pb)
	}
	return nil, ErrNotPbMessage
}

func handlerPbDecode(data []byte, reply interface{}) error {
	if pb, ok := reply.(proto.Message); ok {
		return proto.Unmarshal(data, pb)
	}
	return ErrNotPbMessage
}
