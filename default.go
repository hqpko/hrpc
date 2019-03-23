package hrpc

import (
	"errors"
	"reflect"
	"time"

	fast "github.com/gogo/protobuf/proto"
	"github.com/golang/protobuf/proto"
)

const (
	defChannelSize      = 1 << 6
	defReadChannelCount = 1 << 4
)

var errorType = reflect.TypeOf((*error)(nil)).Elem()
var ErrNotPbMessage = errors.New("hrpc: args is not pb.message")
var ErrCallTimeout = errors.New("hrpc: call timeout")
var (
	defTimeoutCall         = 8 * time.Second
	defTimeoutMaxDuration  = 16 * time.Second
	defTimeoutStepDuration = 500 * time.Millisecond
)

type Translator interface {
	Marshal(value interface{}) ([]byte, error)
	Unmarshal(data []byte, value interface{}) error
}

type translatorProto struct {
}

func NewTranslatorProto() Translator {
	return new(translatorProto)
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

func NewTranslatorFashProto() Translator {
	return new(translatorFastProto)
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
