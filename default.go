package hrpc

import (
	"errors"
	"reflect"
	"time"

	fast "github.com/gogo/protobuf/proto"
	"github.com/golang/protobuf/proto"
	"github.com/hqpko/hbuffer"
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
	Marshal(value interface{}, writeBuffer *hbuffer.Buffer) error
	Unmarshal(readBuffer *hbuffer.Buffer, value interface{}) error
}

type translatorProto struct{}

func NewTranslatorProto() Translator {
	return new(translatorProto)
}

func (tp *translatorProto) Marshal(value interface{}, writeBuffer *hbuffer.Buffer) error {
	if pb, ok := value.(proto.Message); ok {
		if bs, err := proto.Marshal(pb); err != nil {
			return err
		} else {
			writeBuffer.WriteBytes(bs)
			return nil
		}
	}
	return ErrNotPbMessage
}

func (tp *translatorProto) Unmarshal(readBuffer *hbuffer.Buffer, value interface{}) error {
	if pb, ok := value.(proto.Message); ok {
		return proto.Unmarshal(readBuffer.GetRestOfBytes(), pb)
	}
	return ErrNotPbMessage
}

type translatorFastProto struct{}

func NewTranslatorFashProto() Translator {
	return new(translatorFastProto)
}

func (tp *translatorFastProto) Marshal(value interface{}, writeBuffer *hbuffer.Buffer) error {
	if pb, ok := value.(proto.Message); ok {
		if bs, err := fast.Marshal(pb); err != nil {
			return err
		} else {
			writeBuffer.WriteBytes(bs)
			return nil
		}
	}
	return ErrNotPbMessage
}

func (tp *translatorFastProto) Unmarshal(readBuffer *hbuffer.Buffer, value interface{}) error {
	if pb, ok := value.(proto.Message); ok {
		return fast.Unmarshal(readBuffer.GetRestOfBytes(), pb)
	}
	return ErrNotPbMessage
}
