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
	callTypeRequest  = 0x01
	callTypeResponse = 0x02

	defChannelSize      = 1 << 6
	defReadChannelCount = 1 << 4
)

var (
	errorType       = reflect.TypeOf((*error)(nil)).Elem()
	ErrNotPbMessage = errors.New("hrpc: value is not pb.message")
	ErrCallTimeout  = errors.New("hrpc: call timeout")
	ErrNotMarshal   = errors.New("hrpc: value is not marshal")
)

const (
	defTimeoutCall         = 8 * time.Second
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
	if pb, ok := value.(fast.Message); ok {
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
	if pb, ok := value.(fast.Message); ok {
		return fast.Unmarshal(readBuffer.GetRestOfBytes(), pb)
	}
	return ErrNotPbMessage
}

type marshaler interface {
	Marshal(writeBuffer *hbuffer.Buffer) error
	Unmarshal(readBuffer *hbuffer.Buffer) error
}

type translatorMarshaler struct{}

func NewTranslatorMarshaler() Translator {
	return new(translatorMarshaler)
}

func (tm *translatorMarshaler) Marshal(value interface{}, writeBuffer *hbuffer.Buffer) error {
	if m, ok := value.(marshaler); ok {
		return m.Marshal(writeBuffer)
	}
	return ErrNotMarshal
}

func (tm *translatorMarshaler) Unmarshal(readBuffer *hbuffer.Buffer, value interface{}) error {
	if m, ok := value.(marshaler); ok {
		return m.Unmarshal(readBuffer)
	}
	return ErrNotMarshal
}
