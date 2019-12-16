package hrpc

import (
	"errors"
	"reflect"
	"time"

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
	defTimeoutStepDuration = 100 * time.Millisecond
)

type Translator interface {
	Marshal(value interface{}, writeBuffer *hbuffer.Buffer) error
	Unmarshal(readBuffer *hbuffer.Buffer, value interface{}) error
}
