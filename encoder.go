package hrpc

import (
	"encoding/gob"
	"errors"

	"github.com/golang/protobuf/proto"
	"github.com/hqpko/hbuffer"
)

var (
	ErrNotPbMessage  = errors.New("args is not pb.message")
	ErrNotBufMessage = errors.New("args is not buf.message")
)

type encoder interface {
	encode(args interface{}) ([]byte, error)
}

type gobEncoder struct {
	buffer *hbuffer.Buffer
	enc    *gob.Encoder
}

func newGobEncoder() *gobEncoder {
	buffer := hbuffer.NewBuffer()
	return &gobEncoder{enc: gob.NewEncoder(buffer), buffer: buffer}
}

func (e *gobEncoder) encode(args interface{}) ([]byte, error) {
	e.buffer.Reset()
	if err := e.enc.Encode(args); err != nil {
		return nil, err
	}
	return e.buffer.GetBytes(), nil
}

type pbEncoder struct{}

func newPbEncoder() *pbEncoder {
	return &pbEncoder{}
}

func (e *pbEncoder) encode(args interface{}) ([]byte, error) {
	if pb, ok := args.(proto.Message); ok {
		return proto.Marshal(pb)
	}
	return nil, ErrNotPbMessage
}

type bufEncoder struct {
}

func newBufEncoder() *bufEncoder {
	return &bufEncoder{}
}

func (e *bufEncoder) encode(args interface{}) ([]byte, error) {
	if pb, ok := args.(BufMessage); ok {
		buf := hbuffer.NewBuffer()
		pb.Marshal(buf)
		return buf.GetBytes(), nil
	}
	return nil, ErrNotBufMessage
}
