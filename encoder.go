package hrpc

import (
	"encoding/gob"
	"errors"

	"github.com/golang/protobuf/proto"
	"github.com/hqpko/hbuffer"
)

var ErrNotPbMessage = errors.New("args is not pb.message")

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

func (e *pbEncoder) encode(args interface{}) ([]byte, error) {
	if pb, ok := args.(proto.Message); ok {
		return proto.Marshal(pb)
	}
	return nil, ErrNotPbMessage
}
