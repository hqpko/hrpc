package hrpc

import (
	"encoding/gob"

	"github.com/golang/protobuf/proto"
	"github.com/hqpko/hbuffer"
)

type decoder interface {
	decode(data []byte, reply interface{}) error
}

type gobDecoder struct {
	buffer *hbuffer.Buffer
	dec    *gob.Decoder
}

func newGobDecoder(buffer *hbuffer.Buffer) *gobDecoder {
	return &gobDecoder{dec: gob.NewDecoder(buffer), buffer: buffer}
}

func (e *gobDecoder) decode(data []byte, reply interface{}) error {
	e.buffer.Reset()
	if err := e.dec.Decode(reply); err != nil {
		return err
	}
	return nil
}

type pbDecoder struct {
}

func newPbDecoder() *pbDecoder {
	return &pbDecoder{}
}

func (e *pbDecoder) decode(data []byte, reply interface{}) error {
	if pb, ok := reply.(proto.Message); ok {
		return proto.Unmarshal(data, pb)
	}
	return ErrNotPbMessage
}
