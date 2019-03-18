package hrpc

import (
	"encoding/gob"

	"github.com/golang/protobuf/proto"
	"github.com/hqpko/hbuffer"
)

type decoder interface {
	decode(reply interface{}) error
}

type gobDecoder struct {
	buffer *hbuffer.Buffer
	dec    *gob.Decoder
}

func newGobDecoder(buffer *hbuffer.Buffer) *gobDecoder {
	return &gobDecoder{dec: gob.NewDecoder(buffer), buffer: buffer}
}

func (e *gobDecoder) decode(reply interface{}) error {
	e.buffer.Reset()
	if err := e.dec.Decode(reply); err != nil {
		return err
	}
	return nil
}

type pbDecoder struct {
	buffer *hbuffer.Buffer
}

func newPbDecoder(buffer *hbuffer.Buffer) *pbDecoder {
	return &pbDecoder{buffer: buffer}
}

func (e *pbDecoder) decode(reply interface{}) error {
	if pb, ok := reply.(proto.Message); ok {
		return proto.Unmarshal(e.buffer.GetRestOfBytes(), pb)
	}
	return ErrNotPbMessage
}
