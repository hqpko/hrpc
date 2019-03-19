package hrpc

import (
	"encoding/gob"

	"github.com/golang/protobuf/proto"
	"github.com/hqpko/hbuffer"
)

type BufMessage interface {
	Marshal(buffer *hbuffer.Buffer)
	Unmarshal(buffer *hbuffer.Buffer) error
}

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

type bufDecoder struct {
}

func newBufDecoder() *bufDecoder {
	return &bufDecoder{}
}

func (e *bufDecoder) decode(data []byte, reply interface{}) error {
	if pb, ok := reply.(BufMessage); ok {
		buf := hbuffer.NewBufferWithBytes(data)
		buf.SetBytes(data)
		return pb.Unmarshal(buf)
	}
	return ErrNotBufMessage
}
