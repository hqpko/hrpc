package tests

import (
	"github.com/hqpko/hbuffer"
)

var (
	bufReqMarshalMap = map[int32]func(req *BufReq, buffer *hbuffer.Buffer){
		1: func(req *BufReq, buffer *hbuffer.Buffer) {
			if req.A != 0 {
				buffer.WriteInt32(1)
				buffer.WriteInt32(req.A)
			}
		},
	}
	bufReqUnmarshalMap = map[int32]func(req *BufReq, buffer *hbuffer.Buffer) error{
		1: func(req *BufReq, buffer *hbuffer.Buffer) (err error) {
			req.A, err = buffer.ReadInt32()
			return
		},
	}

	bufRespMarshalMap = map[int32]func(req *BufResp, buffer *hbuffer.Buffer){
		1: func(req *BufResp, buffer *hbuffer.Buffer) {
			if req.B != 0 {
				buffer.WriteInt32(1)
				buffer.WriteInt32(req.B)
			}
		},
	}
	bufRespUnmarshalMap = map[int32]func(req *BufResp, buffer *hbuffer.Buffer) error{
		1: func(req *BufResp, buffer *hbuffer.Buffer) (err error) {
			req.B, err = buffer.ReadInt32()
			return
		},
	}
)

type BufReq struct {
	A int32
}

func (b *BufReq) Marshal(buffer *hbuffer.Buffer) {
	for _, f := range bufReqMarshalMap {
		f(b, buffer)
	}
}

func (b *BufReq) Unmarshal(buffer *hbuffer.Buffer) error {
	for buffer.Available() > 0 {
		id, err := buffer.ReadInt32()
		if err != nil {
			return err
		}
		if f, ok := bufReqUnmarshalMap[id]; ok {
			if err = f(b, buffer); err != nil {
				return err
			}
		}
	}
	return nil
}

type BufResp struct {
	B int32
}

func (b *BufResp) Marshal(buffer *hbuffer.Buffer) {
	for _, f := range bufRespMarshalMap {
		f(b, buffer)
	}
}

func (b *BufResp) Unmarshal(buffer *hbuffer.Buffer) error {
	for buffer.Available() > 0 {
		id, err := buffer.ReadInt32()
		if err != nil {
			return err
		}
		if f, ok := bufRespUnmarshalMap[id]; ok {
			if err = f(b, buffer); err != nil {
				return err
			}
		}
	}
	return nil
}
