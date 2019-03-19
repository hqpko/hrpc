package hrpc

import (
	"fmt"
	"reflect"

	"github.com/hqpko/hbuffer"
	"github.com/hqpko/hconcurrent"
	"github.com/hqpko/hnet"
)

type Option struct {
	HandlerEncode HandlerEncode
	HandlerDecode HandlerDecode
}

type methodInfo struct {
	method reflect.Value
	args   reflect.Type
	reply  reflect.Type
}

type Server struct {
	enc       HandlerEncode
	dec       HandlerDecode
	socket    *hnet.Socket
	protocols map[int32]*methodInfo

	sendChannel *hconcurrent.Concurrent
	readChannel *hconcurrent.Concurrent
}

func NewServer(option Option) *Server {
	s := &Server{protocols: map[int32]*methodInfo{}, enc: option.HandlerEncode, dec: option.HandlerDecode}
	s.sendChannel = hconcurrent.NewConcurrent(defChannelSize, 1, s.handlerSend)
	s.sendChannel.Start()
	s.readChannel = hconcurrent.NewConcurrent(defChannelSize, defReadChannelCount, s.handlerRead)
	s.readChannel.Start()
	return s
}

func (s *Server) handlerSend(i interface{}) interface{} {
	if buffer, ok := i.(*hbuffer.Buffer); ok {
		_ = s.socket.WritePacket(buffer.GetBytes())
		bufferPool.Put(buffer)
	}
	return nil
}

func (s *Server) handlerRead(i interface{}) interface{} {
	for {
		buffer, ok := i.(*hbuffer.Buffer)
		if !ok {
			break
		}
		pid, err := buffer.ReadInt32()
		if err != nil {
			break
		}
		oneWay, err := buffer.ReadBool()
		if err != nil {
			break
		}
		var seq uint64
		if !oneWay {
			seq, err = buffer.ReadUint64()
			if err != nil {
				break
			}
		}
		mi, ok := s.protocols[pid]
		if !ok {
			break
		}
		args := reflect.New(mi.args.Elem())
		reply := reflect.New(mi.reply.Elem())
		err = s.dec(buffer.GetRestOfBytes(), args.Interface())
		if err != nil {
			break
		}
		mi.method.Call([]reflect.Value{args, reply})
		if !oneWay {
			d, err := s.enc(reply.Interface())
			if err != nil {
				break
			}
			buffer.Reset()
			buffer.WriteUint64(seq)
			buffer.WriteBytes(d)
			s.sendChannel.MustInput(buffer)
		}
		break
	}
	return nil
}

func (s *Server) Register(protocolID int32, handler interface{}) {
	if _, ok := s.protocols[protocolID]; ok {
		panic(fmt.Sprintf("register protocol error,id exist:%d", protocolID))
	}
	mValue := reflect.ValueOf(handler)
	if mValue.Kind() != reflect.Func {
		panic("hrpc: server register handler is not method")
	}
	mType := reflect.TypeOf(handler)
	if mType.NumIn() != 2 {
		panic("hrpc: server register handler num in is not 2")
	}
	s.protocols[protocolID] = &methodInfo{
		method: mValue,
		args:   mType.In(0),
		reply:  mType.In(1),
	}
}

func (s *Server) Listen(socket *hnet.Socket) error {
	s.socket = socket
	return socket.ReadBuffer(func(buffer *hbuffer.Buffer) {
		s.readChannel.MustInput(buffer)
	}, s.getBuffer)
}

func (s *Server) getBuffer() *hbuffer.Buffer {
	return bufferPool.Get()
}
