package hrpc

import (
	"fmt"
	"net/rpc"
	"reflect"

	"github.com/hqpko/hbuffer"
	"github.com/hqpko/hconcurrent"
	"github.com/hqpko/hnet"
)

type HandlerRPC = func(args, reply interface{}) error

type methodInfo struct {
	method reflect.Value
	args   reflect.Type
	reply  reflect.Type
}

type Server struct {
	rpc.Call
	enc       encoder
	dec       decoder
	socket    *hnet.Socket
	protocols map[int32]*methodInfo

	sendChannel *hconcurrent.Concurrent
	readChannel *hconcurrent.Concurrent
}

func NewServer() *Server {
	s := &Server{protocols: map[int32]*methodInfo{}, enc: newPbEncoder(), dec: newPbDecoder()}
	s.sendChannel = hconcurrent.NewConcurrent(defChannelSize, 1, s.handlerSend)
	s.sendChannel.Start()
	s.readChannel = hconcurrent.NewConcurrent(defChannelSize, defReadChannelCount, s.handlerRead)
	s.readChannel.Start()
	return s
}

func (s *Server) handlerSend(i interface{}) interface{} {
	if buffer, ok := i.(*hbuffer.Buffer); ok {
		_ = s.socket.WritePacket(buffer.GetBytes())
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
		seq, err := buffer.ReadUint64()
		if err != nil {
			break
		}
		mi, ok := s.protocols[pid]
		if !ok {
			break
		}
		args := reflect.New(mi.args.Elem())
		reply := reflect.New(mi.reply.Elem())
		err = s.dec.decode(buffer.GetRestOfBytes(), args.Interface())
		if err != nil {
			break
		}
		mi.method.Call([]reflect.Value{args, reply})
		d, err := s.enc.encode(reply.Interface())
		if err != nil {
			break
		}
		buffer.Reset()
		buffer.WriteUint64(seq)
		buffer.WriteBytes(d)
		s.sendChannel.MustInput(buffer)
		break
	}
	return nil
}

func (s *Server) Register(protocolID int32, handler interface{}) {
	if _, ok := s.protocols[protocolID]; ok {
		panic(fmt.Sprintf("register protocol error,id exist:%d", protocolID))
	}
	mtype := reflect.TypeOf(handler)
	mtype.In(0)
	s.protocols[protocolID] = &methodInfo{
		method: reflect.ValueOf(handler),
		args:   mtype.In(0),
		reply:  mtype.In(1),
	}
}

func (s *Server) Listen(socket *hnet.Socket) error {
	s.socket = socket
	return socket.ReadBuffer(func(buffer *hbuffer.Buffer) {
		s.readChannel.MustInput(buffer)
	}, s.getBuffer)
}

func (s *Server) getBuffer() *hbuffer.Buffer {
	return hbuffer.NewBuffer()
}
