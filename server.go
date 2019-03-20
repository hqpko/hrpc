package hrpc

import (
	"errors"
	"fmt"
	"reflect"
	"sync"

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
	lock      *sync.RWMutex
	enc       HandlerEncode
	dec       HandlerDecode
	socket    *hnet.Socket
	protocols map[int32]*methodInfo

	sendChannel *hconcurrent.Concurrent
	readChannel *hconcurrent.Concurrent
}

func NewServer(option Option) *Server {
	s := &Server{lock: new(sync.RWMutex), protocols: map[int32]*methodInfo{}, enc: option.HandlerEncode, dec: option.HandlerDecode}
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
		mi, ok := s.getMethodInfo(pid)
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

// handler is
// 1. func (args interface{}) error (send args only,no reply)
// 2. func (args, reply interface{}) error
func (s *Server) Register(protocolID int32, handler interface{}) {
	mValue := reflect.ValueOf(handler)
	if mValue.Kind() != reflect.Func {
		panic("hrpc: server register handler is not method")
	}
	mType := reflect.TypeOf(handler)
	if mType.NumIn() < 1 {
		panic("hrpc: server register handler num in < 1")
	}
	if mType.NumOut() != 1 {
		panic("hrpc: server register handler num out != 1")
	}
	outType := mType.Out(0)
	if outType.Kind() != reflect.TypeOf(errors.New("")).Kind() {
		panic("hrpc: server register handler num out type is not error")
	}
	methodInfo := &methodInfo{
		method: mValue,
		args:   mType.In(0),
	}
	if mType.NumIn() > 1 {
		methodInfo.reply = mType.In(1)
	}
	if !s.setMethodInfo(protocolID, methodInfo) {
		panic(fmt.Sprintf("register protocol error,id exist:%d", protocolID))
	}
}

func (s *Server) getMethodInfo(protocolID int32) (*methodInfo, bool) {
	s.lock.RLock()
	defer s.lock.RUnlock()
	methodInfo, ok := s.protocols[protocolID]
	return methodInfo, ok
}

func (s *Server) setMethodInfo(protocolID int32, info *methodInfo) bool {
	s.lock.Lock()
	defer s.lock.Unlock()
	_, ok := s.protocols[protocolID]
	if ok {
		return false
	}
	s.protocols[protocolID] = info
	return true
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
