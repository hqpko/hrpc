package hrpc

import (
	"fmt"
	"log"
	"reflect"
	"sync"

	"github.com/hqpko/hbuffer"
	"github.com/hqpko/hconcurrent"
	"github.com/hqpko/hnet"
	"github.com/hqpko/hutils"
)

type methodInfo struct {
	method reflect.Value
	args   reflect.Type
	reply  reflect.Type
}

func (m *methodInfo) isOneWay() bool {
	return m.reply == nil
}

type Server struct {
	lock       *sync.RWMutex
	bufferPool *hutils.BufferPool
	socket     *hnet.Socket
	translator Translator
	protocols  map[int32]*methodInfo

	sendChannel *hconcurrent.Concurrent
	readChannel *hconcurrent.Concurrent
}

func NewServer() *Server {
	s := &Server{
		lock:       new(sync.RWMutex),
		bufferPool: hutils.NewBufferPool(),
		protocols:  map[int32]*methodInfo{},
		translator: new(translatorProto),
	}
	s.sendChannel = hconcurrent.NewConcurrent(defChannelSize, 1, s.handlerSend)
	s.sendChannel.Start()
	s.readChannel = hconcurrent.NewConcurrent(defChannelSize, defReadChannelCount, s.handlerRead)
	s.readChannel.Start()
	return s
}

func (s *Server) SetTranslator(translator Translator) *Server {
	s.translator = translator
	return s
}

func (s *Server) handlerSend(i interface{}) interface{} {
	if buffer, ok := i.(*hbuffer.Buffer); ok {
		_ = s.socket.WritePacket(buffer.GetBytes())
		s.bufferPool.Put(buffer)
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
			log.Printf("hrpc: server read pid error:%s", err.Error())
			break
		}
		mi, ok := s.getMethodInfo(pid)
		if !ok {
			log.Printf("hrpc: server method is nil,pid:%d error:%s", pid, err.Error())
			break
		}

		if mi.isOneWay() {
			args, err := s.decodeArgs(mi.args, buffer)
			if err != nil {
				log.Printf("hrpc: server decode args error:%s", err.Error())
				break
			}
			mi.method.Call([]reflect.Value{args})
			break
		}

		seq, err := buffer.ReadUint64()
		if err != nil {
			log.Printf("hrpc: server read seq error:%s", err.Error())
			break
		}

		args, err := s.decodeArgs(mi.args, buffer)
		if err != nil {
			log.Printf("hrpc: server decode args error:%s", err.Error())
			break
		}

		reply := reflect.New(mi.reply.Elem())
		returnValues := mi.method.Call([]reflect.Value{args, reply})

		buffer.Reset()
		buffer.WriteUint64(seq)
		errInter := returnValues[0].Interface()
		if errInter != nil {
			errMsg := errInter.(error).Error()
			buffer.WriteString(errMsg)
		} else {
			buffer.WriteString("") // error msg is empty
			err := s.translator.Marshal(reply.Interface(), buffer)
			if err != nil {
				log.Printf("hrpc: server encode reply error:%s", err.Error())
				break
			}
		}
		s.sendChannel.MustInput(buffer)
		break
	}
	return nil
}

// handler is
// 1. func (args interface{}) (send args only,no reply)
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

	isOneWay := mType.NumIn() == 1
	if !isOneWay { // handler will return error
		if mType.NumOut() != 1 {
			panic("hrpc: server register handler num out != 1")
		}
		outType := mType.Out(0)
		if outType != errorType {
			panic("hrpc: server register handler num out type is not error")
		}
	}

	methodInfo := &methodInfo{
		method: mValue,
		args:   mType.In(0),
	}
	if !isOneWay {
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
	}, s.bufferPool.Get)
}

func (s *Server) decodeArgs(argsType reflect.Type, argsBuffer *hbuffer.Buffer) (reflect.Value, error) {
	args := reflect.New(argsType.Elem())
	return args, s.translator.Unmarshal(argsBuffer, args.Interface())
}
