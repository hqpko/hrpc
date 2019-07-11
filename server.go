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

type server struct {
	lock       *sync.RWMutex
	bufferPool *hutils.BufferPool
	socket     *hnet.Socket
	translator Translator
	protocols  map[int32]*methodInfo

	sendChannel *hconcurrent.Concurrent
	readChannel *hconcurrent.Concurrent
}

func newServer(bufferPool *hutils.BufferPool) *server {
	s := &server{
		lock:       new(sync.RWMutex),
		bufferPool: bufferPool,
		protocols:  map[int32]*methodInfo{},
		translator: new(translatorProto),
	}
	s.sendChannel = hconcurrent.NewConcurrent(defChannelSize, 1, s.handlerSend)
	s.readChannel = hconcurrent.NewConcurrent(defChannelSize, defReadChannelCount, s.handlerRead)
	return s
}

func (s *server) setTranslator(translator Translator) *server {
	s.translator = translator
	return s
}

func (s *server) handlerSend(i interface{}) interface{} {
	if buffer, ok := i.(*hbuffer.Buffer); ok {
		_ = s.socket.WriteBuffer(buffer)
		s.bufferPool.Put(buffer)
	}
	return nil
}

func (s *server) handlerRead(i interface{}) interface{} {
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
			log.Printf("hrpc: server method is nil,pid:%d", pid)
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
		buffer.WriteEndianUint32(0)
		buffer.WriteByte(callTypeResponse)
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
		buffer.SetPosition(0)
		buffer.WriteEndianUint32(uint32(buffer.Len() - 4))
		s.sendChannel.MustInput(buffer)
		break
	}
	return nil
}

// handler is
// 1. func (args interface{}) (send args only,no reply)
// 2. func (args, reply interface{}) error
func (s *server) register(protocolID int32, handler interface{}) {
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

func (s *server) getMethodInfo(protocolID int32) (*methodInfo, bool) {
	s.lock.RLock()
	defer s.lock.RUnlock()
	methodInfo, ok := s.protocols[protocolID]
	return methodInfo, ok
}

func (s *server) setMethodInfo(protocolID int32, info *methodInfo) bool {
	s.lock.Lock()
	defer s.lock.Unlock()
	_, ok := s.protocols[protocolID]
	if ok {
		return false
	}
	s.protocols[protocolID] = info
	return true
}

func (s *server) run(socket *hnet.Socket) {
	s.socket = socket
	s.readChannel.Start()
	s.sendChannel.Start()
}

func (s *server) close() {
	s.readChannel.Stop()
	s.sendChannel.Stop()
}

func (s *server) decodeArgs(argsType reflect.Type, argsBuffer *hbuffer.Buffer) (reflect.Value, error) {
	args := reflect.New(argsType.Elem())
	return args, s.translator.Unmarshal(argsBuffer, args.Interface())
}
