package hrpc

import (
	"errors"
	"sync"

	"github.com/hqpko/hbuffer"
	"github.com/hqpko/hnet"
)

const (
	msgTypeUnknown = iota
	msgTypeCall
	msgTypeOneWay
	msgTypeReply
)

var ErrConnClosed = errors.New("conn closed")

type conn struct {
	lock          sync.RWMutex
	socket        *hnet.Socket
	pending       *pending
	readBuffer    *hbuffer.Buffer
	writeBuffer   *hbuffer.Buffer
	handlerCall   func(pid int32, seq uint32, args []byte)
	handlerOneWay func(pid int32, args []byte)
}

func newConn(socket *hnet.Socket) *conn {
	return &conn{socket: socket, pending: newPending(), readBuffer: hbuffer.NewBuffer(), writeBuffer: hbuffer.NewBuffer()}
}

func (c *conn) setHandlerOneWay(handler func(pid int32, args []byte)) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.handlerOneWay = handler
}

func (c *conn) setHandlerCall(handler func(pid int32, seq uint32, args []byte)) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.handlerCall = handler
}

func (c *conn) Run() error {
	return c.socket.ReadPacket(func(packet []byte) {
		c.readBuffer.Reset().SetBytes(packet)
		msgType, _ := c.readBuffer.ReadByte()
		switch msgType {
		case msgTypeOneWay:
			pid, _ := c.readBuffer.ReadInt32()
			c.handlerOneWay(pid, c.readBuffer.GetRestOfBytes())
		case msgTypeCall:
			pid, _ := c.readBuffer.ReadInt32()
			seq, _ := c.readBuffer.ReadUint32()
			c.handlerCall(pid, seq, c.readBuffer.GetRestOfBytes())
		case msgTypeReply:
			seq, _ := c.readBuffer.ReadUint32()
			c.pending.reply(seq, c.readBuffer.GetRestOfBytes())
		}
	})
}

func (c *conn) OneWay(pid int32, args []byte) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.socket == nil {
		return ErrConnClosed
	}
	return c.socket.WriteBuffer(c.fillOneWay(pid, args))
}

func (c *conn) Call(pid int32, args []byte) ([]byte, error) {
	call, seq := c.pending.get()
	if err := c.tryCall(pid, seq, args); err != nil {
		c.pending.del(seq)
		c.pending.put(call)
		return nil, err
	}
	reply, err := call.Done()
	if err == ErrCallTimeout {
		c.pending.del(seq)
	} else {
		c.pending.put(call)
	}
	return reply, err
}

func (c *conn) tryCall(pid int32, seq uint32, args []byte) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.socket == nil {
		return ErrConnClosed
	}
	return c.socket.WriteBuffer(c.fillCall(pid, seq, args))
}

func (c *conn) reply(seq uint32, reply []byte) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.socket == nil {
		return ErrConnClosed
	}
	return c.socket.WriteBuffer(c.fillReply(seq, reply))
}

func (c *conn) fillReply(seq uint32, reply []byte) *hbuffer.Buffer {
	bf := c.writeBuffer.Reset()
	bf.WriteUint32(0)
	_ = bf.WriteByte(msgTypeReply)
	bf.WriteUint32(seq)
	bf.WriteBytes(reply)
	return bf.UpdateHead()
}

func (c *conn) fillOneWay(pid int32, args []byte) *hbuffer.Buffer {
	bf := c.writeBuffer.Reset()
	bf.WriteUint32(0)
	_ = bf.WriteByte(msgTypeOneWay)
	bf.WriteInt32(pid)
	bf.WriteBytes(args)
	return bf.UpdateHead()
}

func (c *conn) fillCall(pid int32, seq uint32, args []byte) *hbuffer.Buffer {
	bf := c.writeBuffer.Reset()
	bf.WriteUint32(0)
	_ = bf.WriteByte(msgTypeCall)
	bf.WriteInt32(pid)
	bf.WriteUint32(seq)
	bf.WriteBytes(args)
	return bf.UpdateHead()
}

func (c *conn) Close() (err error) {
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.socket != nil {
		err = c.socket.Close()
		c.socket = nil
	}
	return
}
