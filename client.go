package hrpc

import (
	"sync"
	"time"

	"github.com/hqpko/hbuffer"
	"github.com/hqpko/hnet"
	"github.com/hqpko/hutils"
)

type Call struct {
	reply       []byte
	timeoutNano int64
	c           chan *Call
}

func (c *Call) done() {
	select {
	case c.c <- c:
	default:
	}
}

func (c *Call) Done() []byte {
	<-c.c
	return c.reply
}

type Client struct {
	lock       sync.RWMutex
	bufferPool *hutils.BufferPool
	seq        uint64
	pending    sync.Map
	protocols  sync.Map
	socket     *hnet.Socket

	timeoutStep     time.Duration
	timeoutTicker   *time.Ticker
	timeoutDuration time.Duration
}

func NewClient(socket *hnet.Socket) *Client {
	return &Client{
		socket:          socket,
		bufferPool:      hutils.NewBufferPool(),
		timeoutDuration: time.Second * 8,
	}
}

// RegisterOneWay, client can only register one way
func (c *Client) RegisterOneWay(pid int32, handler func(args []byte)) {
	c.protocols.Store(pid, handler)
}

func (c *Client) Run() error {
	c.timeoutTicker = time.NewTicker(c.timeoutDuration)
	go c.loopTimeout()
	defer c.timeoutTicker.Stop()

	return c.socket.ReadBuffer(func(buffer *hbuffer.Buffer) {
		isReply, _ := buffer.ReadBool()
		if isReply {
			seq, _ := buffer.ReadUint64()
			if value, ok := c.pending.Load(seq); ok {
				call := value.(*Call)
				call.reply = buffer.CopyRestOfBytes()
				call.done()
			}
		} else {
			pid, _ := buffer.ReadInt32()
			if handler, ok := c.protocols.Load(pid); ok {
				handler.(func(args []byte))(buffer.CopyRestOfBytes())
			}
		}
		c.bufferPool.Put(buffer)
	}, c.bufferPool.Get)
}

func (c *Client) loopTimeout() {
	for {
		<-c.timeoutTicker.C
	}
}

func (c *Client) Call(pid int32, args []byte) ([]byte, error) {
	call, err := c.Go(pid, args)
	if err != nil {
		return nil, err
	}
	return call.Done(), nil
}

func (c *Client) OneWay(pid int32, args []byte) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	buf := c.bufferPool.Get()
	defer c.bufferPool.Put(buf)
	c.fillOneWay(buf, pid, args)
	return c.socket.WriteBuffer(buf)
}

func (c *Client) Go(pid int32, args []byte) (call *Call, err error) {
	c.lock.Lock()
	defer c.lock.Unlock()
	call = c.newCall()
	c.seq++
	c.pending.Store(c.seq, call)

	buf := c.bufferPool.Get()
	defer c.bufferPool.Put(buf)
	c.fillCall(buf, pid, c.seq, args)
	if err = c.socket.WriteBuffer(buf); err != nil {
		c.pending.Delete(c.seq)
	}
	return
}

func (c *Client) fillCall(buffer *hbuffer.Buffer, pid int32, seq uint64, args []byte) {
	buffer.WriteEndianUint32(0)
	buffer.WriteInt32(pid)
	buffer.WriteUint64(seq)
	buffer.WriteBytes(args)
	buffer.SetPosition(0)
	buffer.WriteEndianUint32(uint32(buffer.Len() - 4))
}

func (c *Client) fillOneWay(buffer *hbuffer.Buffer, pid int32, args []byte) {
	buffer.WriteEndianUint32(0)
	buffer.WriteInt32(pid)
	buffer.WriteBytes(args)
	buffer.SetPosition(0)
	buffer.WriteEndianUint32(uint32(buffer.Len() - 4))
}

func (c *Client) newCall() *Call {
	return &Call{timeoutNano: time.Now().Add(c.timeoutDuration).UnixNano(), c: make(chan *Call, 1)}
}
