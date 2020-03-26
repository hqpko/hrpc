package hrpc

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hqpko/hbuffer"
	"github.com/hqpko/hnet"
)

const (
	defCallTimeoutDuration = time.Second * 8
)

var Timeout = errors.New("call timeout")

type Call struct {
	reply []byte
	err   error
	c     chan *Call
	timer *time.Timer
}

func (c *Call) done() {
	c.timer.Stop()
	select {
	case c.c <- c:
	default:
	}
}

func (c *Call) doneWithErr(err error) {
	c.err = err
	c.done()
}

func (c *Call) doneWithReply(reply []byte) {
	c.reply = reply
	c.done()
}

func (c *Call) Done() ([]byte, error) {
	<-c.c
	return c.reply, c.err
}

type Client struct {
	*conn
	seq         uint64
	pendingLock sync.Mutex
	pending     map[uint64]*Call

	callTimeoutDuration time.Duration
}

func NewClient(socket *hnet.Socket) *Client {
	client := &Client{
		conn:                newConn(socket),
		pending:             map[uint64]*Call{},
		callTimeoutDuration: defCallTimeoutDuration,
	}
	return client
}

func (c *Client) SetCallTimeout(timeoutDuration time.Duration) *Client {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.callTimeoutDuration = timeoutDuration
	return c
}

func (c *Client) SetHandlerOneWay(handler func(pid int32, args []byte)) *Client {
	c.setHandlerOneWay(handler)
	return c
}

func (c *Client) Run() error {
	return c.run(func(buffer *hbuffer.Buffer) {
		msgType, _ := buffer.ReadByte()
		if msgType == msgTypeOneWay {
			pid, _ := buffer.ReadInt32()
			c.handlerOneWay(pid, buffer.CopyRestOfBytes())
		} else if msgType == msgTypeReply {
			seq, _ := buffer.ReadUint64()
			c.callReply(seq, buffer.CopyRestOfBytes())
		}
	})
}

func (c *Client) Call(pid int32, args []byte) ([]byte, error) {
	return c.Go(pid, args).Done()
}

func (c *Client) Go(pid int32, args []byte) *Call {
	seq := atomic.AddUint64(&c.seq, 1)
	call := c.callPush(seq, c.callTimeoutDuration)

	if err := c.call(pid, seq, args); err != nil {
		c.callError(seq, err)
	}
	return call
}

func (c *Client) callPush(seq uint64, timeout time.Duration) *Call {
	call := &Call{c: make(chan *Call, 1), timer: time.AfterFunc(timeout, func() {
		c.callError(seq, Timeout)
	})}
	c.pendingLock.Lock()
	defer c.pendingLock.Unlock()
	c.pending[seq] = call
	return call
}

func (c *Client) callReply(seq uint64, reply []byte) {
	if call := c.callPop(seq); call != nil {
		call.doneWithReply(reply)
	}
}

func (c *Client) callError(seq uint64, err error) {
	if call := c.callPop(seq); call != nil {
		call.doneWithErr(err)
	}
}

func (c *Client) callPop(seq uint64) *Call {
	c.pendingLock.Lock()
	defer c.pendingLock.Unlock()
	if call, ok := c.pending[seq]; ok {
		delete(c.pending, seq)
		return call
	}
	return nil
}
