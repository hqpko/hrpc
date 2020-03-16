package hrpc

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hqpko/hbuffer"
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

	timeoutCallDuration time.Duration
}

func NewClient() *Client {
	return NewClientWithOption(defCallTimeoutDuration)
}

// timeoutCallDuration 每个 Call 的超时时间
func NewClientWithOption(timeoutCallDuration time.Duration) *Client {
	client := &Client{
		conn:                newConn(),
		pending:             map[uint64]*Call{},
		timeoutCallDuration: timeoutCallDuration,
	}
	return client
}

func (c *Client) Run() error {
	return c.run(func(buffer *hbuffer.Buffer) {
		isCall, _ := buffer.ReadBool()
		if isCall {
			seq, _ := buffer.ReadUint64()
			c.callReply(seq, buffer.CopyRestOfBytes())
		} else {
			pid, _ := buffer.ReadInt32()
			c.handlerOneWay(pid, buffer.CopyRestOfBytes())
		}
	})
}

func (c *Client) Call(pid int32, args []byte) ([]byte, error) {
	return c.Go(pid, args).Done()
}

func (c *Client) Go(pid int32, args []byte) *Call {
	seq := atomic.AddUint64(&c.seq, 1)
	call := c.callPush(seq, c.timeoutCallDuration)

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
