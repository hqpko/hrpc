package hrpc

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hqpko/hbuffer"
)

const (
	defCallTimeoutDuration      = time.Second * 8
	defCheckTimeoutStepDuration = time.Millisecond * 100
)

var Timeout = errors.New("call timeout")

type Call struct {
	reply       []byte
	timeoutNano int64
	err         error
	c           chan *Call
}

func (c *Call) done() {
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

type timeoutTicker struct {
	ticker *time.Ticker
	step   time.Duration
	f      func()
	close  chan bool
}

func newTimeoutTicker(step time.Duration, f func()) *timeoutTicker {
	return &timeoutTicker{step: step, f: f, close: make(chan bool, 1)}
}

func (tt *timeoutTicker) start() {
	tt.ticker = time.NewTicker(tt.step)
	for {
		select {
		case <-tt.ticker.C:
			tt.f()
		case <-tt.close:
			return
		}
	}
}

func (tt *timeoutTicker) stop() {
	if tt.ticker != nil {
		tt.ticker.Stop()
	}
	tt.close <- true
}

type Client struct {
	*conn
	seq         uint64
	pendingLock sync.Mutex
	pending     map[uint64]*Call

	timeoutTicker       *timeoutTicker
	timeoutCallDuration time.Duration
}

func NewClient() *Client {
	return NewClientWithOption(defCheckTimeoutStepDuration, defCallTimeoutDuration)
}

// timeoutCheckStep 每隔 step 时间检查一次所有未完成的 Call 的超时状态
// timeoutCallDuration 每个 Call 的超时时间
func NewClientWithOption(timeoutCheckStep, timeoutCallDuration time.Duration) *Client {
	client := &Client{
		conn:                newConn(),
		pending:             map[uint64]*Call{},
		timeoutCallDuration: timeoutCallDuration,
	}
	client.timeoutTicker = newTimeoutTicker(timeoutCheckStep, client.checkTimeout)
	return client
}

func (c *Client) Run() error {
	go c.timeoutTicker.start()
	defer c.timeoutTicker.stop()

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
	call := c.callPush(seq)

	if err := c.call(pid, seq, args); err != nil {
		c.callError(seq, err)
	}
	return call
}

func (c *Client) checkTimeout() {
	nowNano := time.Now().UnixNano()
	c.pendingLock.Lock()
	defer c.pendingLock.Unlock()
	for seq, call := range c.pending {
		if call.timeoutNano <= nowNano {
			call.doneWithErr(Timeout)
			delete(c.pending, seq)
		}
	}
}

func (c *Client) callPush(seq uint64) *Call {
	call := &Call{timeoutNano: time.Now().Add(c.timeoutCallDuration).UnixNano(), c: make(chan *Call, 1)}
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
