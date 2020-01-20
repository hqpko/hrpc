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
	seq     uint64
	pending sync.Map

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
			if value, ok := c.pending.Load(seq); ok {
				call := value.(*Call)
				call.doneWithReply(buffer.CopyRestOfBytes())
			}
		} else {
			pid, _ := buffer.ReadInt32()
			c.handlerOneWay(pid, buffer.CopyRestOfBytes())
		}
	})
}

func (c *Client) Call(pid int32, args []byte) ([]byte, error) {
	call, err := c.Go(pid, args)
	if err != nil {
		return nil, err
	}
	return call.Done()
}

func (c *Client) Go(pid int32, args []byte) (call *Call, err error) {
	call = c.newCall()
	seq := atomic.AddUint64(&c.seq, 1)
	c.pending.Store(seq, call)

	if err = c.call(pid, seq, args); err != nil {
		c.pending.Delete(seq)
	}
	return
}

func (c *Client) checkTimeout() {
	nowNano := time.Now().UnixNano()
	c.pending.Range(func(key, value interface{}) bool {
		call := value.(*Call)
		if call.timeoutNano <= nowNano {
			call.doneWithErr(Timeout)
			c.pending.Delete(key)
		}
		return true
	})
}

func (c *Client) newCall() *Call {
	return &Call{timeoutNano: time.Now().Add(c.timeoutCallDuration).UnixNano(), c: make(chan *Call, 1)}
}
