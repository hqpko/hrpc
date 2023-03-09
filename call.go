package hrpc

import (
	"errors"
	"sync"
	"time"
)

const defCallTimeoutDuration = time.Second * 8

var ErrCallTimeout = errors.New("call timeout")
var callPool = sync.Pool{
	New: func() interface{} { return newCall() },
}

type call struct {
	reply []byte
	err   error
	c     chan *call
	timer *time.Timer
}

func newCall() *call {
	return &call{c: make(chan *call, 1), timer: time.NewTimer(defCallTimeoutDuration)}
}

func (c *call) reset() {
	if !c.timer.Stop() && c.err != ErrCallTimeout {
		<-c.timer.C
	}
	c.reply = nil
	c.err = nil
}

func (c *call) done() {
	select {
	case c.c <- c:
	default:
	}
}

func (c *call) doneWithErr(err error) {
	c.err = err
	c.done()
}

func (c *call) doneWithReply(reply []byte) {
	c.reply = reply
	c.done()
}

func (c *call) Done() ([]byte, error) {
	select {
	case <-c.c:
	case <-c.timer.C:
		c.err = ErrCallTimeout
	}
	return c.reply, c.err
}

type pending struct {
	lock    sync.Mutex
	seq     uint32
	timeout time.Duration
	pending map[uint32]*call
}

func newPending() *pending {
	return &pending{timeout: defCallTimeoutDuration, pending: map[uint32]*call{}}
}

func (p *pending) setTimeout(timeout time.Duration) {
	p.lock.Lock()
	defer p.lock.Unlock()
	p.timeout = timeout
}

func (p *pending) get() (*call, uint32) {
	p.lock.Lock()
	defer p.lock.Unlock()
	p.seq++
	seq := p.seq
	call := callPool.Get().(*call)
	call.timer.Reset(p.timeout)
	p.pending[seq] = call
	return call, seq
}

func (p *pending) put(call *call) {
	call.reset()
	callPool.Put(call)
}

func (p *pending) reply(seq uint32, reply []byte) {
	if call := p.pop(seq); call != nil {
		call.doneWithReply(reply)
	}
}

func (p *pending) error(seq uint32, err error) {
	if call := p.pop(seq); call != nil {
		call.doneWithErr(err)
	}
}

func (p *pending) pop(seq uint32) *call {
	p.lock.Lock()
	defer p.lock.Unlock()
	if call, ok := p.pending[seq]; ok {
		delete(p.pending, seq)
		return call
	}
	return nil
}
