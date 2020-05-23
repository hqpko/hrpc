package hrpc

import (
	"errors"
	"sync"
	"time"
)

const defCallTimeoutDuration = time.Second * 8

var ErrCallTimeout = errors.New("call timeout")

type Call struct {
	reply []byte
	err   error
	c     chan *Call
	timer *time.Timer
}

func newCall() *Call {
	return &Call{c: make(chan *Call, 1)}
}

func (c *Call) setTimeout(timeout time.Duration, f func()) *Call {
	c.timer = time.AfterFunc(timeout, f)
	return c
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

type pending struct {
	lock    sync.Mutex
	seq     uint64
	timeout time.Duration
	pending map[uint64]*Call
}

func newPending() *pending {
	return &pending{timeout: defCallTimeoutDuration, pending: map[uint64]*Call{}}
}

func (p *pending) setTimeout(timeout time.Duration) {
	p.lock.Lock()
	defer p.lock.Unlock()
	p.timeout = timeout
}

func (p *pending) new() (*Call, uint64) {
	p.lock.Lock()
	defer p.lock.Unlock()
	p.seq++
	seq := p.seq
	call := newCall().setTimeout(p.timeout, func() {
		p.error(seq, ErrCallTimeout)
	})
	p.pending[seq] = call
	return call, seq
}

func (p *pending) reply(seq uint64, reply []byte) {
	if call := p.pop(seq); call != nil {
		call.doneWithReply(reply)
	}
}

func (p *pending) error(seq uint64, err error) {
	if call := p.pop(seq); call != nil {
		call.doneWithErr(err)
	}
}

func (p *pending) pop(seq uint64) *Call {
	p.lock.Lock()
	defer p.lock.Unlock()
	if call, ok := p.pending[seq]; ok {
		delete(p.pending, seq)
		return call
	}
	return nil
}
