package hrpc

import (
	"errors"
	"sync"
	"time"
)

const defCallTimeoutDuration = time.Second * 8

var ErrCallTimeout = errors.New("call timeout")

type call struct {
	reply []byte
	err   error
	c     chan *call
	timer *time.Timer
}

func newCall() *call {
	return &call{c: make(chan *call, 1)}
}

func (c *call) setTimeout(timeout time.Duration, f func()) *call {
	c.timer = time.AfterFunc(timeout, f)
	return c
}

func (c *call) done() {
	c.timer.Stop()
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
	<-c.c
	return c.reply, c.err
}

type pending struct {
	lock    sync.Mutex
	seq     uint64
	timeout time.Duration
	pending map[uint64]*call
}

func newPending() *pending {
	return &pending{timeout: defCallTimeoutDuration, pending: map[uint64]*call{}}
}

func (p *pending) setTimeout(timeout time.Duration) {
	p.lock.Lock()
	defer p.lock.Unlock()
	p.timeout = timeout
}

func (p *pending) new() (*call, uint64) {
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

func (p *pending) pop(seq uint64) *call {
	p.lock.Lock()
	defer p.lock.Unlock()
	if call, ok := p.pending[seq]; ok {
		delete(p.pending, seq)
		return call
	}
	return nil
}
