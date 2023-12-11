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
	c     chan []byte
	timer *time.Timer
}

func newCall() *call {
	return &call{c: make(chan []byte, 1), timer: time.NewTimer(defCallTimeoutDuration)}
}

func (c *call) reset() {
	if !c.timer.Stop() {
		select {
		case <-c.timer.C:
		default:
		}
	}
	select {
	case <-c.c:
	default:
	}
}

func (c *call) reply(reply []byte) {
	select {
	case c.c <- reply:
	default:
	}
}

func (c *call) Done() ([]byte, error) {
	select {
	case b := <-c.c:
		return b, nil
	case <-c.timer.C:
		return nil, ErrCallTimeout
	}
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

func (p *pending) put(seq uint32, call *call) {
	p.del(seq)
	call.reset()
	callPool.Put(call)
}

func (p *pending) reply(seq uint32, reply []byte) {
	if c := p.pop(seq); c != nil {
		c.reply(reply)
	}
}

func (p *pending) pop(seq uint32) *call {
	p.lock.Lock()
	defer p.lock.Unlock()
	if c, ok := p.pending[seq]; ok {
		delete(p.pending, seq)
		return c
	}
	return nil
}

func (p *pending) del(seq uint32) {
	p.lock.Lock()
	defer p.lock.Unlock()
	delete(p.pending, seq)
}
