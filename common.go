package netx

import (
	"sync"
)

func newPipeManager() *pipeManager {
	return &pipeManager{
		m: make(map[uint32]*pipe),
	}
}

type pipeManager struct {
	m map[uint32]*pipe
	l sync.RWMutex
}

func (p *pipeManager) Set(k uint32, v *pipe) {
	p.l.Lock()
	p.m[k] = v
	p.l.Unlock()
}

func (p *pipeManager) Get(k uint32) (*pipe, bool) {
	p.l.RLock()
	v, ok := p.m[k]
	p.l.RUnlock()
	return v, ok
}

func (p *pipeManager) Del(k uint32) {
	p.l.Lock()
	delete(p.m, k)
	p.l.Unlock()
}

func newConnManager() *connManager {
	return &connManager{
		m: make(map[string]*Conn),
	}
}

type connManager struct {
	m map[string]*Conn
	l sync.RWMutex
}

func (c *connManager) Set(k string, v *Conn) {
	c.l.Lock()
	c.m[k] = v
	c.l.Unlock()
}
func (c *connManager) Get(k string) (*Conn, bool) {
	c.l.RLock()
	v, ok := c.m[k]
	c.l.RUnlock()
	return v, ok
}
func (c *connManager) Del(k string) {
	c.l.Lock()
	delete(c.m, k)
	c.l.Unlock()
}

func (c *connManager) RangeConn(fn func(conn *Conn) bool) {
	c.l.RLock()
	for _, conn := range c.m {
		if fn(conn) {
			break
		}
	}
	c.l.RUnlock()
}

func (c *connManager) Clear() {
	c.l.Lock()
	c.m = make(map[string]*Conn)
	c.l.Unlock()
}
