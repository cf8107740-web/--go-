package singleflight

import "sync"

type call struct {
	wg  sync.WaitGroup
	val interface{}
	err error
}

type Group struct {
	m sync.Map
}

func (g *Group) Do(key string, fn func() (interface{}, error)) (interface{}, error) {
	c := &call{}
	c.wg.Add(1)

	if existing, loaded := g.m.LoadOrStore(key, c); loaded {
		existingCall := existing.(*call)
		existingCall.wg.Wait()
		return existingCall.val, existingCall.err
	}

	g.doCall(c, key, fn)
	return c.val, c.err
}

func (g *Group) doCall(c *call, key string, fn func() (interface{}, error)) {
	defer func() {
		c.wg.Done()
		g.m.Delete(key)
		if r := recover(); r != nil {
			panic(r)
		}
	}()
	c.val, c.err = fn()
}
