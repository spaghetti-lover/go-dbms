package kv

import "container/list"

type entry struct {
	key   string
	value []byte
}

type BufferPool struct {
	capacity int
	ll       *list.List
	cache    map[string]*list.Element
}

func NewBufferPool(cap int) *BufferPool {
	return &BufferPool{
		capacity: cap,
		ll:       list.New(),
		cache:    make(map[string]*list.Element),
	}
}

func (bp *BufferPool) Get(key string) ([]byte, bool) {
	if ele, ok := bp.cache[key]; ok {
		bp.ll.MoveToFront(ele)
		return ele.Value.(*entry).value, true
	}
	return nil, false
}

func (bp *BufferPool) Set(key string, value []byte) {
	if ele, ok := bp.cache[key]; ok {
		ele.Value.(*entry).value = value
		bp.ll.MoveToFront(ele)
		return
	}
	ele := bp.ll.PushFront(&entry{key, value})
	bp.cache[key] = ele
	if bp.ll.Len() > bp.capacity {
		last := bp.ll.Back()
		if last != nil {
			bp.ll.Remove(last)
			delete(bp.cache, last.Value.(*entry).key)
		}
	}
}

func (bp *BufferPool) Del(key string) {
	if ele, ok := bp.cache[key]; ok {
		bp.ll.Remove(ele)
		delete(bp.cache, key)
	}
}
