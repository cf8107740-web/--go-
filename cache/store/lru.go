package store

import (
	"container/list"
	"sync"
	"time"
)

// lruCache 是基于标准库 list 的 LRU 缓存实现
type lruCache struct {
	mu              sync.RWMutex
	list            *list.List                    // 双向链表，用于维护 LRU 顺序
	items           map[string]*list.Element      // 键到链表节点的映射
	expires         map[string]time.Time          // 过期时间映射
	maxBytes        int64                         // 最大允许字节数
	usedBytes       int64                         // 当前使用的字节数
	onEvicted       func(key string, value Value) //淘汰回调
	cleanupInterval time.Duration
	cleanupTicker   *time.Ticker
	closeCh         chan struct{} // 用于优雅关闭清理协程
}

// lruEntry 表示缓存中的一个条目
type lruEntry struct {
	key   string
	value Value
}

// newLRUCache 创建一个新的 LRU 缓存实例
func newLRUCache(opts Options) *lruCache {
	// 设置默认清理间隔
	cleanupInterval := opts.CleanupInterval
	if cleanupInterval <= 0 {
		cleanupInterval = time.Minute
	}

	c := &lruCache{
		list:            list.New(),
		items:           make(map[string]*list.Element),
		expires:         make(map[string]time.Time),
		maxBytes:        opts.MaxBytes,
		onEvicted:       opts.OnEvicted,
		cleanupInterval: cleanupInterval,
		closeCh:         make(chan struct{}),
	}

	// 启动定期清理协程
	c.cleanupTicker = time.NewTicker(c.cleanupInterval)
	go c.cleanupLoop()

	return c
}

// Get获取缓存项，如果存在且未过期返回
func (c *lruCache) Get(key string) (Value, bool) {
	c.mu.RLock()
	elem, ok := c.items[key]
	if !ok {
		c.mu.RUnlock()
		return nil, false
	}
	//检查是否过期
	if expTime, hasExp := c.expires[key]; hasExp && time.Now().After(expTime) {
		c.mu.RUnlock()
		//如果过期,异步删除过期项
		go c.Delete(key)
		return nil, false
	}
	//获取值并释放读锁
	entry := elem.Value.(*lruEntry)
	value := entry.value
	c.mu.RUnlock()
	// 更新 LRU 位置需要写锁
	c.mu.Lock()
	// 再次检查元素是否仍然存在（可能在获取写锁期间被其他协程删除）
	if _, ok := c.items[key]; ok {
		c.list.MoveToBack(elem)
	}
	c.mu.Unlock()

	return value, true
}

// Set添加或者更新缓存项
func (c *lruCache) Set(key string, value Value) error {
	return c.SetWithExpiration(key, value, 0)
}

// SetWithExpiration相比于set，设置了过期时间
func (c *lruCache) SetWithExpiration(key string, value Value, expiration time.Duration) error {
	if value == nil {
		c.Delete(key)
		return nil
	}
	//上写锁
	c.mu.Lock()
	//保证写完成之后再解锁
	defer c.mu.Unlock()
	var expTime time.Time
	//判断是否是永久
	if expiration > 0 {
		expTime = time.Now().Add(expiration)
		c.expires[key] = expTime
	} else {
		delete(c.expires, key)
	}
	//查看是否已经存在，如果存在，更新
	if elem, ok := c.items[key]; ok {
		//取出旧节点的value
		oldEntry := elem.Value.(*lruEntry)
		//更新字节数
		c.usedBytes += int64(value.Len() - oldEntry.value.Len())
		//更新value
		oldEntry.value = value
		//移动到最后
		c.list.MoveToBack(elem)
		return nil
	}
	//添加新项
	entry := &lruEntry{key: key, value: value}
	elem := c.list.PushBack(entry)
	c.items[key] = elem
	c.usedBytes += int64(len(key) + value.Len())
	//检查是否需要淘汰旧项
	c.evict()

	return nil
}

// 移除操作
func (c *lruCache) removeElement(elem *list.Element) {
	entry := elem.Value.(*lruEntry)
	c.list.Remove(elem)
	delete(c.items, entry.key)
	delete(c.expires, entry.key)
	c.usedBytes -= int64(len(entry.key) + entry.value.Len())
	if c.onEvicted != nil {
		c.onEvicted(entry.key, entry.value)
	}
}

// 旧项淘汰
func (c *lruCache) evict() {
	now := time.Now()
	for key, expTime := range c.expires {
		if now.After(expTime) {
			if elem, ok := c.items[key]; ok {
				c.removeElement(elem)
			}
		}
	}
	//再根据内存大小删除最久未使用的

	for c.maxBytes > 0 && c.usedBytes > c.maxBytes && c.list.Len() > 0 {
		elem := c.list.Front() // 获取最久未使用的项（链表头部）
		if elem != nil {
			c.removeElement(elem)
		}
	}
}

// cleanupLoop 定期清理过期缓存的协程
func (c *lruCache) cleanupLoop() {
	for {
		select {
		case <-c.cleanupTicker.C:
			c.mu.Lock()
			c.evict()
			c.mu.Unlock()
		case <-c.closeCh:
			return
		}
	}
}

// Delete 从缓存中删除指定键的项
func (c *lruCache) Delete(key string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	if elem, ok := c.items[key]; ok {
		c.removeElement(elem)
		return true
	}
	return false
}

// Clear 清空缓存
func (c *lruCache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()

	// 如果设置了回调函数，遍历所有项调用回调
	if c.onEvicted != nil {
		for _, elem := range c.items {
			entry := elem.Value.(*lruEntry)
			c.onEvicted(entry.key, entry.value)
		}
	}

	c.list.Init()
	c.items = make(map[string]*list.Element)
	c.expires = make(map[string]time.Time)
	c.usedBytes = 0
}

// Keys 返回缓存中所有键
func (c *lruCache) Keys() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	keys := make([]string, 0, len(c.items))
	for key := range c.items {
		keys = append(keys, key)
	}
	return keys
}

// GetValue 获取指定键的原始值（不更新 LRU 位置）
func (c *lruCache) GetValue(key string) (Value, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	elem, ok := c.items[key]
	if !ok {
		return nil, false
	}
	if expTime, hasExp := c.expires[key]; hasExp && time.Now().After(expTime) {
		return nil, false
	}
	entry := elem.Value.(*lruEntry)
	return entry.value, true
}

// Close 关闭缓存，停止清理协程
func (c *lruCache) Close() {
	if c.cleanupTicker != nil {
		c.cleanupTicker.Stop()
		close(c.closeCh)
	}
}

// Len 返回缓存中的项数
func (c *lruCache) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.list.Len()
}
