package pool

import (
	"container/list"
	"sync"
	"time"
)

type stickyBinding struct {
	key       string
	tag       string
	expiresAt time.Time
}

// stickyCache is a bounded, expiring LRU. Source identities can be attacker
// controlled when a listener is exposed, so affinity must never be an
// unbounded map.
type stickyCache struct {
	mu         sync.Mutex
	ttl        time.Duration
	maxEntries int
	items      map[string]*list.Element
	lru        list.List
}

func newStickyCache(ttl time.Duration, maxEntries int) *stickyCache {
	if ttl <= 0 {
		ttl = 30 * time.Minute
	}
	if maxEntries <= 0 {
		maxEntries = 4096
	}
	return &stickyCache{
		ttl:        ttl,
		maxEntries: maxEntries,
		items:      make(map[string]*list.Element, maxEntries),
	}
}

func (c *stickyCache) get(key string, now time.Time) (string, bool) {
	if c == nil || key == "" {
		return "", false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	element, ok := c.items[key]
	if !ok {
		return "", false
	}
	binding := element.Value.(*stickyBinding)
	if !binding.expiresAt.After(now) {
		c.removeElement(element)
		return "", false
	}
	binding.expiresAt = now.Add(c.ttl)
	c.lru.MoveToFront(element)
	return binding.tag, true
}

func (c *stickyCache) set(key, tag string, now time.Time) {
	if c == nil || key == "" || tag == "" {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if element, ok := c.items[key]; ok {
		binding := element.Value.(*stickyBinding)
		binding.tag = tag
		binding.expiresAt = now.Add(c.ttl)
		c.lru.MoveToFront(element)
		return
	}
	element := c.lru.PushFront(&stickyBinding{key: key, tag: tag, expiresAt: now.Add(c.ttl)})
	c.items[key] = element
	for len(c.items) > c.maxEntries {
		c.removeElement(c.lru.Back())
	}
}

func (c *stickyCache) delete(key string) {
	if c == nil || key == "" {
		return
	}
	c.mu.Lock()
	if element, ok := c.items[key]; ok {
		c.removeElement(element)
	}
	c.mu.Unlock()
}

func (c *stickyCache) removeElement(element *list.Element) {
	if element == nil {
		return
	}
	binding := element.Value.(*stickyBinding)
	delete(c.items, binding.key)
	c.lru.Remove(element)
}

func (c *stickyCache) len() int {
	if c == nil {
		return 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.items)
}
