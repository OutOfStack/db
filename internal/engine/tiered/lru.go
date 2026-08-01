package tiered

import "container/list"

// lruEntryOverhead approximates the fixed per-entry cost (map bucket, list
// element, string headers) so the cache is sized by real memory, not payload
// bytes alone.
const lruEntryOverhead = 48

type lruKey struct {
	table string
	key   string
}

type lruNode struct {
	k     lruKey
	value string
	bytes int64
}

// lruCache is a byte-sized LRU: a map for O(1) lookup plus a list ordering
// entries most- to least-recently-used. It is not safe for concurrent use; the
// engine calls it under its mutex.
type lruCache struct {
	maxBytes int64
	curBytes int64
	items    map[lruKey]*list.Element
	order    *list.List // front = most recently used
}

func newLRU(maxBytes int64) *lruCache {
	return &lruCache{maxBytes: maxBytes, items: make(map[lruKey]*list.Element), order: list.New()}
}

func nodeBytes(k lruKey, value string) int64 {
	return int64(len(k.table) + len(k.key) + len(value) + lruEntryOverhead)
}

func (c *lruCache) get(table, key string) (string, bool) {
	element, ok := c.items[lruKey{table, key}]
	if !ok {
		return "", false
	}
	c.order.MoveToFront(element)
	//nolint:forcetypeassert // the list holds nothing but *lruNode; put is the only writer
	return element.Value.(*lruNode).value, true
}

// put caches value, evicting to stay within budget. An entry that alone exceeds
// the whole budget is never cached: it stays disk-only, so max_memory is a real
// ceiling. Any previously cached value for the key is dropped, never left stale.
func (c *lruCache) put(table, key, value string) {
	k := lruKey{table, key}
	c.remove(table, key)
	bytes := nodeBytes(k, value)
	if bytes > c.maxBytes {
		return
	}
	c.items[k] = c.order.PushFront(&lruNode{k: k, value: value, bytes: bytes})
	c.curBytes += bytes
	c.evict()
}

func (c *lruCache) remove(table, key string) {
	if element, ok := c.items[lruKey{table, key}]; ok {
		c.drop(element)
	}
}

// evict drops least-recently-used entries until the cache fits its budget.
func (c *lruCache) evict() {
	for c.curBytes > c.maxBytes {
		back := c.order.Back()
		if back == nil {
			return
		}
		c.drop(back)
	}
}

func (c *lruCache) drop(element *list.Element) {
	node := c.order.Remove(element).(*lruNode) //nolint:forcetypeassert // see get
	delete(c.items, node.k)
	c.curBytes -= node.bytes
}
