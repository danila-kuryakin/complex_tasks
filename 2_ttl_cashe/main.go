package main

import (
	"fmt"
	"sync"
	"time"
)

/*
2. TTL Cache

type Cache[K comparable, V any] interface {
    Set(key K, value V, ttl time.Duration)
    Get(key K) (V, bool)
    Delete(key K)
}

Требования:

* Get/Set/Delete безопасны конкурентно;
* TTL;
* просроченные элементы не должны возвращаться;
* background cleanup;
* cleanup не должен создавать goroutine для каждого элемента;
* настроить период cleanup;
* graceful shutdown.
*/

type Cache[K comparable, V any] interface {
	Set(key K, value V, ttl time.Duration)
	Get(key K) (V, bool)
	Delete(key K)
}

type item[V any] struct {
	value     V
	expiresAt time.Time
}

type TTLCache[K comparable, V any] struct {
	mu       sync.RWMutex
	items    map[K]item[V]
	stopChan chan struct{}
	done     chan struct{}
}

func NewTTLCache[K comparable, V any](cleanupInterval time.Duration) *TTLCache[K, V] {
	c := &TTLCache[K, V]{
		items:    make(map[K]item[V]),
		stopChan: make(chan struct{}),
		done:     make(chan struct{}),
	}

	go c.startCleanup(cleanupInterval)
	return c
}

func (c *TTLCache[K, V]) Set(key K, value V, ttl time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.items[key] = item[V]{
		value:     value,
		expiresAt: time.Now().Add(ttl),
	}
}

func (c *TTLCache[K, V]) Get(key K) (V, bool) {
	c.mu.RLock()
	it, exists := c.items[key]
	if !exists {
		c.mu.RUnlock()
		var zero V
		return zero, false
	}

	if time.Now().After(it.expiresAt) {
		c.mu.RUnlock()

		c.mu.Lock()

		it, exists = c.items[key]
		if exists && time.Now().After(it.expiresAt) {
			delete(c.items, key)
		}

		c.mu.Unlock()

		var zero V
		return zero, false
	}

	val := it.value
	c.mu.RUnlock()
	return val, true
}

func (c *TTLCache[K, V]) Delete(key K) {
	c.mu.Lock()
	defer c.mu.Unlock()

	delete(c.items, key)
}

func (c *TTLCache[K, V]) Close() {
	close(c.stopChan)
	<-c.done
}

func (c *TTLCache[K, V]) startCleanup(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	defer close(c.done)

	for {
		select {
		case <-ticker.C:
			c.deleteExpired()
		case <-c.stopChan:
			return
		}
	}
}

func (c *TTLCache[K, V]) deleteExpired() {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now()
	for k, it := range c.items {
		if now.After(it.expiresAt) {
			delete(c.items, k)
		}
	}
}

func main() {
	c := NewTTLCache[string, string](1 * time.Second)
	defer c.Close()

	c.Set("key1", "value1", 500*time.Millisecond)

	if val, ok := c.Get("key1"); ok {
		fmt.Println("Get key1:", val)
	}

	time.Sleep(600 * time.Millisecond)

	if _, ok := c.Get("key1"); !ok {
		fmt.Println("key1 expired and not found")
	}
}
