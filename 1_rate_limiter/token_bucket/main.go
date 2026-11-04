package main

import (
	"fmt"
	"sync"
	"time"
)

/*
Реализовать:

type Limiter interface {
    Allow(key string) bool
}

Требование:

> Для каждого пользователя разрешить не более N запросов за T секунд.

Например:

user-1 → 100 req / 1 min
user-2 → 100 req / 1 min

Требования:

* потокобезопасность;
* несколько пользователей;
* автоматическое удаление неактивных пользователей;
* не создавать goroutine на каждый запрос;
* корректно работать при высокой конкуренции.

**Усложнение:** сделать token bucket.
*/

type Limiter interface {
	Allow(key string) bool
}

type tokenVisitor struct {
	sync.Mutex
	tokens   float64
	lastTime time.Time
	lastSeen time.Time
}

type TokenBucketLimiter struct {
	capacity float64
	fillRate float64
	ttl      time.Duration

	mu      sync.RWMutex
	visitor map[string]*tokenVisitor
	closeCh chan struct{}
}

func NewTokenBucketLimiter(capacty float64, fillRate float64, ttl, cleanInterval time.Duration) *TokenBucketLimiter {
	limiter := &TokenBucketLimiter{
		capacity: capacty,
		fillRate: fillRate,
		ttl:      ttl,

		visitor: make(map[string]*tokenVisitor),
		closeCh: make(chan struct{}),
	}

	go limiter.cleanupLoop(cleanInterval)

	return limiter
}

func (l *TokenBucketLimiter) Allow(key string) bool {
	now := time.Now()
	v := l.getOrCreateVisitor(key, now)

	v.Lock()
	defer v.Unlock()

	v.lastSeen = now

	elapsed := now.Sub(v.lastTime).Seconds()
	v.lastTime = now

	v.tokens += elapsed * l.fillRate
	if v.tokens > l.capacity {
		v.tokens = l.capacity
	}

	if v.tokens >= 1 {
		v.tokens -= 1
		return true
	}
	return false
}

func (l *TokenBucketLimiter) getOrCreateVisitor(key string, now time.Time) *tokenVisitor {
	l.mu.RLock()
	v, exist := l.visitor[key]
	l.mu.RUnlock()

	if exist {
		return v
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	v, exists := l.visitor[key]
	if !exists {
		v = &tokenVisitor{
			tokens:   l.capacity,
			lastTime: now,
			lastSeen: now,
		}
		l.visitor[key] = v
	}
	return v
}

func (l *TokenBucketLimiter) cleanupLoop(interval time.Duration) {
	tk := time.NewTicker(interval)
	defer tk.Stop()

	for {
		select {
		case <-tk.C:
			l.cleanup()
		case <-l.closeCh:
			return
		}
	}
}

func (l *TokenBucketLimiter) cleanup() {
	now := time.Now()

	l.mu.Lock()
	defer l.mu.Unlock()

	for key, v := range l.visitor {
		v.Lock()
		inactive := now.Sub(v.lastSeen) > l.ttl
		v.Unlock()

		if inactive {
			delete(l.visitor, key)
		}
	}
}

func (l *TokenBucketLimiter) Stop() {
	close(l.closeCh)
}

func main() {
	var limiter Limiter = NewTokenBucketLimiter(3, 1.0, 10*time.Second, 5*time.Second)

	if l, ok := limiter.(*TokenBucketLimiter); ok {
		defer l.Stop()
	}

	var wg sync.WaitGroup
	users := []string{"user-1", "user-2", "user-3", "user-4", "user-5"}

	for _, user := range users {
		wg.Add(1)
		go func(u string) {
			defer wg.Done()

			for i := 1; i <= 5; i++ {
				allowed := limiter.Allow(u)
				fmt.Printf("[%s] Request %d: allowed = %v\n", u, i, allowed)
				time.Sleep(300 * time.Millisecond)
			}
		}(user)
	}

	wg.Wait()
	fmt.Println("All requests completed successfully.")
}
