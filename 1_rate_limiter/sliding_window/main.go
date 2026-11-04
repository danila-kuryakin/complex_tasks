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
*/

type Limiter interface {
	Allow(key string) bool
}

type visitor struct {
	sync.Mutex
	timestamp []time.Time
	lastSeen  time.Time
}

type SlidingWindowLimiter struct {
	rate   int
	window time.Duration
	ttl    time.Duration

	mu       sync.RWMutex
	visitors map[string]*visitor
	closeCh  chan struct{}
}

func NewSlidingWindowLimiter(rate int, window, ttl, cleanInterval time.Duration) *SlidingWindowLimiter {
	limiter := &SlidingWindowLimiter{
		rate:   rate,
		window: window,
		ttl:    ttl,

		visitors: make(map[string]*visitor),
		closeCh:  make(chan struct{}),
	}

	go limiter.cleanupLoop(cleanInterval)

	return limiter
}

func (l *SlidingWindowLimiter) Allow(key string) bool {
	now := time.Now()
	windowStart := now.Add(-l.window)

	v := l.GetOrCreateVisitor(key, now)

	v.Lock()
	defer v.Unlock()

	v.lastSeen = now

	validIndex := 0
	for _, t := range v.timestamp {
		if t.After(windowStart) {
			break
		}
		validIndex++
	}
	if validIndex > 0 {
		v.timestamp = v.timestamp[validIndex:]
	}

	if len(v.timestamp) < l.rate {
		v.timestamp = append(v.timestamp, now)
		return true
	}
	return false
}

func (l *SlidingWindowLimiter) GetOrCreateVisitor(key string, now time.Time) *visitor {
	l.mu.RLock()
	v, exist := l.visitors[key]
	l.mu.RUnlock()

	if exist {
		return v
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	v, exists := l.visitors[key]
	if !exists {
		v = &visitor{lastSeen: now}
		l.visitors[key] = v
	}
	return v
}

func (l *SlidingWindowLimiter) Stop() {
	close(l.closeCh)
}

func (l *SlidingWindowLimiter) cleanupLoop(interval time.Duration) {
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

func (l *SlidingWindowLimiter) cleanup() {
	now := time.Now()

	l.mu.Lock()
	defer l.mu.Unlock()

	for key, v := range l.visitors {
		v.Lock()
		inactive := now.Sub(v.lastSeen) > l.ttl
		v.Unlock()

		if inactive {
			delete(l.visitors, key)
		}
	}
}

func main() {
	var limiter Limiter = NewSlidingWindowLimiter(3, 2*time.Second, 4*time.Second, 5*time.Second)

	if l, ok := limiter.(*SlidingWindowLimiter); ok {
		defer l.Stop()
	}

	user1 := "user-1"

	for i := 1; i <= 5; i++ {
		allowed := limiter.Allow(user1)
		fmt.Printf("Request %d for %s: allowed = %v\n", i, user1, allowed)
		time.Sleep(300 * time.Millisecond)
	}

	fmt.Println("Waiting for window to reset...")
	time.Sleep(2 * time.Second) // Исправлена синтаксическая ошибка

	allowed := limiter.Allow(user1)
	fmt.Printf("Request after reset for %s: allowed = %v\n", user1, allowed)
}
