package server

import (
	"context"
	"sync"
	"time"
)

const minerTrackCacheTTL = time.Minute

var globalMinerTrackCache = newMinerTrackCache(minerTrackCacheTTL)
var minerTrackDBTimeout = 5 * time.Second

type minerTrackCache struct {
	mu   sync.Mutex
	ttl  time.Duration
	last map[string]time.Time
}

func newMinerTrackCache(ttl time.Duration) *minerTrackCache {
	return &minerTrackCache{
		ttl:  ttl,
		last: make(map[string]time.Time),
	}
}

func (c *minerTrackCache) shouldTrack(address string, now time.Time) (bool, time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.pruneExpiredLocked(now)
	if trackedAt, ok := c.last[address]; ok && now.Sub(trackedAt) < c.ttl {
		return false, trackedAt
	}
	c.last[address] = now
	return true, now
}

func (c *minerTrackCache) forgetIfMatch(address string, trackedAt time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.last[address].Equal(trackedAt) {
		delete(c.last, address)
	}
}

func (c *minerTrackCache) len() int {
	c.mu.Lock()
	defer c.mu.Unlock()

	return len(c.last)
}

func (c *minerTrackCache) pruneExpiredLocked(now time.Time) {
	for address, trackedAt := range c.last {
		if !now.Before(trackedAt.Add(c.ttl)) {
			delete(c.last, address)
		}
	}
}

func addMinerWithTimeout(add func(context.Context) error) error {
	ctx, cancel := context.WithTimeout(context.Background(), minerTrackDBTimeout)
	defer cancel()
	return add(ctx)
}
