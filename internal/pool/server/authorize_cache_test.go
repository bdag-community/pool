package server

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestMinerTrackCacheSkipsWithinTTL(t *testing.T) {
	cache := newMinerTrackCache(time.Minute)
	now := time.Unix(100, 0)

	if ok, _ := cache.shouldTrack("0x1", now); !ok {
		t.Fatal("first track should be allowed")
	}
	if ok, _ := cache.shouldTrack("0x1", now.Add(30*time.Second)); ok {
		t.Fatal("second track inside TTL should be skipped")
	}
}

func TestMinerTrackCacheAllowsAfterTTL(t *testing.T) {
	cache := newMinerTrackCache(time.Minute)
	now := time.Unix(100, 0)

	if ok, _ := cache.shouldTrack("0x1", now); !ok {
		t.Fatal("first track should be allowed")
	}
	if ok, _ := cache.shouldTrack("0x1", now.Add(time.Minute+time.Nanosecond)); !ok {
		t.Fatal("track after TTL should be allowed")
	}
}

func TestMinerTrackCacheForgetIfMatch(t *testing.T) {
	cache := newMinerTrackCache(time.Minute)
	now := time.Unix(100, 0)

	ok, trackedAt := cache.shouldTrack("0x1", now)
	if !ok {
		t.Fatal("first track should be allowed")
	}
	cache.forgetIfMatch("0x1", trackedAt.Add(time.Nanosecond))
	if ok, _ := cache.shouldTrack("0x1", now.Add(30*time.Second)); ok {
		t.Fatal("mismatched forget should not clear cache entry")
	}
	cache.forgetIfMatch("0x1", trackedAt)
	if ok, _ := cache.shouldTrack("0x1", now.Add(31*time.Second)); !ok {
		t.Fatal("matched forget should clear cache entry")
	}
}

func TestMinerTrackCachePrunesExpiredEntries(t *testing.T) {
	cache := newMinerTrackCache(time.Minute)
	now := time.Unix(100, 0)

	for _, address := range []string{"0x1", "0x2", "0x3"} {
		if ok, _ := cache.shouldTrack(address, now); !ok {
			t.Fatalf("first track for %s should be allowed", address)
		}
	}
	if got := cache.len(); got != 3 {
		t.Fatalf("cache len = %d, want 3", got)
	}

	if ok, _ := cache.shouldTrack("0x4", now.Add(time.Minute)); !ok {
		t.Fatal("new track after TTL should be allowed")
	}
	if got := cache.len(); got != 1 {
		t.Fatalf("cache len after prune = %d, want only new address", got)
	}
}

func TestAddMinerWithTimeoutCancelsHungWrite(t *testing.T) {
	previousTimeout := minerTrackDBTimeout
	minerTrackDBTimeout = 5 * time.Millisecond
	t.Cleanup(func() { minerTrackDBTimeout = previousTimeout })

	start := time.Now()
	err := addMinerWithTimeout(func(ctx context.Context) error {
		<-ctx.Done()
		return ctx.Err()
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("addMinerWithTimeout error = %v, want deadline exceeded", err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("timeout took %s, want bounded duration", elapsed)
	}
}
