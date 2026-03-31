package inmemory

import (
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
)

type Deduplication struct {
	mu           sync.RWMutex
	store        map[uuid.UUID]time.Time
	ttl          time.Duration
	vacuumPeriod time.Duration
}

func NewDeduplication() *Deduplication {
	return &Deduplication{
		store:        make(map[uuid.UUID]time.Time),
		ttl:          time.Minute * 5,
		vacuumPeriod: time.Minute * 1,
	}
}

func (dedup *Deduplication) Add(uuid uuid.UUID) {
	dedup.mu.Lock()
	defer dedup.mu.Unlock()
	dedup.store[uuid] = time.Now()
}

func (dedup *Deduplication) AddIfAbsent(id uuid.UUID) bool {
	dedup.mu.Lock()
	defer dedup.mu.Unlock()

	if _, exists := dedup.store[id]; exists {
		return false
	}

	dedup.store[id] = time.Now()
	return true
}

func (dedup *Deduplication) Remove(uuid uuid.UUID) {
	dedup.mu.Lock()
	defer dedup.mu.Unlock()
	delete(dedup.store, uuid)
}

func (dedup *Deduplication) Exist(uuid uuid.UUID) bool {
	dedup.mu.RLock()
	defer dedup.mu.RUnlock()
	_, exists := dedup.store[uuid]
	return exists
}

func (dedup *Deduplication) StartVacuum() {
	ticker := time.NewTicker(dedup.vacuumPeriod)

	go func() {
		defer ticker.Stop()
		for range ticker.C {
			slog.Info("Vacuum started")
			now := time.Now()

			var toDelete []uuid.UUID
			dedup.mu.RLock()
			for id, ts := range dedup.store {
				if now.Sub(ts) > dedup.ttl {
					toDelete = append(toDelete, id)
				}
			}
			dedup.mu.RUnlock()

			if len(toDelete) == 0 {
				continue // fast path, no need to acquire write lock
			}

			dedup.mu.Lock()
			for _, id := range toDelete {
				if ts, ok := dedup.store[id]; ok && now.Sub(ts) > dedup.ttl {
					delete(dedup.store, id)
				}
			}
			dedup.mu.Unlock()
		}
	}()
}