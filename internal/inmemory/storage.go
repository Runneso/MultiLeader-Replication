package inmemory

import (
	"HM3/internal/protocol"
	"sync"
)
type Storage struct {
	mu    sync.RWMutex
	store map[string]protocol.Entity
}

func NewStorage() *Storage {
	return &Storage{
		store: make(map[string]protocol.Entity),
	}
}

func (storage *Storage) Put(key, value string, version protocol.Version) {
	storage.mu.Lock()
	defer storage.mu.Unlock()
	if existing, exists := storage.store[key]; !exists || version.IsNewerThan(existing.Version) {
		storage.store[key] = protocol.Entity{
			Value:   value,
			Version: version,
		}
	}
}

func (storage *Storage) Get(key string) (protocol.Entity, bool) {
	storage.mu.RLock()
	defer storage.mu.RUnlock()
	value, exists := storage.store[key]
	return value, exists
}

func (storage *Storage) Dump() map[string]protocol.Entity {
	storage.mu.RLock()
	defer storage.mu.RUnlock()

	storageCopy := make(map[string]protocol.Entity, len(storage.store))
	for k, v := range storage.store {
		storageCopy[k] = v
	}
	return storageCopy
}
