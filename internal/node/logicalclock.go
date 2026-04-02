package node

import "sync"

type LogicalClock struct {
	mu      sync.RWMutex
	lamport int
}

func NewLogicalClock() *LogicalClock {
	return &LogicalClock{
		lamport: 0,
	}
}

func (clock *LogicalClock) Tick() int {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	clock.lamport++
	return clock.lamport
}

func (clock *LogicalClock) Update(remote int) {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	if remote > clock.lamport {
		clock.lamport = remote
	}
	clock.lamport++
}
