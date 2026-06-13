package session

import (
	"sync"

	"github.com/filipgorny/agent/stream"
)

// InMemory is a non-persistent Store (tests / no-save mode).
type InMemory struct {
	mu sync.Mutex
	m  map[string][]stream.Record
}

func NewInMemory() *InMemory {
	return &InMemory{m: map[string][]stream.Record{}}
}

func (s *InMemory) Append(sessionID string, m stream.Record) error {
	s.mu.Lock()

	defer s.mu.Unlock()

	s.m[sessionID] = append(s.m[sessionID], m)

	return nil
}

func (s *InMemory) List(sessionID string) ([]stream.Record, error) {
	s.mu.Lock()

	defer s.mu.Unlock()

	return append([]stream.Record(nil), s.m[sessionID]...), nil
}
