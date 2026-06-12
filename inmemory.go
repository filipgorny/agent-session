package session

import (
	"sync"

	"github.com/filipgorny/agent/stream"
)

// InMemory is a non-persistent Store (tests / no-save mode).
type InMemory struct {
	mu sync.Mutex
	m  map[string][]stream.Message
}

func NewInMemory() *InMemory {
	return &InMemory{m: map[string][]stream.Message{}}
}

func (s *InMemory) Append(sessionID string, m stream.Message) error {
	s.mu.Lock()

	defer s.mu.Unlock()

	s.m[sessionID] = append(s.m[sessionID], m)

	return nil
}

func (s *InMemory) List(sessionID string) ([]stream.Message, error) {
	s.mu.Lock()

	defer s.mu.Unlock()

	return append([]stream.Message(nil), s.m[sessionID]...), nil
}
