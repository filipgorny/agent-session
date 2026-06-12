// Package session records and persists an agent's outbound message stream as an
// identified session. Store is abstract; SQLite and InMemory are implementations.
package session

import "github.com/filipgorny/agent/stream"

// Store persists session messages, keyed by session id.
type Store interface {
	Append(sessionID string, m stream.Message) error
	List(sessionID string) ([]stream.Message, error)
}
