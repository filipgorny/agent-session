package session

import (
	"context"
	"sync"
	"time"

	"github.com/filipgorny/agent"
	"github.com/filipgorny/agent/stream"
)

// Session wraps an agent: it records every outbound message to a Store and
// re-exposes the stream, while accepting user input and answers. The agent owns
// the message stream and ask machinery; the Session adds identity + persistence.
type Session struct {
	agent *agent.Agent
	store Store
	id    string
	out   chan stream.Record
	done  chan struct{}
	once  sync.Once
}

// New starts a session over the agent, persisting to store.
func New(a *agent.Agent, store Store) *Session {
	s := &Session{
		agent: a,
		store: store,
		id:    newID(),
		out:   make(chan stream.Record, 256),
		done:  make(chan struct{}),
	}

	s.record(stream.Record{
		Type:      stream.TypeSession,
		Subtype:   "START",
		Payload:   map[string]any{"id": s.id, "root": a.Root()},
		CreatedAt: time.Now(),
	})

	go s.pump()

	return s
}

// ID returns the session id.
func (s *Session) ID() string {
	return s.id
}

// Stream returns the session's record stream (persisted as it flows).
func (s *Session) Stream() <-chan stream.Record {
	return s.out
}

// Send submits user text; the agent reasons in the background, streaming messages.
func (s *Session) Send(ctx context.Context, text string) {
	go func() {
		_, _ = s.agent.Ask(ctx, text)
	}()
}

// Answer delivers a user reply to a pending ask_user/ask_choice.
func (s *Session) Answer(text string) {
	s.agent.Answer(text)
}

// Close ends the session.
func (s *Session) Close() {
	s.once.Do(func() {
		s.record(stream.Record{Type: stream.TypeSession, Subtype: "END", CreatedAt: time.Now()})
		close(s.done)
	})
}

func (s *Session) pump() {
	for {
		select {

		case <-s.done:
			return

		case m := <-s.agent.Stream():
			s.record(m)
		}
	}
}

func (s *Session) record(m stream.Record) {
	_ = s.store.Append(s.id, m)

	select {

	case s.out <- m:

	default:
	}
}
