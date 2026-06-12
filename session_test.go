package session

import (
	"path/filepath"
	"testing"
	"time"

	agentpkg "github.com/filipgorny/agent"
	"github.com/filipgorny/agent/config"
	"github.com/filipgorny/agent/stream"
	llm "github.com/filipgorny/llm-provider"
)

func TestStores(t *testing.T) {
	stores := map[string]func(t *testing.T) Store{
		"inmemory": func(t *testing.T) Store { return NewInMemory() },
		"sqlite": func(t *testing.T) Store {
			s, err := NewSQLite(filepath.Join(t.TempDir(), "s.db"))

			if err != nil {
				t.Fatalf("sqlite: %v", err)
			}

			return s
		},
	}

	for name, build := range stores {

		t.Run(name, func(t *testing.T) {
			st := build(t)

			_ = st.Append("s1", stream.Message{Type: stream.TypeLog, Subtype: stream.LogToolCall, Payload: map[string]any{"action": "grep"}, CreatedAt: time.Now()})
			_ = st.Append("s1", stream.Message{Type: stream.TypeAnswerUser, Payload: "hi", CreatedAt: time.Now()})
			_ = st.Append("s2", stream.Message{Type: stream.TypeSession, CreatedAt: time.Now()})

			got, err := st.List("s1")

			if err != nil {
				t.Fatalf("list: %v", err)
			}

			if len(got) != 2 || got[0].Type != stream.TypeLog || got[1].Type != stream.TypeAnswerUser {
				t.Fatalf("s1 = %+v", got)
			}

			if got[1].Payload != "hi" {
				t.Errorf("payload = %v", got[1].Payload)
			}
		})
	}
}

func buildAgent(t *testing.T) *agentpkg.Agent {
	t.Helper()

	a, err := agentpkg.NewAgentFromConfig(config.Config{
		Llm:    llm.Config{Llm: "ollama", Ollama: llm.OllamaConfig{URL: "http://localhost:11434", Model: "m"}},
		Memory: config.MemoryConfig{Backend: "inmemory"},
	})

	if err != nil {
		t.Fatalf("build agent: %v", err)
	}

	return a
}

func TestSessionRecordsStart(t *testing.T) {
	store := NewInMemory()
	s := New(buildAgent(t), store)

	defer s.Close()

	select {

	case m := <-s.Messages():

		if m.Type != stream.TypeSession || m.Subtype != "START" {
			t.Fatalf("first message = %s/%s, want SESSION/START", m.Type, m.Subtype)
		}

	case <-time.After(time.Second):
		t.Fatal("no SESSION/START message")
	}

	persisted, _ := store.List(s.ID())

	if len(persisted) == 0 || persisted[0].Type != stream.TypeSession {
		t.Errorf("store missing SESSION/START: %+v", persisted)
	}
}
