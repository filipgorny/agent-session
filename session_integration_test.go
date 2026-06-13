//go:build integration

package session

import (
	"context"
	"os"
	"testing"
	"time"

	agentpkg "github.com/filipgorny/agent"
	"github.com/filipgorny/agent/config"
	"github.com/filipgorny/agent/stream"
	llm "github.com/filipgorny/llm-provider"
)

// TestSessionStreamIntegration runs a real agent (Ollama) through a Session and
// verifies the end-to-end stream: tool-call LOGs + a final ANSWER_USER, all
// persisted to the store. No TUI involved. Configure with OLLAMA_URL/OLLAMA_MODEL.
// Run with: go test -tags integration ./...
func TestSessionStreamIntegration(t *testing.T) {
	url := os.Getenv("OLLAMA_URL")

	if url == "" {
		url = "http://localhost:11434"
	}

	model := os.Getenv("OLLAMA_MODEL")

	if model == "" {
		model = "qwen3:14b"
	}

	a, err := agentpkg.NewAgentFromConfig(config.Config{
		Interactive: true,
		Llm: llm.Config{Llm: "ollama", Ollama: llm.OllamaConfig{
			URL: url, Model: model, Options: map[string]any{"num_ctx": 8192},
		}},
		Plugins: []string{"files", "reasoning"},
		Skills:  []string{"dir_list", "grep", "think"},
		Memory:  config.MemoryConfig{Backend: "inmemory"},
	})

	if err != nil {
		t.Fatalf("build agent: %v", err)
	}

	store := NewInMemory()
	s := New(a, store)

	defer s.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)

	defer cancel()

	s.Send(ctx, "How many Go source files (.go) are in the current directory? Use the available tools to find out.")

	types := map[string]bool{}
	answer := ""
	deadline := time.After(180 * time.Second)

loop:
	for {
		select {

		case m := <-s.Stream():
			types[m.Type+"/"+m.Subtype] = true

			if m.Type == stream.TypeAnswerUser {
				answer, _ = m.Payload.(string)

				break loop
			}

		case <-deadline:
			t.Fatalf("no ANSWER_USER within timeout; saw %v", types)
		}
	}

	t.Logf("stream types: %v", types)
	t.Logf("answer: %q", answer)

	if answer == "" {
		t.Error("empty final answer")
	}

	if !types[stream.TypeLog+"/"+stream.LogToolCall] {
		t.Error("expected a TOOL_CALL log in the stream")
	}

	persisted, _ := store.List(s.ID())

	if len(persisted) < 2 {
		t.Errorf("store persisted %d messages, want more", len(persisted))
	}
}
