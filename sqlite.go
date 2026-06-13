package session

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/filipgorny/agent/stream"
	_ "modernc.org/sqlite"
)

// SQLite persists session messages to a SQLite database (pure-Go modernc driver).
type SQLite struct {
	db *sql.DB
}

const sqliteSchema = `
CREATE TABLE IF NOT EXISTS session_messages (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id TEXT NOT NULL,
    type       TEXT NOT NULL,
    subtype    TEXT,
    payload    TEXT,
    created_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS session_messages_sid ON session_messages(session_id);
`

// NewSQLite opens (or creates) a session store at path. Empty path = in-memory.
func NewSQLite(path string) (*SQLite, error) {
	if path == "" {
		path = ":memory:"
	}

	db, err := sql.Open("sqlite", path)

	if err != nil {
		return nil, fmt.Errorf("session: open sqlite: %w", err)
	}

	db.SetMaxOpenConns(1)

	if _, err := db.Exec(sqliteSchema); err != nil {
		db.Close()

		return nil, fmt.Errorf("session: init schema: %w", err)
	}

	return &SQLite{db: db}, nil
}

// Close releases the underlying database.
func (s *SQLite) Close() error {
	return s.db.Close()
}

func (s *SQLite) Append(sessionID string, m stream.Record) error {
	var payload string

	if m.Payload != nil {
		b, err := json.Marshal(m.Payload)

		if err != nil {
			return fmt.Errorf("session: marshal payload: %w", err)
		}

		payload = string(b)
	}

	_, err := s.db.Exec(
		`INSERT INTO session_messages(session_id, type, subtype, payload, created_at) VALUES(?, ?, ?, ?, ?)`,
		sessionID, m.Type, m.Subtype, payload, m.CreatedAt.UnixNano())

	if err != nil {
		return fmt.Errorf("session: append: %w", err)
	}

	return nil
}

func (s *SQLite) List(sessionID string) ([]stream.Record, error) {
	rows, err := s.db.Query(
		`SELECT type, subtype, payload, created_at FROM session_messages WHERE session_id = ? ORDER BY id`,
		sessionID)

	if err != nil {
		return nil, fmt.Errorf("session: list: %w", err)
	}

	defer rows.Close()

	var out []stream.Record

	for rows.Next() {
		var (
			typ     string
			subtype sql.NullString
			payload sql.NullString
			created int64
		)

		if err := rows.Scan(&typ, &subtype, &payload, &created); err != nil {
			return nil, fmt.Errorf("session: scan: %w", err)
		}

		m := stream.Record{Type: typ, Subtype: subtype.String, CreatedAt: time.Unix(0, created)}

		if payload.Valid && payload.String != "" {
			_ = json.Unmarshal([]byte(payload.String), &m.Payload)
		}

		out = append(out, m)
	}

	return out, rows.Err()
}
