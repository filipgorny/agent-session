package session

import (
	"crypto/rand"
	"encoding/hex"
)

// newID returns a random hex session id.
func newID() string {
	var b [12]byte

	if _, err := rand.Read(b[:]); err != nil {
		return "session"
	}

	return hex.EncodeToString(b[:])
}
