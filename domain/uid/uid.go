package uid

import (
	"crypto/rand"
	"crypto/sha256"
	"fmt"
	"strings"
)

// New generates a random UUID v4 string.
func New() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic("uid: crypto/rand unavailable: " + err.Error())
	}
	b[6] = (b[6] & 0x0f) | 0x40 // version 4
	b[8] = (b[8] & 0x3f) | 0x80 // variant bits
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

// masterdataNamespace is a fixed UUIDv4 namespace used for deterministic
// aggregate-ID derivation from idempotency keys (R1-04). It ensures the same
// idempotency key produces the same aggregate UUID across retries, so that
// concurrency.NoStream() rejects duplicate aggregate creation even when the
// idempotency record was not yet persisted.
var masterdataNamespace = [16]byte{
	0x8e, 0x2b, 0x1a, 0x05, 0xc4, 0x7d, 0x4f, 0x9a,
	0xa1, 0x6e, 0xe8, 0x3c, 0x55, 0x0d, 0x39, 0xf7,
}

// FromKey derives a deterministic UUID v5 string from the given key. The same
// key always produces the same UUID, so register-command retries that carry the
// same Idempotency-Key but no client-supplied id reuse the same KurrentDB stream
// id. NoStream() then rejects the duplicate append, closing the partial-failure
// window where the idempotency record was not yet persisted (R1-04).
func FromKey(key string) string {
	h := sha256.Sum256(append(masterdataNamespace[:], []byte(key)...))
	var b [16]byte
	copy(b[:], h[:16])
	b[6] = (b[6] & 0x0f) | 0x50 // version 5
	b[8] = (b[8] & 0x3f) | 0x80 // variant bits
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

// IsValid reports whether s is a well-formed UUID string
// (xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx, lowercase or uppercase hex).
func IsValid(s string) bool {
	s = strings.ToLower(s)
	if len(s) != 36 {
		return false
	}
	for i, ch := range s {
		switch i {
		case 8, 13, 18, 23:
			if ch != '-' {
				return false
			}
		default:
			if !isHex(byte(ch)) {
				return false
			}
		}
	}
	return true
}

func isHex(c byte) bool {
	return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')
}
