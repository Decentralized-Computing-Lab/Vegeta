package sha256

import "crypto/sha256"

// ComputeHash computes hash of the given raw message
func ComputeHash(raw []byte) ([]byte, error) {
	hash := sha256.New()
	hash.Write(raw)
	return hash.Sum(nil), nil
}
