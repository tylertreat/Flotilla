package broker

import "crypto/rand"

// GenerateName returns a randomly generated, 32-byte alphanumeric name. This
// is useful for cases where multiple clients which need to subscribe to a
// broker topic.
func GenerateName() string {
	alphanum := "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
	bytes := make([]byte, 32)
	rand.Read(bytes)
	for i, b := range bytes {
		bytes[i] = alphanum[b%byte(len(alphanum))]
	}
	return string(bytes)
}
