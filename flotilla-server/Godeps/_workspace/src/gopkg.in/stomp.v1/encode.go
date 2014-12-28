package stomp

import (
	"strings"
)

// Encodes a header value using STOMP value encoding
// TODO: replace with more efficient version.
func encodeValue(s string) string {
	s = strings.Replace(s, "\\", "\\\\", -1)
	s = strings.Replace(s, "\r", "\\r", -1)
	s = strings.Replace(s, "\n", "\\n", -1)
	s = strings.Replace(s, ":", "\\c", -1)
	return s
}

// Unencodes a header value using STOMP value encoding
// TODO: replace with more efficient version.
func unencodeValue(s string) string {
	s = strings.Replace(s, "\\r", "\r", -1)
	s = strings.Replace(s, "\\n", "\n", -1)
	s = strings.Replace(s, "\\c", ":", -1)
	s = strings.Replace(s, "\\\\", "\\", -1)
	return s
}
