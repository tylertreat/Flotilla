package client

import (
	"time"
)

// Convert a time.Duration to milliseconds in an integer.
// Returns the duration in milliseconds, or max if the
// duration is greater than max milliseconds.
func asMilliseconds(d time.Duration, max int) int {
	if max < 0 {
		max = 0
	}
	max64 := int64(max)
	msec64 := int64(d / time.Millisecond)
	if msec64 > max64 {
		msec64 = max64
	}
	msec := int(msec64)
	return msec
}
