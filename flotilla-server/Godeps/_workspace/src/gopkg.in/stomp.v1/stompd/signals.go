package main

import (
	"os"
	"os/signal"
)

// newStopChannel creates a channel for receiving signals
// for stopping the program. Calls an os-dependent setupStopSignals
// function.
func newStopChannel() chan os.Signal {
	c := make(chan os.Signal, 2)
	signal.Notify(c, os.Interrupt)

	// os dependent between windows and unix
	setupStopSignals(c)

	return c
}
