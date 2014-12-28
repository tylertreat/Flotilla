package main

import (
	"os"
	"os/signal"
	"syscall"
)

// setupStopSignals sets up UNIX-specific signals for terminating
// the program
func setupStopSignals(signalChannel chan os.Signal) {
	// TODO: not sure whether SIGHUP should be used here, only if not in
	// daemon mode
	signal.Notify(signalChannel, syscall.SIGHUP)

	signal.Notify(signalChannel, syscall.SIGTERM)
}
