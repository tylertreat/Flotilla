package main

import (
	"os"
)

func signals(signalChannel chan os.Signal) {
	// Windows has no other signals other than os.Interrupt

	// TODO: What might be good here is to simulate a signal
	// if running as a Windows service and the stop request is
	// received. Not sure how to do this though.
}
