package daemon

// Daemon is a server process which runs on a specified port. It handles
// requests from the flotilla client.
type Daemon interface {
	// Start will start the Daemon on the given port so that it can begin
	// processing requests. It returns an error if it could not be successfully
	// started.
	Start(int) error
}
