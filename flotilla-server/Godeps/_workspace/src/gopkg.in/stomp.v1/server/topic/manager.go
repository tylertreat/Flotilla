package topic

// Manager is a struct responsible for finding topics. Topics are
// not created by the package user, rather they are created on demand
// by the topic manager.
type Manager struct {
	topics map[string]*Topic
}

// NewManager creates a new topic manager.
func NewManager() *Manager {
	tm := &Manager{topics: make(map[string]*Topic)}
	return tm
}

// Finds the topic for the given destination, and creates it if necessary.
func (tm *Manager) Find(destination string) *Topic {
	t, ok := tm.topics[destination]
	if !ok {
		t = newTopic(destination)
		tm.topics[destination] = t
	}
	return t
}
