package queue

// Queue manager.
type Manager struct {
	qstore Storage // handles queue storage
	queues map[string]*Queue
}

// Create a queue manager with the specified queue storage mechanism
func NewManager(qstore Storage) *Manager {
	qm := &Manager{qstore: qstore, queues: make(map[string]*Queue)}
	return qm
}

// Finds the queue for the given destination, and creates it if necessary.
func (qm *Manager) Find(destination string) *Queue {
	q, ok := qm.queues[destination]
	if !ok {
		q = newQueue(destination, qm.qstore)
		qm.queues[destination] = q
	}
	return q
}
