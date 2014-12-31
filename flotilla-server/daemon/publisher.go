package daemon

import (
	"encoding/binary"
	"errors"
	"log"
	"sync"
	"time"
)

type publisher struct {
	peer
	id          int
	numMessages int
	messageSize int64
	results     *result
	mu          sync.Mutex
}

func (p *publisher) start() {
	p.Setup()
	defer p.Done()

	var (
		send    = p.Send()
		errors  = p.Errors()
		message = make([]byte, p.messageSize)
		start   = time.Now().UnixNano()
	)

	for i := 0; i < p.numMessages; i++ {
		binary.PutVarint(message, time.Now().UnixNano())
		select {
		case send <- message:
			continue
		case err := <-errors:
			// TODO: If a publish fails, a subscriber will probably deadlock.
			// The best option is probably to signal back to the client that
			// a publisher failed so it can orchestrate a shutdown.
			log.Printf("Failed to send message: %s", err.Error())
			p.mu.Lock()
			p.results = &result{Err: err.Error()}
			p.mu.Unlock()
			return
		}
	}
	stop := time.Now().UnixNano()
	ms := float32(stop-start) / 1000000
	p.mu.Lock()
	p.results = &result{
		Duration:   ms,
		Throughput: 1000 * float32(p.numMessages) / ms,
	}
	p.mu.Unlock()
	log.Println("Publisher completed")
}

func (p *publisher) getResults() (*result, error) {
	p.mu.Lock()
	r := p.results
	p.mu.Unlock()
	if r == nil {
		return nil, errors.New("Results not ready")
	}
	return r, nil
}
