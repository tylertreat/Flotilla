package daemon

import (
	"encoding/binary"
	"errors"
	"log"
	"sync"
	"time"

	"github.com/codahale/hdrhistogram"
)

const (
	maxRecordableLatencyMS = 300000
	sigFigs                = 5
)

type subscriber struct {
	peer
	id          int
	numMessages int
	messageSize int64
	hasStarted  bool
	started     int64
	stopped     int64
	counter     int
	results     *result
	mu          sync.Mutex
}

type latencyResults struct {
	Min    int64   `json:"min"`
	Q1     int64   `json:"q1"`
	Q2     int64   `json:"q2"`
	Q3     int64   `json:"q3"`
	Max    int64   `json:"max"`
	Mean   float64 `json:"mean"`
	StdDev float64 `json:"std_dev"`
}

func (s *subscriber) start() {
	latencies := hdrhistogram.New(0, maxRecordableLatencyMS, sigFigs)
	for {
		message, err := s.Recv()
		now := time.Now().UnixNano()
		if err != nil {
			log.Printf("Subscriber error: %s", err.Error())
			s.mu.Lock()
			s.results = &result{Err: err.Error()}
			s.mu.Unlock()
			return
		}

		then, _ := binary.Varint(message)
		latencies.RecordValue((now - then) / 1000000)

		if !s.hasStarted {
			s.hasStarted = true
			s.started = time.Now().UnixNano()
		}

		s.counter++
		if s.counter == s.numMessages {
			s.stopped = time.Now().UnixNano()
			durationMS := float32(s.stopped-s.started) / 1000000.0
			s.mu.Lock()
			s.results = &result{
				Duration:   durationMS,
				Throughput: 1000 * float32(s.numMessages) / durationMS,
				Latency: &latencyResults{
					Min:    latencies.Min(),
					Q1:     latencies.ValueAtQuantile(25),
					Q2:     latencies.ValueAtQuantile(50),
					Q3:     latencies.ValueAtQuantile(75),
					Max:    latencies.Max(),
					Mean:   latencies.Mean(),
					StdDev: latencies.StdDev(),
				},
			}
			s.mu.Unlock()
			log.Println("Subscriber completed")
			return
		}
	}
}

func (s *subscriber) getResults() (*result, error) {
	s.mu.Lock()
	r := s.results
	s.mu.Unlock()
	if r == nil {
		return nil, errors.New("Results not ready")
	}
	return r, nil
}
