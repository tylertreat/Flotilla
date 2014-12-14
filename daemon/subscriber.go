package daemon

import (
	"encoding/binary"
	"fmt"
	"time"

	"github.com/codahale/hdrhistogram"
)

type subscriber struct {
	peer
	id          int
	numMessages int
	messageSize int64
	test        test
	hasStarted  bool
	started     int64
	stopped     int64
	counter     int
	results     chan *result
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
	switch s.test {
	case throughput:
		s.testThroughput()
	case latency:
		s.testLatency()
	default:
		panic(fmt.Sprintf("Invalid test: %s", s.test))
	}
}

func (s *subscriber) testThroughput() {
	for {
		s.Recv()

		if !s.hasStarted {
			s.hasStarted = true
			s.started = time.Now().UnixNano()
		}

		s.counter++
		if s.counter == s.numMessages {
			s.stopped = time.Now().UnixNano()
			ms := float32(s.stopped-s.started) / 1000000.0
			s.results <- &result{
				Duration:   ms,
				Throughput: 1000 * float32(s.numMessages) / ms,
			}
			return
		}
	}
}

func (s *subscriber) testLatency() {
	latencies := hdrhistogram.New(0, 60000, 5)
	for {
		message := s.Recv()
		now := time.Now().UnixNano()
		then, _ := binary.Varint(message)

		latencies.RecordValue((now - then) / 1000000)

		s.counter++
		if s.counter == s.numMessages {
			s.results <- &result{
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
			return
		}
	}
}
