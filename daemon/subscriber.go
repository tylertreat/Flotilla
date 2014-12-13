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
	complete    chan<- bool
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
			fmt.Printf("Received %d messages in %f ms\n", s.numMessages, ms)
			fmt.Printf("Received %f per second\n", 1000*float32(s.numMessages)/ms)
			s.complete <- true
			return
		}
	}
}

func (s *subscriber) testLatency() {
	latencies := hdrhistogram.New(0, 60000, 32)
	for {
		message := s.Recv()
		now := time.Now().UnixNano()
		then, _ := binary.Varint(message)

		latencies.RecordValue((now - then) / 1000000)

		s.counter++
		if s.counter == s.numMessages {
			fmt.Printf("Received %d messages\n", s.numMessages)
			fmt.Printf("Min latency: %d ms\n", latencies.Min())
			fmt.Printf("Q1 latency: %d ms\n", latencies.ValueAtQuantile(25))
			fmt.Printf("Q2 latency: %d ms\n", latencies.ValueAtQuantile(50))
			fmt.Printf("Q3 latency: %d ms\n", latencies.ValueAtQuantile(75))
			fmt.Printf("Max latency: %d ms\n", latencies.Max())
			fmt.Printf("Mean latency: %d ms\n", latencies.Mean())
			fmt.Printf("Standard deviation: %d ms\n", latencies.StdDev())
			s.complete <- true
			return
		}
	}
}
