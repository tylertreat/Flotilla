package broker

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/gdamore/mangos"
	"github.com/gdamore/mangos/protocol/req"
	"github.com/gdamore/mangos/transport/tcp"
)

type operation string
type daemon string

const (
	start        operation = "start"
	stop         operation = "stop"
	sub          operation = "subscribers"
	pub          operation = "publishers"
	run          operation = "run"
	results      operation = "results"
	teardown     operation = "teardown"
	resultsSleep           = time.Second
)

type request struct {
	Operation   operation `json:"operation"`
	Broker      string    `json:"broker"`
	Port        string    `json:"port"`
	NumMessages int       `json:"num_messages"`
	MessageSize int64     `json:"message_size"`
	Count       int       `json:"count"`
	Host        string    `json:"host"`
}

type response struct {
	Success    bool        `json:"success"`
	Message    string      `json:"message"`
	Result     interface{} `json:"result"`
	PubResults []*Result   `json:"pub_results,omitempty"`
	SubResults []*Result   `json:"sub_results,omitempty"`
}

type Benchmark struct {
	BrokerdHost string
	BrokerName  string
	BrokerHost  string
	BrokerPort  string
	PeerHosts   []string
	NumMessages int
	MessageSize int64
	Publishers  int
	Subscribers int
}

type Result struct {
	Duration   float32        `json:"duration,omitempty"`
	Throughput float32        `json:"throughput,omitempty"`
	Latency    LatencyResults `json:"latency,omitempty"`
	Err        string         `json:"error"`
}

type ResultContainer struct {
	Peer              string
	PublisherResults  []*Result
	SubscriberResults []*Result
}

type LatencyResults struct {
	Min    int64   `json:"min"`
	Q1     int64   `json:"q1"`
	Q2     int64   `json:"q2"`
	Q3     int64   `json:"q3"`
	Max    int64   `json:"max"`
	Mean   float64 `json:"mean"`
	StdDev float64 `json:"std_dev"`
}

type Client struct {
	brokerd   mangos.Socket
	peerd     map[string]mangos.Socket
	Benchmark *Benchmark
}

func NewClient(b *Benchmark) (*Client, error) {
	brokerd, err := req.NewSocket()
	if err != nil {
		return nil, err
	}

	brokerd.AddTransport(tcp.NewTransport())

	if err := brokerd.Dial(fmt.Sprintf("tcp://%s", b.BrokerdHost)); err != nil {
		return nil, err
	}

	peerd := make(map[string]mangos.Socket, len(b.PeerHosts))
	for _, peer := range b.PeerHosts {
		s, err := req.NewSocket()
		if err != nil {
			return nil, err
		}

		s.AddTransport(tcp.NewTransport())

		if err := s.Dial(fmt.Sprintf("tcp://%s", peer)); err != nil {
			return nil, err
		}

		peerd[peer] = s
	}

	return &Client{
		brokerd:   brokerd,
		peerd:     peerd,
		Benchmark: b,
	}, nil
}

func (c *Client) StartBroker() error {
	resp, err := sendRequest(c.brokerd, request{
		Operation: start,
		Broker:    c.Benchmark.BrokerName,
		Host:      c.Benchmark.BrokerHost,
		Port:      c.Benchmark.BrokerPort,
	})

	if err != nil {
		return err
	}

	if !resp.Success {
		return errors.New(resp.Message)
	}

	return nil
}

func (c *Client) StopBroker() error {
	resp, err := sendRequest(c.brokerd, request{Operation: stop})
	if err != nil {
		return err
	}

	if !resp.Success {
		return errors.New(resp.Message)
	}

	return nil
}

func (c *Client) StartSubscribers() error {
	for _, peerd := range c.peerd {
		resp, err := sendRequest(peerd, request{
			Operation:   sub,
			Broker:      c.Benchmark.BrokerName,
			Host:        fmt.Sprintf("%s:%s", c.Benchmark.BrokerHost, c.Benchmark.BrokerPort),
			Count:       c.Benchmark.Subscribers,
			NumMessages: c.Benchmark.NumMessages,
			MessageSize: c.Benchmark.MessageSize,
		})

		if err != nil {
			return err
		}

		if !resp.Success {
			return errors.New(resp.Message)
		}
	}
	return nil
}

func (c *Client) StartPublishers() error {
	for _, peerd := range c.peerd {
		resp, err := sendRequest(peerd, request{
			Operation:   pub,
			Broker:      c.Benchmark.BrokerName,
			Host:        fmt.Sprintf("%s:%s", c.Benchmark.BrokerHost, c.Benchmark.BrokerPort),
			Count:       c.Benchmark.Publishers,
			NumMessages: c.Benchmark.NumMessages,
			MessageSize: c.Benchmark.MessageSize,
		})

		if err != nil {
			return err
		}

		if !resp.Success {
			return errors.New(resp.Message)
		}
	}
	return nil
}

func (c *Client) RunBenchmark() error {
	for _, peerd := range c.peerd {
		resp, err := sendRequest(peerd, request{Operation: run})
		if err != nil {
			return err
		}

		if !resp.Success {
			return errors.New(resp.Message)
		}
	}
	return nil
}

func (c *Client) CollectResults() <-chan []*ResultContainer {
	resultsChan := make(chan []*ResultContainer, 1)

	go func(chan<- []*ResultContainer) {
		results := make([]*ResultContainer, 0, len(c.peerd))
		subResults := make(chan *ResultContainer, len(c.peerd))
		complete := 0

		for host, peerd := range c.peerd {
			go collectResultsFromPeer(host, peerd, subResults)
		}

		for {
			select {
			case subResult := <-subResults:
				results = append(results, subResult)
				complete++
			}

			if complete == len(c.peerd) {
				resultsChan <- results
				return
			}
		}
	}(resultsChan)

	return resultsChan
}

func (c *Client) Teardown() error {
	for _, peerd := range c.peerd {
		_, err := sendRequest(peerd, request{Operation: teardown})
		if err != nil {
			return err
		}
	}
	return nil
}

func sendRequest(s mangos.Socket, request request) (*response, error) {
	requestJSON, err := json.Marshal(request)
	if err != nil {
		// This is not recoverable.
		panic(err)
	}

	if err := s.Send(requestJSON); err != nil {
		return nil, err
	}

	rep, err := s.Recv()
	if err != nil {
		return nil, err
	}

	var resp response
	if err := json.Unmarshal(rep, &resp); err != nil {
		return nil, err
	}

	return &resp, nil
}

func collectResultsFromPeer(host string, peerd mangos.Socket, subResults chan *ResultContainer) {
	for {
		resp, err := sendRequest(peerd, request{Operation: results})
		if err != nil {
			fmt.Println("Failed to collect results from peer:", err.Error())
			subResults <- nil
		}

		if !resp.Success {
			fmt.Printf("Failed to collect results from peer: %s", resp.Message)
			subResults <- nil
		}

		if resp.Message == "Results not ready" {
			time.Sleep(resultsSleep)
			continue
		}

		subResults <- &ResultContainer{
			Peer:              host,
			PublisherResults:  resp.PubResults,
			SubscriberResults: resp.SubResults,
		}
		break
	}
}
