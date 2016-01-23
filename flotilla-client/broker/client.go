package broker

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/go-mangos/mangos"
	"github.com/go-mangos/mangos/protocol/req"
	"github.com/go-mangos/mangos/transport/tcp"
)

type operation string
type daemon string

const (
	minNumMessages             = 100
	minMessageSize             = 9
	start            operation = "start"
	stop             operation = "stop"
	sub              operation = "subscribers"
	pub              operation = "publishers"
	run              operation = "run"
	results          operation = "results"
	teardown         operation = "teardown"
	resultsSleep               = time.Second
	sendRecvDeadline           = 5 * time.Second
)

type request struct {
	Operation   operation `json:"operation"`
	Broker      string    `json:"broker"`
	Port        string    `json:"port"`
	NumMessages uint      `json:"num_messages"`
	MessageSize uint64    `json:"message_size"`
	Count       uint      `json:"count"`
	Host        string    `json:"host"`
}

type response struct {
	Success    bool        `json:"success"`
	Message    string      `json:"message"`
	Result     interface{} `json:"result"`
	PubResults []*Result   `json:"pub_results,omitempty"`
	SubResults []*Result   `json:"sub_results,omitempty"`
}

// Benchmark contains configuration settings for broker tests.
type Benchmark struct {
	BrokerdHost   string
	BrokerName    string
	BrokerHost    string
	BrokerPort    string
	PeerHosts     []string
	NumMessages   uint
	MessageSize   uint64
	Publishers    uint
	Subscribers   uint
	StartupSleep  uint
	DaemonTimeout uint
}

func (b *Benchmark) validate() error {
	if b.BrokerdHost == "" {
		return errors.New("Invalid broker daemon host")
	}

	if b.BrokerName == "" {
		return errors.New("Invalid broker name")
	}

	if b.BrokerHost == "" {
		return errors.New("Invalid broker host")
	}

	if b.BrokerPort == "" {
		return errors.New("Invalid broker port")
	}

	if len(b.PeerHosts) == 0 {
		return errors.New("Must provide at least one peer host")
	}

	if b.NumMessages < minNumMessages {
		return fmt.Errorf("Number of messages must be at least %d", minNumMessages)
	}

	if b.MessageSize < minMessageSize {
		return fmt.Errorf("Message size must be at least %d", minMessageSize)
	}

	if b.Publishers <= 0 {
		return errors.New("Number of producers must be greater than zero")
	}

	if b.Subscribers <= 0 {
		return errors.New("Number of consumers must be greater than zero")
	}

	return nil
}

// Result contains test result data for a single peer.
type Result struct {
	Duration   float32        `json:"duration,omitempty"`
	Throughput float32        `json:"throughput,omitempty"`
	Latency    LatencyResults `json:"latency,omitempty"`
	Err        string         `json:"error"`
}

// ResultContainer contains the Results for a single node.
type ResultContainer struct {
	Peer              string
	PublisherResults  []*Result
	SubscriberResults []*Result
}

// LatencyResults contains the latency result data for a single peer.
type LatencyResults struct {
	Min    int64   `json:"min"`
	Q1     int64   `json:"q1"`
	Q2     int64   `json:"q2"`
	Q3     int64   `json:"q3"`
	Max    int64   `json:"max"`
	Mean   float64 `json:"mean"`
	StdDev float64 `json:"std_dev"`
}

// Client provides an API for interacting with Flotilla.
type Client struct {
	brokerd   mangos.Socket
	peerd     map[string]mangos.Socket
	Benchmark *Benchmark
}

// NewClient creates and returns a new Client from the provided Benchmark
// configuration. It returns an error if the Benchmark is not valid or it
// can't communicate with any of the specified peers.
func NewClient(b *Benchmark) (*Client, error) {
	if err := b.validate(); err != nil {
		return nil, err
	}

	brokerd, err := req.NewSocket()
	if err != nil {
		return nil, err
	}

	brokerd.AddTransport(tcp.NewTransport())
	brokerd.SetOption(mangos.OptionSendDeadline, time.Duration(b.DaemonTimeout)*time.Second)
	brokerd.SetOption(mangos.OptionRecvDeadline, time.Duration(b.DaemonTimeout)*time.Second)

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
		s.SetOption(mangos.OptionSendDeadline, time.Duration(b.DaemonTimeout)*time.Second)
		s.SetOption(mangos.OptionRecvDeadline, time.Duration(b.DaemonTimeout)*time.Second)

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

// Start begins the broker test.
func (c *Client) Start() ([]*ResultContainer, error) {
	fmt.Println("Starting broker - if the image hasn't been pulled yet, this may take a while...")
	if err := c.startBroker(); err != nil {
		return nil, fmt.Errorf("Failed to start broker: %s", err.Error())
	}

	// Allow some time for broker startup.
	time.Sleep(time.Duration(c.Benchmark.StartupSleep) * time.Second)

	fmt.Println("Preparing producers")
	if err := c.startPublishers(); err != nil {
		return nil, fmt.Errorf("Failed to start producers: %s", err.Error())
	}

	fmt.Println("Preparing consumers")
	if err := c.startSubscribers(); err != nil {
		return nil, fmt.Errorf("Failed to start consumers %s:", err.Error())
	}

	fmt.Println("Running benchmark")
	if err := c.runBenchmark(); err != nil {
		return nil, fmt.Errorf("Failed to run benchmark %s:", err.Error())
	}

	return <-c.collectResults(), nil
}

func (c *Client) startBroker() error {
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

func (c *Client) startSubscribers() error {
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

func (c *Client) startPublishers() error {
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

func (c *Client) runBenchmark() error {
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

func (c *Client) collectResults() <-chan []*ResultContainer {
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
			case subResult, ok := <-subResults:
				if !ok {
					return
				}
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

// Teardown performs any necessary cleanup logic, including stopping the
// broker and tearing down peers.
func (c *Client) Teardown() {
	fmt.Println("Tearing down peers")
	for _, peerd := range c.peerd {
		_, err := sendRequest(peerd, request{Operation: teardown})
		if err != nil {
			fmt.Printf("Failed to teardown peer: %s\n", err.Error())
		}
	}

	fmt.Println("Stopping broker")
	if err := c.stopBroker(); err != nil {
		fmt.Printf("Failed to stop broker: %s\n", err.Error())
	}
}

func (c *Client) stopBroker() error {
	resp, err := sendRequest(c.brokerd, request{Operation: stop})
	if err != nil {
		return err
	}

	if !resp.Success {
		return errors.New(resp.Message)
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
			close(subResults)
			return
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
