package main

import (
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/olekukonko/tablewriter"
	"github.com/tylertreat/flotilla/client/broker"
)

const (
	defaultBrokerPort   = "5000"
	defaultNumMessages  = 500000
	defaultMessageSize  = 1000
	defaultNumProducers = 1
	defaultNumConsumers = 1
)

var brokers = []string{
	"beanstalkd",
	"nats",
	"kafka",
	"kestrel",
	"activemq",
	"rabbitmq",
	"nsq",
	"pubsub",
}

func main() {
	var (
		brokerName  = flag.String("broker", brokers[0], brokerList())
		brokerPort  = flag.String("broker-port", defaultBrokerPort, "host machine broker port")
		brokerHost  = flag.String("broker-host", "localhost", "host machine to run broker")
		brokerdHost = flag.String("host", "localhost:9000", "machine running broker daemon")
		peerHosts   = flag.String("peer-hosts", "localhost:9000", "comma-separated list of machines to run peers")
		test        = flag.String("test", string(broker.Throughput), "[throughput|latency]")
		producers   = flag.Int("producers", defaultNumProducers, "number of producers per host")
		consumers   = flag.Int("consumers", defaultNumConsumers, "number of consumers per host")
		numMessages = flag.Int("num-messages", defaultNumMessages, "number of messages to send from each producer")
		messageSize = flag.Int64("message-size", defaultMessageSize, "size of each message in bytes")
	)
	flag.Parse()

	peers := strings.Split(*peerHosts, ",")

	client, err := broker.NewClient(&broker.Benchmark{
		BrokerdHost: *brokerdHost,
		BrokerName:  *brokerName,
		BrokerHost:  *brokerHost,
		BrokerPort:  *brokerPort,
		PeerHosts:   peers,
		NumMessages: *numMessages,
		MessageSize: *messageSize,
		Publishers:  *producers,
		Subscribers: *consumers,
		Test:        broker.Test(*test),
	})
	if err != nil {
		fmt.Println("Failed to connect to flotilla:", err)
		os.Exit(1)
	}

	runBenchmark(client)
}

func runBenchmark(client *broker.Client) {
	fmt.Println("Starting broker - if the image hasn't been pulled yet, this may take a while...")
	if err := client.StartBroker(); err != nil {
		fmt.Println("Failed to start broker:", err)
		os.Exit(1)
	}

	// Allow some time for broker startup.
	time.Sleep(7 * time.Second)

	fmt.Println("Preparing producers")
	if err := client.StartPublishers(); err != nil {
		fmt.Println("Failed to start producers:", err)
		stopBroker(client)
		os.Exit(1)
	}

	fmt.Println("Preparing subscribers")
	if err := client.StartSubscribers(); err != nil {
		fmt.Println("Failed to start subscribers:", err)
		teardownPeers(client)
		stopBroker(client)
		os.Exit(1)
	}

	// TODO: clean up subscribers

	fmt.Println("Running benchmark")
	if err := client.RunBenchmark(); err != nil {
		fmt.Println("Failed to run benchmark:", err)
		teardownPeers(client)
		stopBroker(client)
		os.Exit(1)
	}

	results := <-client.CollectResults()
	printResults(results, client)

	teardownPeers(client)
	stopBroker(client)
}

func printResults(results []*broker.ResultContainer, client *broker.Client) {
	publisherData := [][]string{}
	i := 1
	for p, peerResults := range results {
		for _, result := range peerResults.PublisherResults {
			publisherData = append(publisherData, []string{
				strconv.Itoa(i),
				client.Benchmark.PeerHosts[p],
				strconv.FormatBool(result.Err != ""),
				strconv.FormatFloat(float64(result.Duration), 'f', 3, 32),
				strconv.FormatFloat(float64(result.Throughput), 'f', 3, 32),
			})
			i++
		}
	}
	printTable([]string{
		"Producer",
		"Node",
		"Error",
		"Duration (ms)",
		"Throughput (msg/sec)",
	}, publisherData)

	switch client.Benchmark.Test {
	case broker.Throughput:
		subscriberData := [][]string{}
		i = 1
		for p, peerResults := range results {
			for _, result := range peerResults.SubscriberResults {
				subscriberData = append(subscriberData, []string{
					strconv.Itoa(i),
					client.Benchmark.PeerHosts[p],
					strconv.FormatBool(result.Err != ""),
					strconv.FormatFloat(float64(result.Duration), 'f', 3, 32),
					strconv.FormatFloat(float64(result.Throughput), 'f', 3, 32),
				})
				i++
			}
		}
		printTable([]string{
			"Consumer",
			"Node",
			"Error",
			"Duration (ms)",
			"Throughput (msg/sec)",
		}, subscriberData)
	case broker.Latency:
		latencyData := [][]string{}
		i = 1
		for p, peerResults := range results {
			for _, result := range peerResults.SubscriberResults {
				latencyData = append(latencyData, []string{
					strconv.Itoa(i),
					client.Benchmark.PeerHosts[p],
					strconv.FormatBool(result.Err != ""),
					strconv.FormatInt(result.Latency.Min, 10),
					strconv.FormatInt(result.Latency.Q1, 10),
					strconv.FormatInt(result.Latency.Q2, 10),
					strconv.FormatInt(result.Latency.Q3, 10),
					strconv.FormatInt(result.Latency.Max, 10),
					strconv.FormatFloat(result.Latency.Mean, 'f', 3, 64),
					strconv.FormatInt(result.Latency.Q3-result.Latency.Q1, 10),
					strconv.FormatFloat(result.Latency.StdDev, 'f', 3, 64),
				})
				i++
			}
		}
		printTable([]string{
			"Consumer",
			"Node",
			"Error",
			"Min",
			"Q1",
			"Q2",
			"Q3",
			"Max",
			"Mean",
			"IQR",
			"Std Dev",
		}, latencyData)
	default:
		panic(fmt.Sprintf("Invalid test: %s", client.Benchmark.Test))
	}
}

func printTable(headers []string, data [][]string) {
	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader(headers)
	for _, row := range data {
		table.Append(row)
	}
	table.SetAlignment(tablewriter.ALIGN_LEFT)
	table.Render()
}

func stopBroker(c *broker.Client) {
	if err := c.StopBroker(); err != nil {
		fmt.Println("Failed to stop broker:", err)
	}
}

func teardownPeers(c *broker.Client) {
	if err := c.Teardown(); err != nil {
		fmt.Println("Failed to teardown peers:", err)
	}
}

func brokerList() string {
	brokerList := "["
	for i, broker := range brokers {
		brokerList = brokerList + broker
		if i != len(brokers)-1 {
			brokerList = brokerList + "|"
		}
	}
	brokerList = brokerList + "]"
	return brokerList
}
