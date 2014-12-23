package main

import (
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/olekukonko/tablewriter"
	"github.com/tylertreat/flotilla/flotilla-client/broker"
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
		producers   = flag.Uint("producers", defaultNumProducers, "number of producers per host")
		consumers   = flag.Uint("consumers", defaultNumConsumers, "number of consumers per host")
		numMessages = flag.Uint("num-messages", defaultNumMessages, "number of messages to send from each producer")
		messageSize = flag.Uint64("message-size", defaultMessageSize, "size of each message in bytes")
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

	fmt.Println("Preparing consumers")
	if err := client.StartSubscribers(); err != nil {
		fmt.Println("Failed to start consumers:", err)
		teardownPeers(client)
		stopBroker(client)
		os.Exit(1)
	}

	fmt.Println("Running benchmark")
	if err := client.RunBenchmark(); err != nil {
		fmt.Println("Failed to run benchmark:", err)
		teardownPeers(client)
		stopBroker(client)
		os.Exit(1)
	}

	results := <-client.CollectResults()
	printSummary(client.Benchmark)
	printResults(results)

	teardownPeers(client)
	stopBroker(client)
}

func printSummary(benchmark *broker.Benchmark) {
	brokerHost := strings.Split(benchmark.BrokerdHost, ":")[0] + ":" + benchmark.BrokerPort
	msgSent := int(benchmark.NumMessages) * len(benchmark.PeerHosts) * int(benchmark.Publishers)
	msgRecv := int(benchmark.NumMessages) * len(benchmark.PeerHosts) * int(benchmark.Subscribers)
	dataSentKB := (msgSent * int(benchmark.MessageSize)) / 1000
	dataRecvKB := (msgRecv * int(benchmark.MessageSize)) / 1000
	fmt.Println("\nTEST SUMMARY\n")
	fmt.Printf("Broker:             %s (%s)\n", benchmark.BrokerName, brokerHost)
	fmt.Printf("Nodes:              %s\n", benchmark.PeerHosts)
	fmt.Printf("Producers per node: %d\n", benchmark.Publishers)
	fmt.Printf("Consumers per node: %d\n", benchmark.Subscribers)
	fmt.Printf("Messages produced:  %d\n", msgSent)
	fmt.Printf("Messages consumed:  %d\n", msgRecv)
	fmt.Printf("Bytes per message:  %d\n", benchmark.MessageSize)
	fmt.Printf("Data produced (kb): %d\n", dataSentKB)
	fmt.Printf("Data consumed (kb): %d\n", dataRecvKB)
	fmt.Println("")
}

func printResults(results []*broker.ResultContainer) {
	producerData := [][]string{}
	i := 1
	for _, peerResults := range results {
		for _, result := range peerResults.PublisherResults {
			producerData = append(producerData, []string{
				strconv.Itoa(i),
				peerResults.Peer,
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
		"Duration",
		"Throughput (msg/sec)",
	}, producerData)

	consumerData := [][]string{}
	i = 1
	for _, peerResults := range results {
		for _, result := range peerResults.SubscriberResults {
			consumerData = append(consumerData, []string{
				strconv.Itoa(i),
				peerResults.Peer,
				strconv.FormatBool(result.Err != ""),
				strconv.FormatFloat(float64(result.Duration), 'f', 3, 32),
				strconv.FormatFloat(float64(result.Throughput), 'f', 3, 32),
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
		"Duration",
		"Throughput (msg/sec)",
		"Min",
		"Q1",
		"Q2",
		"Q3",
		"Max",
		"Mean",
		"IQR",
		"Std Dev",
	}, consumerData)
	fmt.Println("All units ms unless noted otherwise")
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
