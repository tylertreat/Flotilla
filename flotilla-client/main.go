package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/olekukonko/tablewriter"
	"github.com/tylertreat/Flotilla/coordinate"
	"github.com/tylertreat/Flotilla/flotilla-client/broker"
)

const (
	defaultDaemonPort   = "9500"
	defaultBrokerPort   = "5000"
	defaultNumMessages  = 500000
	defaultMessageSize  = 1000
	defaultNumProducers = 1
	defaultNumConsumers = 1
	defaultStartupSleep = 8
	defaultNumDaemon    = 0
	defaultHost         = "localhost"
	defaultDaemonHost   = defaultHost + ":" + defaultDaemonPort
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
		brokerName   = flag.String("broker", brokers[0], brokerList())
		brokerPort   = flag.String("broker-port", defaultBrokerPort, "host machine broker port")
		dockerHost   = flag.String("docker-host", defaultHost, "host machine (or VM) running Docker")
		brokerdHost  = flag.String("host", defaultDaemonHost, "machine running broker daemon")
		peerHosts    = flag.String("peer-hosts", defaultDaemonHost, "comma-separated list of machines to run peers")
		producers    = flag.Uint("producers", defaultNumProducers, "number of producers per host")
		consumers    = flag.Uint("consumers", defaultNumConsumers, "number of consumers per host")
		numMessages  = flag.Uint("num-messages", defaultNumMessages, "number of messages to send from each producer")
		messageSize  = flag.Uint64("message-size", defaultMessageSize, "size of each message in bytes")
		startupSleep = flag.Uint("startup-sleep", defaultStartupSleep, "seconds to wait after broker start before benchmarking")
		coordinator  = flag.String("coordinator", "", "http ip & port address for etcd")
		flota        = flag.String("flota", "", "test group the deamon is part of")
		numdaemons   = flag.Int("num-daemons", defaultNumDaemon, "The number of daemons the coordinator should wait for before starting")
	)
	flag.Parse()

	peers := strings.Split(*peerHosts, ",")

	// If we are to use etcd then start the cluster coordination
	var cclient coordinate.Client
	if coordinator != nil && *flota != "" && *numdaemons >= 0 {
		log.Printf("Starting Cluster for %s and waiting for %d daemons", *flota, *numdaemons)
		cclient := coordinate.NewSimpleCoordinator(*coordinator, *flota)
		up, err := cclient.StartCluster(*numdaemons, int(*startupSleep))
		if up {
			log.Println("Cluster Started")
		} else {
			log.Println("Cluster did not start")
			if err != nil {
				log.Println("Cluster start up error was ", err.Error())
			}
			os.Exit(1)
		}
		peers = append(peers, cclient.ClusterMembers()...)
	}

	client, err := broker.NewClient(&broker.Benchmark{
		BrokerdHost:  *brokerdHost,
		BrokerName:   *brokerName,
		BrokerHost:   *dockerHost,
		BrokerPort:   *brokerPort,
		PeerHosts:    peers,
		NumMessages:  *numMessages,
		MessageSize:  *messageSize,
		Publishers:   *producers,
		Subscribers:  *consumers,
		StartupSleep: *startupSleep,
	})
	if err != nil {
		cclient.StopCluster()
		fmt.Println("Failed to connect to flotilla:", err)
		os.Exit(1)
	}

	start := time.Now()
	results, err := runBenchmark(client)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	elapsed := time.Since(start)

	printSummary(client.Benchmark, elapsed)
	printResults(results)
}

func runBenchmark(client *broker.Client) ([]*broker.ResultContainer, error) {
	defer client.Teardown()
	sig := make(chan os.Signal, 2)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sig
		client.Teardown()
		os.Exit(1)
	}()

	return client.Start()
}

func printSummary(benchmark *broker.Benchmark, elapsed time.Duration) {
	brokerHost := strings.Split(benchmark.BrokerdHost, ":")[0] + ":" + benchmark.BrokerPort
	msgSent := int(benchmark.NumMessages) * len(benchmark.PeerHosts) * int(benchmark.Publishers)
	msgRecv := int(benchmark.NumMessages) * len(benchmark.PeerHosts) * int(benchmark.Subscribers)
	dataSentKB := (msgSent * int(benchmark.MessageSize)) / 1000
	dataRecvKB := (msgRecv * int(benchmark.MessageSize)) / 1000
	fmt.Println("\nTEST SUMMARY\n")
	fmt.Printf("Time Elapsed:       %s\n", elapsed.String())
	fmt.Printf("Broker:             %s (%s)\n", benchmark.BrokerName, brokerHost)
	fmt.Printf("Nodes:              %s\n", benchmark.PeerHosts)
	fmt.Printf("Producers per node: %d\n", benchmark.Publishers)
	fmt.Printf("Consumers per node: %d\n", benchmark.Subscribers)
	fmt.Printf("Messages produced:  %d\n", msgSent)
	fmt.Printf("Messages consumed:  %d\n", msgRecv)
	fmt.Printf("Bytes per message:  %d\n", benchmark.MessageSize)
	fmt.Printf("Data produced (KB): %d\n", dataSentKB)
	fmt.Printf("Data consumed (KB): %d\n", dataRecvKB)
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
