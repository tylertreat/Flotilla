package coordinate

import (
	"errors"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/coreos/go-etcd/etcd"
	"github.com/tylertreat/Flotilla/flotilla-server/daemon/broker"
)

var ErrorUnableToConnect = errors.New("Unable to Connect to etcd")
var ErrorUnableToStartCluster = errors.New("Unable to start cluster")
var ErrorUnableToStopCluster = errors.New("Unable to stop cluster")

const (
	rootDir    = "/running/"
	config     = "/config"
	defaultTTL = 1200
)

type WatchFunc func(*etcd.Response)

type Client struct {
	client    *etcd.Client
	flota     string
	id        string
	watchLock sync.Mutex
	watchList map[string]WatchFunc
	stopList  map[string]chan bool
}

// These should only be used by the flotilla client
func (c *Client) StartCluster(numdaemons int, startsleep int) {
	t := time.Now()
	value := t.Format(time.RFC3339)
	resp, err := c.client.Create(rootDir+c.flota, string(value), defaultTTL)
	if resp == nil || err != nil {
		panic(ErrorUnableToStartCluster)
	}

	// Wait for the cluster to spin up for the sleep time or return when
	// the cluster is all up. It checks every second
	step := 0
	wait := time.Duration(startsleep) * time.Second
	select {
	case <-time.After(time.Second * 1):
		count := c.ClusterCount()
		if count >= numdaemons {
			return
		}
		if step > startsleep {
			return
		}
		step++
	case <-time.After(wait):
		return
	default:
	}

}

// These should only be used by the flotilla client
func (c *Client) StopCluster() {
	c.Close()
	resp, err := c.client.Delete(rootDir+c.flota, false)
	if resp == nil || err != nil {
		panic(ErrorUnableToStopCluster)
	}
}

func (c *Client) ClusterCount() int {

	// starting at the root flota node we need to go through all the registered nodes
	// and pull out their ip address values (which are a comma seperated list)
	resp, err := c.client.Get(rootDir+c.flota, false, false)

	if err != nil {
		return 0
	}
	if resp.Node.Dir == true {
		return len(resp.Node.Nodes)
	}

	return 0
}

// This will concatanate all deamon's who have registered IP addresses into
// a []string of IP addresses
func (c *Client) ClusterMembers() []string {

	var results []string

	// starting at the root flota node we need to go through all the registered nodes
	// and pull out their ip address values (which are a comma seperated list)
	resp, err := c.client.Get(rootDir+c.flota, false, false)

	if err != nil {
		return nil
	}
	if resp.Node.Dir == true {
		for _, node := range resp.Node.Nodes {
			values := strings.Split(node.Value, ",")
			results = append(results, values...)
		}
	}
	return results
}

// Register will add to the path under the rootdir and flota so its IP address
// can be pulled down.
// TODO: Expand this to cover ports and maybe a way to pick a single address
func (c *Client) Register(port string) error {

	// This is to ensure the client started and the test should actually be run
	resp, cerr := c.client.Get(rootDir+c.flota, false, false)
	if resp == nil || cerr != nil {
		return cerr
	}

	c.id = broker.GenerateName()

	addresses, iperr := getIPs(port)
	if iperr != nil {
		return iperr
	}
	_, err := c.client.Set(rootDir+c.flota+"/"+c.id, strings.Join(addresses, ","), defaultTTL)
	return err
}

// Unregister will remove the path to the given client
func (c *Client) Unregister() error {
	c.Close()
	_, err := c.client.Delete(rootDir+c.flota+c.id, false)
	return err
}

// GetConfig is to be used later for shared configuration
func (c *Client) GetConfig() string {
	resp, err := c.client.Get(rootDir+c.flota+config, false, false)

	if resp == nil || err != nil {
		return ""
	}

	return resp.Node.Value
}

// Watch is to allow the cluster to respond to common events from the cluster
func (c *Client) Watch(prefix string, alert WatchFunc) {
	watchChan := make(chan *etcd.Response)
	stopWatch := make(chan bool)

	c.watchLock.Lock()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {

		stopChan := make(chan bool)
		go c.client.Watch(prefix, 0, false, watchChan, stopChan)
		wg.Done()
		select {

		case response := <-watchChan:
			go alert(response)
		case stop := <-stopWatch:
			stopChan <- stop
			return
		}
	}()

	wg.Wait()
	c.watchList[prefix] = alert
	c.stopList[prefix] = stopWatch
	c.watchLock.Unlock()
}

// Close will send out on all the stop channels to close down any watch goroutines
func (c *Client) Close() {

	c.watchLock.Lock()
	for _, v := range c.stopList {
		v <- true
	}
	c.watchLock.Unlock()
}

// NewSimpleCoordinator returns a Client for the Client and Daemon to use to
// register themselves with etcd. Address should the http addressable address
// of where the etcd server is running
func NewSimpleCoordinator(address, flota string) *Client {

	client := etcd.NewClient([]string{address})

	// Panic if we cannot connect to the coordination service, the daemon will
	// not be able to participate in the test run.
	if client == nil {
		panic(ErrorUnableToConnect)
	}
	watchList := make(map[string]WatchFunc, 0)
	stopList := make(map[string]chan bool, 0)

	return &Client{client: client, flota: flota, watchList: watchList, stopList: stopList}
}

func getIPs(port string) ([]string, error) {

	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return nil, err
	}

	var results []string
	for _, a := range addrs {
		if ipnet, ok := a.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				results = append(results, ipnet.IP.String())
			}
		}
	}
	return results, nil
}
