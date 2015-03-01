package coordinate

import (
	"errors"
	"log"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/coreos/go-etcd/etcd"
	"github.com/tylertreat/Flotilla/flotilla-server/daemon/broker"
)

// ErrorUnableToConnect - Was not able to connect to the etcd instance
var ErrorUnableToConnect = errors.New("Unable to Connect to etcd")

// ErrorUnableToRegister
var ErrorUnableToRegister = errors.New("Unable to Register with etcd")

// ErrorUnableToStartCluster - can't start the cluster
var ErrorUnableToStartCluster = errors.New("Unable to start cluster")

// ErrorUnableToStopCluster - can't stop the cluster
var ErrorUnableToStopCluster = errors.New("Unable to stop cluster")

const (
	rootDir    = "/running/"
	config     = "/config"
	defaultTTL = 1200
)

// WatchFunc is a type that is used to handle responses from watches that are
// created against etcd
type WatchFunc func(*etcd.Response)

// Client wraps the go-etcd package and its interactions with etcd into simple
// operations that the flotilla-client and flotilla-server can use to spin up a
// test cluster. the flotilla-client manages the cluster and watches for
// for flotilla-server instances to join the flota specific cluster.
type Client struct {
	client     *etcd.Client
	flota      string
	id         string
	watchLock  sync.Mutex
	watchList  map[string]WatchFunc
	stopList   map[string]chan bool
	Registered bool
}

func (c *Client) dir() string {
	return rootDir + c.flota
}

// getOrCreateRoot will delete the root + flota and its children if they do exist
func (c *Client) createOrResetRoot() error {

	resp, err := c.client.Get(c.dir(), false, false)
	if err != nil {
		return err
	}

	if resp != nil {
		_, der := c.client.Delete(c.dir(), true)
		if der != nil {
			return der
		}
		log.Println("Remove previous run ", c.dir())
	}
	_, er := c.client.CreateDir(c.dir(), defaultTTL)
	if er != nil {
		return er
	}
	log.Println("Create new run ", c.dir())

	return nil
}

// StartCluster will
// 1. create the flota dir that all the daemons will register to.
// 2. wait until it all the daemons have been registered
// 3. will timeout on its wait and return false or return true if all found
//
// These should only be used by the flotilla client
func (c *Client) StartCluster(numdaemons int, startsleep int) (bool, error) {

	err := c.createOrResetRoot()
	if err != nil {
		return false, err
	}
	// Wait for the cluster to spin up for the sleep time or return when
	// the cluster is all up. It checks every second.
	step := 0

	for {

		step++
		count := c.ClusterCount()
		log.Printf("%d daemons have registered in %d seconds", count, step)

		if count >= numdaemons {
			return true, nil
		}

		if step > startsleep {
			for _, member := range c.ClusterMembers() {
				log.Printf("Member %s had joined", member)
			}
			return false, errors.New("Daemons not all registered")
		}

		time.Sleep(time.Second * 1)
	}

	return false, errors.New("Unable to start Cluster")
}

// ClusterCount will get against the rootdir + the flota and count how many nodes
// it finds.
func (c *Client) ClusterCount() int {

	// starting at the root flota node we need to go through all the registered nodes
	// and pull out their ip address values (which are a comma seperated list)
	resp, err := c.client.Get(c.dir(), false, false)

	if err != nil {
		return 0
	}
	if resp.Node.Dir == true {
		return len(resp.Node.Nodes)
	}

	return 0
}

// ClusterMembers will concatanate all deamon's who have registered IP addresses into
// a []string of IP addresses
func (c *Client) ClusterMembers() []string {

	var results []string

	// starting at the root flota node we need to go through all the registered nodes
	// and pull out their ip address values (which are a comma seperated list)
	resp, err := c.client.Get(c.dir(), false, false)

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
func (c *Client) Register(port int) error {

	// This is to ensure the client started and the test should actually be run
	resp, cerr := c.client.Get(c.dir(), false, false)
	if cerr != nil {
		return cerr
	}
	if resp == nil {
		return ErrorUnableToRegister
	}

	c.id = broker.GenerateName()

	addresses, iperr := getIPs(port)
	if iperr != nil {
		return iperr
	}
	_, err := c.client.Set(c.dir()+"/"+c.id, strings.Join(addresses, ","), defaultTTL)
	if err == nil {
		c.Registered = true
	}
	return err
}

// Unregister will remove the path to the given client
func (c *Client) Unregister() error {
	c.Close()
	_, err := c.client.Delete(c.dir()+"/"+c.id, false)
	return err
}

// GetConfig is to be used later for shared configuration
func (c *Client) GetConfig() string {
	resp, err := c.client.Get(c.dir()+config, false, false)

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
		for {
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
//
// Note: flota should be 'unique' as the cluster creation process will remove
// any existing nodes and their children to ensure the test run starts with a
// clean slate.
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

// getIPs will get *ALL* IP addresses that are not Loopback ones, so there is a
// potential issue if a daemon has more than one IP address, both will be registered
// with the cluster.
// TODO: Augment server to be able to set IP to be used for registration
func getIPs(port int) ([]string, error) {

	p := strconv.Itoa(port)
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return nil, err
	}

	var results []string
	for _, a := range addrs {
		if ipnet, ok := a.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				results = append(results, ipnet.IP.String()+":"+p)
			}
		}
	}
	return results, nil
}
