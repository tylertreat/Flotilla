package main

import (
	"flag"
	"fmt"

	"github.com/tylertreat/Flotilla/coordinate"
	"github.com/tylertreat/Flotilla/flotilla-server/daemon"
)

const defaultPort = 9500

func main() {
	var (
		port            = flag.Int("port", defaultPort, "daemon port")
		gCloudProjectID = flag.String("gcloud-project-id", "",
			"Google Cloud project id (needed for Cloud Pub/Sub)")
		gCloudJSONKey = flag.String("gcloud-json-key", "",
			"Google Cloud project JSON key file (needed for Cloud Pub/Sub)")
		coordinator = flag.String("coordinator", "", "http ip & port address for etcd")
		flota       = flag.String("flota", "", "test group the deamon is part of")
	)
	flag.Parse()

	config := &daemon.Config{
		GoogleCloudProjectID: *gCloudProjectID,
		GoogleCloudJSONKey:   *gCloudJSONKey,
	}

	d, err := daemon.NewDaemon(config)

	if err != nil {
		panic(err)
	}

	// Connect to coordinator service to establish our presence now that we
	// are up and running ok.
	// TODO: Modify this so that we can do TLS connections if we have the right
	// information
	// TODO: Consider go func () ... the actual registration
	var client coordinate.Client
	if coordinator != nil && flota != nil {
		client := coordinate.NewSimpleCoordinator(*coordinator, *flota)
		err := client.Register(*port)
		if err != nil {
			panic(err)
		}
	}

	fmt.Printf("Flotilla daemon started on port %d...\n", *port)
	if err := d.Start(*port); err != nil {
		// We have to try to unregister ourselves if we can
		if client.Registered {
			client.Unregister()
		}
		panic(err)
	}
}
