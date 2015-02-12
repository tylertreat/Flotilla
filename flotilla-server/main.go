package main

import (
	"flag"
	"fmt"
	"runtime"

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
	)
	flag.Parse()
	runtime.GOMAXPROCS(runtime.NumCPU())

	config := &daemon.Config{
		GoogleCloudProjectID: *gCloudProjectID,
		GoogleCloudJSONKey:   *gCloudJSONKey,
	}

	d, err := daemon.NewDaemon(config)
	if err != nil {
		panic(err)
	}

	fmt.Printf("Flotilla daemon started on port %d...\n", *port)
	if err := d.Start(*port); err != nil {
		panic(err)
	}
}
