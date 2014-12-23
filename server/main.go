package main

import (
	"flag"
	"fmt"

	"github.com/tylertreat/flotilla/server/daemon"
)

const defaultPort = 9000

func main() {
	var (
		port            = flag.Int("port", defaultPort, "daemon port")
		gCloudProjectID = flag.String("gcloud-project-id", "",
			"Google Cloud project id (needed for Cloud Pub/Sub)")
		gCloudJSONKey = flag.String("gcloud-json-key", "",
			"Google Cloud project JSON key file (needed for Cloud Pub/Sub)")
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

	fmt.Println("flotilla daemon started...")
	if err := d.Start(*port); err != nil {
		panic(err)
	}
}
