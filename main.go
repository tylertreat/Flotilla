package main

import (
	"flag"
	"fmt"

	"github.com/tylertreat/flotillad/daemon"
)

const defaultPort = 9000

func main() {
	port := flag.Int("port", defaultPort, "daemon port")
	flag.Parse()

	d, err := daemon.NewDaemon()
	if err != nil {
		panic(err)
	}

	fmt.Println("flotilla daemon started...")
	if err := d.Start(*port); err != nil {
		panic(err)
	}
}
