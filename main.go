package main

import (
	"flag"

	"github.com/tylertreat/flotillad/daemon"
	"github.com/tylertreat/flotillad/daemon/mq"
	"github.com/tylertreat/flotillad/daemon/peer"
)

const (
	mqMode      = "mq"
	peerMode    = "peer"
	defaultPort = 9000
)

func main() {
	var (
		mode = flag.String("mode", mqMode, "[mq|peer]")
		port = flag.Int("port", defaultPort, "daemon port")
	)
	flag.Parse()

	if *mode != mqMode && *mode != peerMode {
		panic("mode must be [mq|peer]")
	}

	var daemon daemon.Daemon
	switch *mode {
	case mqMode:
		if d, err := mq.NewDaemon(); err != nil {
			panic(err)
		} else {
			daemon = d
		}
	case peerMode:
		if d, err := peer.NewDaemon(); err != nil {
			panic(err)
		} else {
			daemon = d
		}
	}

	daemon.Start(*port)
}
