package kestrel

import (
	"bufio"
	"fmt"
	"net"
	"regexp"
	"time"
)

var statQueueNameLine = regexp.MustCompile(`^STAT queue_(\S+)_open_transactions`)

// The kestrel thrift interface provides no method that I can find to list
// queue names. Instead we fall back to the memcached interface's STATS
// command here.
func QueueNames(host string, memcachedPort int) ([]string, error) {
	conn, err := net.Dial("tcp", fmt.Sprintf("%s:%d", host, memcachedPort))
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	err = conn.SetDeadline(time.Now().Add(10 * time.Second))
	if err != nil {
		return nil, err
	}

	_, err = conn.Write([]byte("STATS\r\n"))
	if err != nil {
		return nil, err
	}

	queues := make([]string, 0, 32)
	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		text := scanner.Text()
		if text == "END" {
			conn.Close()
			break
		} else if matches := statQueueNameLine.FindStringSubmatch(scanner.Text()); matches != nil {
			queues = append(queues, matches[1])
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	} else {
		return queues, nil
	}
}
