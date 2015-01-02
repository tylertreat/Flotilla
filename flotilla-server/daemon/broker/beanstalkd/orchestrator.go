package beanstalkd

import (
	"fmt"
	"log"
	"os/exec"
)

const (
	beanstalkd   = "m0ikz/beanstalkd"
	internalPort = "11300"
)

// Broker implements the broker interface for Beanstalkd.
type Broker struct {
	containerID string
}

// Start will start the message broker and prepare it for testing.
func (b *Broker) Start(host, port string) (interface{}, error) {
	containerID, err := exec.Command("/bin/sh", "-c",
		fmt.Sprintf("docker run -d -p %s:%s %s", port, internalPort, beanstalkd)).Output()
	if err != nil {
		log.Printf("Failed to start container %s: %s", beanstalkd, err.Error())
		return "", err
	}

	log.Printf("Started container %s: %s", beanstalkd, containerID)
	b.containerID = string(containerID)
	return string(containerID), nil
}

// Stop will stop the message broker.
func (b *Broker) Stop() (interface{}, error) {
	containerID, err := exec.Command("/bin/sh", "-c",
		fmt.Sprintf("docker kill %s", b.containerID)).Output()
	if err != nil {
		log.Printf("Failed to stop container %s: %s", beanstalkd, err.Error())
		return "", err
	}

	log.Printf("Stopped container %s: %s", beanstalkd, b.containerID)
	return string(containerID), nil
}
