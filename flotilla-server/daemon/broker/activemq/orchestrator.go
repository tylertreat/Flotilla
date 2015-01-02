package activemq

import (
	"fmt"
	"log"
	"os/exec"
)

const (
	activeMQ     = "rmohr/activemq"
	internalPort = "61613"
)

// Broker implements the broker interface for ActiveMQ.
type Broker struct {
	containerID string
}

// Start will start the message broker and prepare it for testing.
func (a *Broker) Start(host, port string) (interface{}, error) {
	containerID, err := exec.Command("/bin/sh", "-c",
		fmt.Sprintf("docker run -d -p %s:%s %s", port, internalPort, activeMQ)).Output()
	if err != nil {
		log.Printf("Failed to start container %s: %s", activeMQ, err.Error())
		return "", err
	}

	log.Printf("Started container %s: %s", activeMQ, containerID)
	a.containerID = string(containerID)
	return string(containerID), nil
}

// Stop will stop the message broker.
func (a *Broker) Stop() (interface{}, error) {
	containerID, err := exec.Command("/bin/sh", "-c",
		fmt.Sprintf("docker kill %s", a.containerID)).Output()
	if err != nil {
		log.Printf("Failed to stop container %s: %s", activeMQ, err.Error())
		return "", err
	}

	log.Printf("Stopped container %s: %s", activeMQ, a.containerID)
	return string(containerID), nil
}
