package kestrel

import (
	"fmt"
	"log"
	"os/exec"
)

const (
	kestrelImage = "thefactory/kestrel"
	internalPort = "2229"
)

// Broker implements the broker interface for Kestrel.
type Broker struct {
	containerID string
}

// Start will start the message broker and prepare it for testing.
func (k *Broker) Start(host, port string) (interface{}, error) {
	containerID, err := exec.Command("/bin/sh", "-c",
		fmt.Sprintf("docker run -d -p %s:%s %s", port, internalPort, kestrelImage)).Output()
	if err != nil {
		log.Printf("Failed to start container %s: %s", kestrelImage, err.Error())
		return "", err
	}

	log.Printf("Started container %s: %s", kestrelImage, containerID)
	k.containerID = string(containerID)
	return string(containerID), nil
}

// Stop will stop the message broker.
func (k *Broker) Stop() (interface{}, error) {
	containerID, err := exec.Command("/bin/sh", "-c",
		fmt.Sprintf("docker kill %s", k.containerID)).Output()
	if err != nil {
		log.Printf("Failed to stop container %s: %s", kestrelImage, err.Error())
		return "", err
	}

	log.Printf("Stopped container %s: %s", kestrelImage, k.containerID)
	return string(containerID), nil
}
