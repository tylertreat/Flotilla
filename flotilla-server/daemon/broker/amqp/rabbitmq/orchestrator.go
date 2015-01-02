package rabbitmq

import (
	"fmt"
	"log"
	"os/exec"
)

const (
	rabbitMQ     = "dockerfile/rabbitmq"
	internalPort = "5672"
)

// Broker implements the Broker interface for RabbitMQ.
type Broker struct {
	containerID string
}

// Start will start the message broker and prepare it for testing.
func (r *Broker) Start(host, port string) (interface{}, error) {
	containerID, err := exec.Command("/bin/sh", "-c",
		fmt.Sprintf("docker run -d -p %s:%s %s", port, internalPort, rabbitMQ)).Output()
	if err != nil {
		log.Printf("Failed to start container %s: %s", rabbitMQ, err.Error())
		return "", err
	}

	log.Printf("Started container %s: %s", rabbitMQ, containerID)
	r.containerID = string(containerID)
	return string(containerID), nil
}

// Stop will stop the message broker.
func (r *Broker) Stop() (interface{}, error) {
	containerID, err := exec.Command("/bin/sh", "-c",
		fmt.Sprintf("docker kill %s", r.containerID)).Output()
	if err != nil {
		log.Printf("Failed to stop container %s: %s", rabbitMQ, err.Error())
		return "", err
	}

	log.Printf("Stopped container %s: %s", rabbitMQ, r.containerID)
	return string(containerID), nil
}
