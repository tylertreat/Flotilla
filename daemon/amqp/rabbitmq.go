package amqp

import (
	"fmt"
	"log"
	"os/exec"
)

const (
	rabbitMQ     = "dockerfile/rabbitmq"
	internalPort = "5672"
)

type RabbitMQBroker struct {
	containerID string
}

func (r *RabbitMQBroker) Start(host, port string) (interface{}, error) {
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

func (r *RabbitMQBroker) Stop() (interface{}, error) {
	containerID, err := exec.Command("/bin/sh", "-c",
		fmt.Sprintf("docker kill %s", r.containerID)).Output()
	if err != nil {
		log.Printf("Failed to stop container %s: %s", rabbitMQ, err.Error())
		return "", err
	}

	log.Printf("Stopped container %s: %s", rabbitMQ, r.containerID)
	return string(containerID), nil
}
