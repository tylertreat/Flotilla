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

type ActiveMQBroker struct {
	containerID string
}

func (a *ActiveMQBroker) Start(host, port string) (interface{}, error) {
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

func (a *ActiveMQBroker) Stop() (interface{}, error) {
	containerID, err := exec.Command("/bin/sh", "-c",
		fmt.Sprintf("docker kill %s", a.containerID)).Output()
	if err != nil {
		log.Printf("Failed to stop container %s: %s", activeMQ, err.Error())
		return "", err
	}

	log.Printf("Stopped container %s: %s", activeMQ, a.containerID)
	return string(containerID), nil
}
