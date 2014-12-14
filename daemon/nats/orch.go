package nats

import (
	"fmt"
	"log"
	"os/exec"
)

const gnatsd = "apcera/gnatsd"

type NATSBroker struct {
	containerID string
}

func (n *NATSBroker) Start(port string) (interface{}, error) {
	containerID, err := exec.Command("/bin/sh", "-c",
		fmt.Sprintf("docker run -d -p %s:4222 %s", port, gnatsd)).Output()
	if err != nil {
		log.Printf("Failed to start container %s: %s", gnatsd, err.Error())
		return "", err
	}

	log.Printf("Started container %s: %s", gnatsd, containerID)
	n.containerID = string(containerID)
	return string(containerID), nil
}

func (n *NATSBroker) Stop() (interface{}, error) {
	containerID, err := exec.Command("/bin/sh", "-c",
		fmt.Sprintf("docker kill %s", n.containerID)).Output()
	if err != nil {
		log.Printf("Failed to stop container: %s", err.Error())
		return "", err
	}

	log.Printf("Stopped container %s", n.containerID)
	n.containerID = ""
	return string(containerID), nil
}
