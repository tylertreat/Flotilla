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

type KestrelBroker struct {
	containerID string
}

func (k *KestrelBroker) Start(host, port string) (interface{}, error) {
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

func (k *KestrelBroker) Stop() (interface{}, error) {
	containerID, err := exec.Command("/bin/sh", "-c",
		fmt.Sprintf("docker kill %s", k.containerID)).Output()
	if err != nil {
		log.Printf("Failed to stop container %s: %s", kestrelImage, err.Error())
		return "", err
	}

	log.Printf("Stopped container %s: %s", kestrelImage, k.containerID)
	return string(containerID), nil
}
