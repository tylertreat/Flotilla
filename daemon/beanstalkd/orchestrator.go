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

type BeanstalkdBroker struct {
	containerID string
}

func (b *BeanstalkdBroker) Start(port string) (interface{}, error) {
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

func (b *BeanstalkdBroker) Stop() (interface{}, error) {
	containerID, err := exec.Command("/bin/sh", "-c",
		fmt.Sprintf("docker kill %s", b.containerID)).Output()
	if err != nil {
		log.Printf("Failed to stop container: %s", err.Error())
		return "", err
	}

	log.Printf("Stopped container %s", b.containerID)
	b.containerID = ""
	return string(containerID), nil
}
