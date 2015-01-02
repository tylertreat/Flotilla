package kafka

import (
	"fmt"
	"log"
	"os/exec"
	"time"
)

const (
	zookeeper     = "jplock/zookeeper:3.4.6"
	zookeeperCmd  = "docker run -d -p %s:%s %s"
	zookeeperPort = "2181"
	kafka         = "ches/kafka"
	kafkaPort     = "9092"
	jmxPort       = "7203"
	// TODO: Use --link.
	kafkaCmd = `docker run -d \
	                     -h %s \
	                     -p %s:%s -p %s:%s \
	                     -e EXPOSED_HOST=%s \
						 -e ZOOKEEPER_IP=%s %s`
)

// Broker implements the broker interface for Kafka.
type Broker struct {
	kafkaContainerID     string
	zookeeperContainerID string
}

// Start will start the message broker and prepare it for testing.
func (k *Broker) Start(host, port string) (interface{}, error) {
	if port == zookeeperPort || port == jmxPort {
		return nil, fmt.Errorf("Port %s is reserved", port)
	}

	cmd := fmt.Sprintf(zookeeperCmd, zookeeperPort, zookeeperPort, zookeeper)
	zkContainerID, err := exec.Command("/bin/sh", "-c", cmd).Output()
	if err != nil {
		log.Printf("Failed to start container %s: %s", zookeeper, err.Error())
		return "", err
	}
	log.Printf("Started container %s: %s", zookeeper, zkContainerID)

	cmd = fmt.Sprintf(kafkaCmd, host, kafkaPort, kafkaPort, jmxPort, jmxPort, host, host, kafka)
	kafkaContainerID, err := exec.Command("/bin/sh", "-c", cmd).Output()
	if err != nil {
		log.Printf("Failed to start container %s: %s", kafka, err.Error())
		k.Stop()
		return "", err
	}

	log.Printf("Started container %s: %s", kafka, kafkaContainerID)
	k.kafkaContainerID = string(kafkaContainerID)
	k.zookeeperContainerID = string(zkContainerID)

	// NOTE: Leader election can take a while. For now, just sleep to try to
	// ensure the cluster is ready. Is there a way to avoid this or make it
	// better?
	time.Sleep(time.Minute)

	return string(kafkaContainerID), nil
}

// Stop will stop the message broker.
func (k *Broker) Stop() (interface{}, error) {
	_, err := exec.Command("/bin/sh", "-c", fmt.Sprintf("docker kill %s", k.zookeeperContainerID)).Output()
	if err != nil {
		log.Printf("Failed to stop container %s: %s", zookeeper, err.Error())
	} else {
		log.Printf("Stopped container %s: %s", zookeeper, k.zookeeperContainerID)
	}

	kafkaContainerID, e := exec.Command("/bin/sh", "-c", fmt.Sprintf("docker kill %s", k.kafkaContainerID)).Output()
	if e != nil {
		log.Printf("Failed to stop container %s: %s", kafka, err.Error())
		err = e
	} else {
		log.Printf("Stopped container %s: %s", kafka, k.kafkaContainerID)
	}

	return string(kafkaContainerID), err
}
