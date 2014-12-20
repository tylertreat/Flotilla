package kafka

import (
	"fmt"
	"log"
	"os/exec"
)

const (
	zookeeper     = "jplock/zookeeper:3.4.6"
	zookeeperPort = "2181"
	kafka         = "stealthly/docker-kafka"
	kafkaPort     = "9092"
	kafkaCmd      = `docker run -d -p %s:%s -e "ZK_PORT_2181_TCP_ADDR=%s" -e "HOST_IP=%s" -e "BROKER_ID=1" -e "PORT=%s" %s`
)

type KafkaBroker struct {
	kafkaContainerID     string
	zookeeperContainerID string
}

func (k *KafkaBroker) Start(host, port string) (interface{}, error) {
	if port == zookeeperPort {
		return nil, fmt.Errorf("Port %s is reserved", port)
	}

	cmd := fmt.Sprintf("docker run -d -p %s:%s  %s", zookeeperPort, zookeeperPort, zookeeper)
	fmt.Println(cmd)
	zkContainerID, err := exec.Command("/bin/sh", "-c", cmd).Output()
	if err != nil {
		log.Printf("Failed to start container %s: %s", zookeeper, err.Error())
		return "", err
	}
	log.Printf("Started container %s: %s", zookeeper, zkContainerID)

	cmd = fmt.Sprintf(kafkaCmd, kafkaPort, kafkaPort, host, host, kafkaPort, kafka)
	fmt.Println(cmd)
	kafkaContainerID, err := exec.Command("/bin/sh", "-c", cmd).Output()
	if err != nil {
		log.Printf("Failed to start container %s: %s", kafka, err.Error())
		k.Stop()
		return "", err
	}

	log.Printf("Started container %s: %s", kafka, kafkaContainerID)
	k.kafkaContainerID = string(kafkaContainerID)
	k.zookeeperContainerID = string(zkContainerID)
	return string(kafkaContainerID), nil
}

func (k *KafkaBroker) Stop() (interface{}, error) {
	_, err := exec.Command("/bin/sh", "-c", fmt.Sprintf("docker kill %s", k.zookeeperContainerID)).Output()
	if err != nil {
		log.Printf("Failed to stop container: %s", err.Error())
	}

	kafkaContainerID, e := exec.Command("/bin/sh", "-c", fmt.Sprintf("docker kill %s", k.kafkaContainerID)).Output()
	if err != nil {
		log.Printf("Failed to stop container: %s", err.Error())
		err = e
	}

	return string(kafkaContainerID), err
	return "", nil
}
