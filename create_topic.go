package klib

import (
	"context"
	"fmt"

	"strconv"
	"time"

	kafka "github.com/segmentio/kafka-go"
)

//create topic if perms allowed

func (self *Klib) CreateTopic(topic string) error {

	//!!! default confluent.cloud replica is 3; if it doesnt match with server config, it will fail create a topic
	numPartitions, replicationFactor := 1, 3

	//non topic specific default numOfPatitions
	if n, err := strconv.Atoi(self.config[fmt.Sprintf(`num_partitions`)]); err == nil {
		if n >= 1 {
			numPartitions = n
		}
	}

	if n, err := strconv.Atoi(self.config[fmt.Sprintf(`num_partitions_%s`, topic)]); err == nil {
		if n >= 1 {
			numPartitions = n
		}
	}

	if n, err := strconv.Atoi(self.config[`replication_factor`]); err == nil {
		if n >= 1 {
			replicationFactor = n
		}
	}

	brokers := self.getBrokers()
	if len(brokers) == 0 {
		return fmt.Errorf(`no brokers configured`)
	}
	firstBroker := self.getBrokers()[0]

	ctx, cancelFn := context.WithTimeout(context.Background(), time.Second*60)
	defer cancelFn()

	brokerConn, err := self.dialer.DialContext(ctx, "tcp", firstBroker)
	if err != nil {
		return fmt.Errorf(`DialContext:%s`, err.Error())
	}

	defer brokerConn.Close()

	controller, err := brokerConn.Controller()

	if err != nil {
		return fmt.Errorf(`brokerConn.Controller:%s`, err.Error())
	}

	controllerHost := fmt.Sprintf("%s:%d", controller.Host, controller.Port)
	controllerConn, err := self.dialer.DialContext(ctx, "tcp", controllerHost)
	if err != nil {
		return err
	}
	defer controllerConn.Close()
	cfg := kafka.TopicConfig{
		NumPartitions:     numPartitions,
		ReplicationFactor: replicationFactor,
		Topic:             topic,
	}
	fmt.Println(`creating topic`, cfg)

	return controllerConn.CreateTopics(cfg)
}

func (self *Klib) CreateTopic2(topic string) error {

	conn, err := kafka.DialLeader(context.Background(), "tcp", self.getBrokers()[0], topic, 0)
	if err != nil {
		return err
	}
	defer conn.Close()
	return nil
}
