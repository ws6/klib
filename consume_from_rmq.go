package klib

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/streadway/amqp"
	// fmt.Sprintf(`amqp://%s:%s@%s:%d/`, userName, password, url, port)
)

func (self *Klib) ConsumeLoopPersistFromRMQ(ctx context.Context, topic string, fn MessageProcessor) error {

	amqpConnStr := self.config[`amqp_connection_string`]
	if amqpConnStr == "" {
		return fmt.Errorf(`amqp_connection_string is empty`)
	}
	conn, err := amqp.Dial(amqpConnStr)
	if err != nil {
		return fmt.Errorf(`amqp.Dial:%s`, err.Error())
	}
	defer conn.Close()

	pubChannel, err := conn.Channel()
	if err != nil {
		return fmt.Errorf(`pubChannel:%s`, err.Error())
	}
	queueName := self.config[`amqp_queue_name`]
	if queueName == "" {
		return fmt.Errorf(`amqp_queue_name is empty`)
	}
	if _, err := pubChannel.QueueDeclare(
		queueName, // name
		true,      // durable
		false,     // delete when unused
		false,     // exclusive
		false,     // no-wait
		nil,       // arguments
	); err != nil {
		return fmt.Errorf(`queueDeclare:%s`, err.Error())
	}
	go func() {
		r := self.GetReader(topic)
		//forward to RMQ
		//no back prressure control for now. making sure kafka doesn't congest RMQ
		if err := self.Consume(ctx, r, func(m *Message) error {

			body, err := json.Marshal(m)
			if err != nil {
				return err
			}
			return pubChannel.Publish(
				"",        // exchange
				queueName, // routing key
				false,     // mandatory
				false,
				amqp.Publishing{
					DeliveryMode: amqp.Persistent,
					ContentType:  "application/json",
					Body:         body,
				},
			)
			return nil
		}); err != nil {
			fmt.Println(`Consume`, err.Error())
			return
		}
	}()

	//consume from RMQ
	ch, err := conn.Channel()
	if err != nil {
		return err
	}

	defer ch.Close()

	msgQueue, err := ch.Consume(

		queueName, // queue
		"",        // consumer
		false,     // auto-ack
		false,     // exclusive
		false,     // no-local
		false,     // no-wait
		nil,       // args
	)
	if err != nil {
		return fmt.Errorf(`Consume:%s`, err.Error())
	}

	for msg := range msgQueue {

		kmsg := new(Message)
		if err := json.Unmarshal(msg.Body, kmsg); err != nil {
			self.TrySendDLQMessage(topic, kmsg, err)
		}

		if err := fn(kmsg); err != nil {
			self.TrySendDLQMessage(topic, kmsg, err)
		}

		msg.Ack(false)

		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			continue
		}

	}

	return nil
}
