package klib

import (
	"time"

	kafka "github.com/segmentio/kafka-go"
)

//let's agree with this is a scope a message go-bases structure.
type Message struct {
	Headers   map[string]string
	Key       string
	Value     []byte
	CreatedAt time.Time
}

func ToKafkaMessage(msg *Message) kafka.Message {
	ret := kafka.Message{}

	for k, v := range msg.Headers {

		h := kafka.Header{}
		h.Key = k
		h.Value = []byte(v)

		ret.Headers = append(ret.Headers, h)
	}

	ret.Key = []byte(msg.Key)
	ret.Value = msg.Value
	return ret
}

func ToKafkaMessages(msgs []*Message) []kafka.Message {
	ret := []kafka.Message{}

	for _, v := range msgs {
		ret = append(ret, ToKafkaMessage(v))
	}

	return ret
}

func FromKafkaMessage(msg *kafka.Message) *Message {
	ret := new(Message)
	ret.Headers = make(map[string]string)
	ret.CreatedAt = msg.Time

	for _, h := range msg.Headers {
		ret.Headers[h.Key] = string(h.Value)
	}

	//adding creation time from writter

	ret.Key = string(msg.Key)
	ret.Value = msg.Value
	return ret
}
