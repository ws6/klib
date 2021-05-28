package klib

//dead_letter_queue.go process dead message

import (
	"context"
	"fmt"
	"strings"
)

const (
	KEY_DLQ_ERROR = `dlq_error`
	DLQ_PREFIX    = `dlq`
)

//!!!GetDLQNameByTopic naming convention of dlq name from a topic
func (self *Klib) GetDLQNameByTopic(sourceTopic string) string {
	return fmt.Sprintf(`%s_%s`, DLQ_PREFIX, sourceTopic)
}

func (self *Klib) IsDLQDisabled(sourceTopic string) bool {
	variableKey := fmt.Sprintf(`disable_dlq_%s`, sourceTopic)
	return self.config[variableKey] == `true`
}

func (self *Klib) DisableDLQ(sourceTopic string) {
	variableKey := fmt.Sprintf(`disable_dlq_%s`, sourceTopic)
	self.config[variableKey] = `true`
}

//TrySendDLQMessage if enabled dlq then send msg, otherwise, ignore

func (self *Klib) CreateIfNotRegistered(topic string) error {
	self.topicCreatedLock.Lock()
	defer self.topicCreatedLock.Unlock()
	if v, ok := self.topicCreated[topic]; ok && v {
		return nil
	}

	if err := self.CreateTopic(topic); err != nil {
		return err
	}
	self.topicCreated[topic] = true
	return nil
}

// TrySendDLQMessage check if enable
//if yes, send message with header modified to dlq_${topic}
func (self *Klib) TrySendDLQMessage(topic string, m *Message, emsg error) error {
	if emsg == nil || m == nil {
		return nil
	}
	dlqName := self.GetDLQNameByTopic(topic)

	if strings.HasPrefix(topic, DLQ_PREFIX) {
		return fmt.Errorf(`dlq of dlq_${topic} is not allowed`)
	}

	if self.IsDLQDisabled(topic) {
		return nil
	}

	if err := self.CreateIfNotRegistered(dlqName); err != nil {
		return fmt.Errorf(`CreateIfNotRegistered-DLQ:%s`, err.Error())
	}
	if m.Headers == nil {
		m.Headers = make(map[string]string)
	}
	m.Headers[KEY_DLQ_ERROR] = emsg.Error()

	//use dlqChan instead to prevent error messages flooding
	//only init once
	if self.dlqChan == nil {
		self.dlqChan = make(chan *Message)
		go func() {
			self.ProduceChan(context.Background(), dlqName, self.dlqChan)
		}()
	}
	self.dlqChan <- m
	return nil
}
