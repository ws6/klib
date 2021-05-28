package klib

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"
)

func getTestMessages() []*Message {
	n := 5
	ret := []*Message{}

	for i := 0; i < n; i++ {
		topush := &Message{
			Key:     fmt.Sprintf(`%d`, time.Now().Unix()),
			Value:   []byte(fmt.Sprintf(`some value %d`, i)),
			Headers: map[string]string{`h1`: `v1`, `h2`: fmt.Sprintf(`%d`, i)},
		}

		ret = append(ret, topush)
	}
	return ret
}

func TestKLib(t *testing.T) {
	cfg, err := GetConfig()

	if err != nil {
		t.Fatalf("config:%s\n", err.Error())
	}

	if len(cfg) == 0 {
		t.Fatal(`no configurations`)
	}

	k, err := NewKlib(cfg)

	if err != nil {
		t.Fatalf("NewKlib:%s\n", err.Error())
	}
	ctx, cancelFn := context.WithCancel(context.Background())
	topic := `test_klib` //!!!create this yourself.
	msgs := getTestMessages()
	go func() {
		k.Produce(context.Background(), topic, msgs)

	}()
	k.SetConsumerGroupId(`klib_test_group_1`)

	consumed := 0
	k.ConsumeLoop(ctx, topic, func(m *Message) error {
		consumed++
		t.Log(`received message`)
		t.Logf("%+v", m)

		if consumed == len(msgs) {
			cancelFn()
		}

		return nil
	})
	t.Log(`done`)

}

func TestKLibDLQ(t *testing.T) {
	cfg, err := GetConfig()
	if err != nil {
		t.Fatalf("config:%s\n", err.Error())
	}

	if len(cfg) == 0 {
		t.Fatal(`no configurations`)
	}

	k, err := NewKlib(cfg)

	if err != nil {
		t.Fatalf("NewKlib:%s\n", err.Error())
	}
	ctx, cancelFn := context.WithTimeout(context.Background(), time.Second*60)
	topic := `test_klib` //!!!create this yourself.
	msgs := getTestMessages()
	go func() {
		k.Produce(context.Background(), topic, msgs)

	}()
	k.SetConsumerGroupId(`klib_test_group_1`)
	go func() {
		k.ConsumeLoop(ctx, topic, func(m *Message) error {
			return fmt.Errorf(`some error2`)
		})
	}()

	//consume from DLQ. if there is a collision with SetConsumerGroupId, you can try use another Klib object

	consumed := 0
	dlqName := k.GetDLQNameByTopic(topic)
	k.DisableDLQ(dlqName) //!!!prevent recursive creation of topics
	k.ConsumeLoop(ctx, dlqName, func(m *Message) error {

		t.Log(`found error message from dlq`, m)

		consumed++
		t.Log(`get msg from dlq`)
		if consumed >= len(msgs) {
			cancelFn() //this will cancel both comsumerLoops
		}
		return nil
	})

	t.Log(`done`)

}

func TestCreateTopic(t *testing.T) {
	cfg, err := GetConfig()
	if err != nil {
		t.Fatalf("config:%s\n", err.Error())
	}

	if len(cfg) == 0 {
		t.Fatal(`no configurations`)
	}

	k, err := NewKlib(cfg)

	if err != nil {
		t.Fatalf("NewKlib:%s\n", err.Error())
	}
	if err := k.CreateTopic(`temp_topic`); err != nil {
		t.Fatal(err.Error())
	}

}

