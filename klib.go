package klib

//this provides toolings for access kafka, specifically supporting confluent-cloud client
import (
	"context"
	"fmt"
	"log"
	"strconv"
	"strings"

	"sync"

	"crypto/tls"
	"crypto/x509"

	"github.com/segmentio/kafka-go/sasl/plain"

	kafka "github.com/segmentio/kafka-go"
)

const (
	DEFAULT_BATCH_SIZE = 1
)

type Klib struct {
	config           map[string]string
	dialer           *kafka.Dialer
	topicCreated     map[string]bool // when a topic is created by CreateTopic, cache it.
	topicCreatedLock sync.Mutex
	dlqChan          chan *Message
}

func (self *Klib) GetConfig() map[string]string {
	return self.config
}

func NewKlib(cfg map[string]string) (*Klib, error) {
	ret := new(Klib)
	ret.topicCreated = make(map[string]bool)

	// deep copy the map

	ret.config = make(map[string]string)

	for k, v := range cfg {
		ret.config[k] = v
	}

	rootCAs, _ := x509.SystemCertPool()
	if rootCAs == nil {
		rootCAs = x509.NewCertPool()
	}
	ret.dialer = &kafka.Dialer{
		DualStack: true,
		SASLMechanism: plain.Mechanism{
			Username: ret.config[`sasl.username`], // access key
			Password: ret.config[`sasl.password`], // secret
		},
		TLS: &tls.Config{
			InsecureSkipVerify: true,
			RootCAs:            rootCAs,
		},
	}

	return ret, nil
}

func (self *Klib) Close() error {
	if self.dlqChan != nil {
		close(self.dlqChan)
	}
	return nil
}

func (self *Klib) getBrokers() []string {
	return strings.Split(self.config[`bootstrap.servers`], ";")
}

func (self *Klib) NewWriter(topic string) *kafka.Writer {
	brokers := self.getBrokers()
	batchSize, _ := strconv.Atoi(self.config[`batch_size`])
	if batchSize <= 0 {
		batchSize = DEFAULT_BATCH_SIZE
	}
	return kafka.NewWriter(kafka.WriterConfig{
		Dialer:  self.dialer,
		Brokers: brokers,
		Topic:   topic,
		//compatible with   librdkafka behavior
		Balancer:  &kafka.CRC32Balancer{},
		BatchSize: batchSize,
	})
}

//ProduceOne only produce one message and open/close a writter. insuffient way to produce a message
//use with caution
func (self *Klib) ProduceOne(ctx context.Context, topic string, msg *Message) error {
	return self.Produce(ctx, topic, []*Message{msg})
}

func (self *Klib) Produce(ctx context.Context, topic string, msgs []*Message) error {
	w := self.NewWriter(topic)
	defer w.Close()
	kmsgs := ToKafkaMessages(msgs)
	//TODO when error create dead letter message

	if err := w.WriteMessages(ctx, kmsgs...); err != nil {
		return err
	}

	return nil
}

func (self *Klib) ProduceChan(ctx context.Context, topic string, msgsChan <-chan *Message) error {
	w := self.NewWriter(topic)
	defer w.Close()

	for msg := range msgsChan {
		kmsg := ToKafkaMessage(msg)
		//TODO when error create dead letter message
		//and retry
		if err := w.WriteMessages(ctx, kmsg); err != nil {
			fmt.Println(`WriteMessages`, err.Error())
			continue
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			continue

		}

	}

	return nil
}

func (self *Klib) GetConsumerGroupId() string {
	return self.config[`consumer_group_id`]
}

func (self *Klib) SetConsumerGroupId(id string) {
	self.config[`consumer_group_id`] = id
}

func (self *Klib) GetReader(topic string) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers: self.getBrokers(),
		GroupID: self.GetConsumerGroupId(),
		Topic:   topic,
		Dialer:  self.dialer,
	})
}

type MessageProcessor func(*Message) error

func (self *Klib) Consume(ctx context.Context, r *kafka.Reader, fn MessageProcessor) error {
	defer r.Close()

	if fn == nil {
		return fmt.Errorf(`no MessageProcessor func`)
	}
	for {
		m, err := r.ReadMessage(ctx)

		if err != nil {
			log.Fatal(err.Error())
			break
		}

		aloeMsg := FromKafkaMessage(&m)
		if err := fn(aloeMsg); err != nil {
			//TODO need to route to dead letter queue rather than error out.
			//check if dlq exists
			//create or get dlq
			//cache it
			// Klib.dlqCache = make(map[string]string)

			//publish dlq msage with header[KEY_DLQ_HEADER] = err.Error()
			// TrySendDLQMessage()
			if _err := self.TrySendDLQMessage(r.Config().Topic, aloeMsg, err); _err != nil {
				log.Printf("message err at offset %d: %s = %s\n", m.Offset, string(m.Key), string(m.Value))
				log.Println(`TrySendDLQMessage`, _err.Error())
			}

		}

		select {
		case <-ctx.Done():
			log.Println(`canceled or timeout`)
			return ctx.Err()
		default:
			continue
		}
	}
	return nil
}

//ConsumerLoop runs as loop
func (self *Klib) ConsumeLoop(ctx context.Context, topic string, fn MessageProcessor) {
	r := self.GetReader(topic)
	self.Consume(ctx, r, fn)
}
