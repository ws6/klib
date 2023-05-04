package klib

//this provides toolings for access kafka, specifically supporting confluent-cloud client
import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"sync"

	"crypto/tls"
	"crypto/x509"

	kafka "github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/plain"
	"github.com/sirupsen/logrus"
)

const (
	DEFAULT_BATCH_SIZE           = 1000
	DEFAULT_PRODUCER_BUFFER_SIZE = 1000
	DEFAULT_PRODUCER_RETRY       = 3
)

type Klib struct {
	config                map[string]string
	dialer                *kafka.Dialer
	topicCreated          map[string]bool // when a topic is created by CreateTopic, cache it.
	topicCreatedLock      sync.Mutex
	dlqChan               chan *Message
	ReturnOnProducerError bool
	ProducerErrorHandler
}

type ProducerErrorHandler interface {
	OnKlibProducerError([]*Message, error) error
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

func (self *Klib) GetClient() *kafka.Client {
	// ret := kafka.Client{
	// 	Addr: kafka.TCP(self.getBrokers()...),
	// }
	// func(context.Context, string, string) (net.Conn, error)

	transport := &kafka.Transport{
		// Dial:     conns.Dial,
		// Dial: self.dialer.DialFunc,
		// Resolver: kafka.NewBrokerResolver(nil),
	}
	_ = transport
	client := &kafka.Client{
		Addr:      kafka.TCP(self.getBrokers()...),
		Timeout:   5 * time.Second,
		Transport: transport,
	}

	return client

	// return &ret, nil
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

func (self *Klib) AutoCreateTopic(topic string) bool {
	if self.config[`auto_create_topic`] == `true` {
		return true
	}

	if self.config[fmt.Sprintf(`auto_create_topic_%s`, topic)] == `true` {
		return true
	}

	return false
}

func (self *Klib) NewWriter(topic string) *kafka.Writer {
	//create if not exist topic when set

	if self.AutoCreateTopic(topic) {
		if err := self.CreateIfNotRegistered(topic); err != nil {
			panic(fmt.Sprintf(`Error Create A topic[%s]:%s`, topic, err.Error()))
		}
	}

	brokers := self.getBrokers()
	batchSize, _ := strconv.Atoi(self.config[`producer_batch_size`])
	if batchSize <= 0 {
		batchSize = DEFAULT_BATCH_SIZE
	}
	logrus.Info(`BatchSize=`, batchSize)
	return kafka.NewWriter(kafka.WriterConfig{
		Dialer:  self.dialer,
		Brokers: brokers,
		Topic:   topic,
		//compatible with   librdkafka behavior
		Balancer:  &kafka.CRC32Balancer{},
		BatchSize: batchSize,
		// BatchSize: 1,
		// RequiredAcks: -1, //by default requires all ack
		// WriteTimeout: time.Second * 1,
		// ErrorLogger:  logrus.New(),

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

func (self *Klib) GetBuffSize() int {
	s := self.config[`producer_buffer_size`]
	if s != "" {
		if n, err := strconv.Atoi(s); err == nil && n > 0 {
			return n
		}
	}
	return DEFAULT_PRODUCER_BUFFER_SIZE
}
func (self *Klib) GetProducerRetryCount() int {
	s := self.config[`producer_retr`]
	if s != "" {
		if n, err := strconv.Atoi(s); err == nil && n > 0 {
			return n
		}
	}
	return DEFAULT_PRODUCER_RETRY
}

func (self *Klib) ProduceChan(ctx context.Context, topic string, msgsChan <-chan *Message) error {
	w := self.NewWriter(topic)
	if self.config[`produce_async`] == `true` {
		w.Async = true
	}
	w.Compression = kafka.Gzip
	w.Completion = func(msgs []kafka.Message, comErr error) {
		if comErr != nil {
			logrus.Warnf(`write message(len=%d) error:%s`, len(msgs), comErr.Error())
			//TODO pub to dlq
		}
	}
	defer w.Close()

	buff := []*Message{}
	_publishBufferAndClean := func() error {

		topub := []kafka.Message{}
		for _, m := range buff {
			topub = append(topub, ToKafkaMessage(m))
		}

		werr := w.WriteMessages(ctx, topub...)
		if werr == nil {
			return nil
		}
		for i := 0; i < self.GetProducerRetryCount(); i++ {
			logrus.Info(`retry WriteMessages`, i, " msg count=", len(buff))
			if werr = w.WriteMessages(ctx, topub...); werr == nil {
				logrus.Info(`retry success!!!`)
				return nil
			}
		}
		return werr
	}
	publishBufferAndClean := func() error {
		if len(buff) == 0 {
			return nil
		}
		defer func() {

			buff = []*Message{}
		}()
		if err := _publishBufferAndClean(); err != nil {
			if self.ProducerErrorHandler == nil {
				return err
			}
			if self.ProducerErrorHandler != nil {
				tolog := []*Message{}
				for _, m := range buff {
					tolog = append(tolog, m)
				}
				return self.ProducerErrorHandler.OnKlibProducerError(tolog, err)
			}
		}
		return nil
	}
	for {
		select {
		case msg, ok := <-msgsChan:

			if !ok {
				//https://stackoverflow.com/questions/13666253/breaking-out-of-a-select-statement-when-all-channels-are-closed
				msgsChan = nil
				break
			}

			buff = append(buff, msg)

			if len(buff) >= w.BatchSize {
				//pub and clean
				if err := publishBufferAndClean(); err != nil {
					fmt.Println(`publishBufferAndClean: `, err.Error())
					//TODO retry?
				}
			}

		case <-time.After(5 * time.Second):

			if err := publishBufferAndClean(); err != nil {
				fmt.Println(`publishBufferAndClean(on timeout): `, err.Error())
				//TODO retry?
			}

		case <-ctx.Done():

			return ctx.Err()

		}
		if msgsChan == nil {
			break
		}
	}

	return nil
}

func (self *Klib) GetConsumerGroupId() string {
	if c := os.Getenv(`KLIB_CONSUMER_GROUP_ID`); c != "" {
		self.config[`consumer_group_id`] = c
	}
	return self.config[`consumer_group_id`]
}

func (self *Klib) SetConsumerGroupId(id string) {
	if c := os.Getenv(`KLIB_CONSUMER_GROUP_ID`); c != "" {
		os.Setenv(`KLIB_CONSUMER_GROUP_ID`, "")
	}
	self.config[`consumer_group_id`] = id
}

func (self *Klib) GetReader(topic string) *kafka.Reader {

	if self.AutoCreateTopic(topic) {
		if err := self.CreateIfNotRegistered(topic); err != nil {
			panic(fmt.Sprintf(`Error Create A topic[%s]:%s`, topic, err.Error()))
		}
	}
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
		//TODO push to RMQ for long time lasting consuming
		//fetch it off RMQ once after pushed

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

func (self *Klib) UseAmqp() bool {
	return self.config[`use_amqp`] == `true`
}

//ConsumerLoop runs as loop
func (self *Klib) ConsumeLoop(ctx context.Context, topic string, fn MessageProcessor) {

	if self.AutoCreateTopic(topic) {
		if err := self.CreateIfNotRegistered(topic); err != nil {
			panic(fmt.Sprintf(`Error Create A topic[%s]:%s`, topic, err.Error()))
		}
	}

	if !self.UseAmqp() {
		logrus.Info(`Not using AMQP`)
		self.ConsumeLoopPlain(ctx, topic, fn)
		return
	}
	logrus.Info(`Using AMQP`)
	for {
		if err := self.ConsumeLoopPersistFromRMQ(ctx, topic, fn); err != nil {
			fmt.Println(`ConsumeLoopPersistFromRMQ exit`, err.Error())
			if err != ERR_AMQP_CONNECTION_CLOSED {
				logrus.Warn(`ConsumeLoopPersistFromRMQ:`, err.Error())
				return
			}
			logrus.Info(`trying to reconnect AMQP in 120 seconds.`)

			time.Sleep(time.Second * 120)
		}
	}

}

//ConsumerLoop runs as loop
func (self *Klib) ConsumeLoopPlain(ctx context.Context, topic string, fn MessageProcessor) {
	r := self.GetReader(topic)
	defer r.Close()
	self.Consume(ctx, r, fn)
}

// ConsumeLoopPersistFromRMQ
