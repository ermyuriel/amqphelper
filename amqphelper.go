package amqphelper

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/streadway/amqp"
)

//Configuration is a configuration object of AMQP standard parameters
type Configuration struct {
	Host                    string
	RoutingKey              string
	ContentType             string
	ContentEncoding         string
	Exchange                string
	AutoAcknowledgeMessages bool
	Durable                 bool
	DeleteIfUnused          bool
	Exclusive               bool
	NoWait                  bool
	NoLocal                 bool
	PrefetchCount           int
	PrefetchByteSize        int
	arguments               amqp.Table
}

//Queue is the object defined by the Configuration object
type Queue struct {
	wg            *sync.WaitGroup
	Connected     bool
	connection    *amqp.Connection
	channel       *amqp.Channel
	internalQueue *amqp.Queue
	Config        *Configuration
	workers       *int
}

//Message represents an element to be consumed from the queue
type Message struct {
	*amqp.Delivery
}

//GetQueue receives Config object and returns a queue for publishing and consuming
func GetQueue(config *Configuration) (*Queue, error) {
	var wg sync.WaitGroup
	var wk int
	q := Queue{&wg, false, nil, nil, nil, nil, &wk}

	conn, err := amqp.Dial(config.Host)
	if err != nil {
		return nil, err
	}

	q.connection = conn
	q.Connected = true

	ch, err := q.connection.Channel()
	if err != nil {
		return nil, err
	}

	q.channel = ch

	q.channel.Qos(config.PrefetchCount, config.PrefetchByteSize, true)

	iq, err := q.channel.QueueDeclare(config.RoutingKey, config.Durable, config.DeleteIfUnused, config.Exclusive, config.NoWait, config.arguments)
	if err != nil {
		return nil, err
	}

	q.internalQueue = &iq
	q.Config = config

	return &q, nil
}

//Publish publishes a message to the queue, receives mandatory and immediate flags for the message
func (q *Queue) Publish(message []byte, headers map[string]interface{}, mandatory, immediate bool) error {
	if q.channel == nil {
		return fmt.Errorf("Queue has not been initialized")
	}
	return q.channel.Publish(q.Config.Exchange, q.Config.RoutingKey, mandatory, immediate, amqp.Publishing{ContentType: q.Config.ContentType, ContentEncoding: q.Config.ContentEncoding, Body: []byte(message), Timestamp: time.Now(), Headers: headers})
}

// GetConsumer returns a consumer with the specified id
func (q *Queue) GetConsumer(ConsumerID string) (<-chan amqp.Delivery, error) {
	return q.channel.Consume(q.Config.RoutingKey, ConsumerID, q.Config.AutoAcknowledgeMessages, q.Config.Exclusive, q.Config.NoLocal, q.Config.NoWait, q.Config.arguments)
}

func (q *Queue) notifyErrors() chan *amqp.Error {
	return q.connection.NotifyClose(make(chan *amqp.Error))
}

//LogErrors spanws a goroutine that logs connection errors for the queue
func (q *Queue) LogErrors() {
	ech := q.notifyErrors()
	q.wg.Add(1)
	*q.workers++
	go func() {
		for err := range ech {
			log.Println(err)
			q.Connected = false
		}
		*q.workers--
		q.wg.Done()
	}()
}

//SpawnWorkers initializes n consumers in n goroutines and processes each received message by passing it to the argument function. Queue.KeepRunning should be called next
func (q *Queue) SpawnWorkers(consumerPrefix string, consumers int, f func(m *Message)) error {
	now := time.Now().UnixNano()
	for i := 0; i < consumers; i++ {

		msgs, err := q.GetConsumer(fmt.Sprintf("%s:%v:%v", consumerPrefix, now, i))
		if err != nil {
			return err
		}
		*q.workers++
		q.wg.Add(1)
		go func() {
			for msg := range msgs {
				f(&Message{&msg})
			}
			*q.workers--
			q.wg.Done()
		}()
	}

	return nil
}

//KeepRunning keeps queue processes running
func (q *Queue) KeepRunning() {
	q.wg.Wait()
}

//Recover allows for client recovery on channel errors
func (q *Queue) Recover() error {
	conn, err := amqp.Dial(q.Config.Host)
	if err != nil {
		return err
	}

	q.connection = conn

	ch, err := q.connection.Channel()
	if err != nil {
		return err
	}

	q.channel = ch

	iq, err := q.channel.QueueDeclare(q.Config.RoutingKey, q.Config.Durable, q.Config.DeleteIfUnused, q.Config.Exclusive, q.Config.NoWait, q.Config.arguments)
	if err != nil {
		return err
	}

	q.internalQueue = &iq
	for i := *q.workers; i >= 0; i-- {
		q.wg.Done()
	}

	return nil
}
