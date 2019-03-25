package amqphelper

import (
	"fmt"
	"sync"

	"github.com/streadway/amqp"
)

//Configuration is a configuration object of AMQP standard parameters
type Configuration struct {
	Host                    string
	RoutingKey              string
	ContentType             string
	Exchange                string
	AutoAcknowledgeMessages bool
	Durable                 bool
	DeleteIfUnused          bool
	Exclusive               bool
	NoWait                  bool
	NoLocal                 bool
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

	ch, err := q.connection.Channel()
	if err != nil {
		return nil, err
	}

	q.channel = ch

	iq, err := q.channel.QueueDeclare(config.RoutingKey, config.Durable, config.DeleteIfUnused, config.Exclusive, config.NoWait, config.arguments)
	if err != nil {
		return nil, err
	}

	q.internalQueue = &iq
	q.Config = config

	return &q, nil
}

//Publish publishes a message to the queue with the initialized
func (q *Queue) Publish(message []byte, mandatory, immediate bool) error {
	if q.channel == nil {
		return fmt.Errorf("Queue has not been initialized")
	}
	return q.channel.Publish(q.Config.Exchange, q.Config.RoutingKey, mandatory, immediate, amqp.Publishing{ContentType: q.Config.ContentType, Body: []byte(message)})
}

// GetConsumer returns a consumer with the specified id
func (q *Queue) GetConsumer(ConsumerID string) (<-chan amqp.Delivery, error) {
	return q.channel.Consume(q.Config.RoutingKey, ConsumerID, q.Config.AutoAcknowledgeMessages, q.Config.Exclusive, q.Config.NoLocal, q.Config.NoWait, q.Config.arguments)
}

//SpawnWorkers initializes n consumers in n goroutines and processes each received message by passing it to the argument function. Queue.KeepRunnig should be called next
func (q *Queue) SpawnWorkers(consumerPrefix string, consumers int, f func(m *Message)) error {

	for i := 0; i < consumers; i++ {
		msgs, err := q.GetConsumer(consumerPrefix)
		if err != nil {
			return err
		}
		*q.workers++
		q.wg.Add(1)

		go func() {
			for msg := range msgs {
				f(&Message{&msg})
			}
			q.wg.Done()
		}()
	}

	return nil
}

//KeepRunning keeps workers running
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
