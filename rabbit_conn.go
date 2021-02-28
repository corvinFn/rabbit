package rabbit

import (
	"fmt"
	"log"
	"strconv"
	"sync"
	"time"

	"github.com/streadway/amqp"
	"gitlab.yewifi.com/golden-cloud/common"
)

const Route_ALL string = "#"

func failOnError(err error, msg string) {
	if err != nil {
		println(fmt.Sprintf("msg:%s, err:%s", msg, err.Error()))
		log.Println(fmt.Sprintf("%s: %v", msg, err))
	}
}

func panicOnError(err error, msg string) {
	if err != nil {
		println(fmt.Sprintf("msg:%s, err:%s", msg, err.Error()))
		log.Fatalf("%s: %v", msg, err)
	}
}

type rabbitConfig struct {
	uri          string
	exchangeName string
	queueType    string // topic direct
	delayQueue   string
	internal     bool
}

type RabbitMqConn struct {
	conn      *amqp.Connection
	conf      *rabbitConfig
	connErrCh chan *amqp.Error
	chErrCh   chan *amqp.Error
	publishCh *amqp.Channel
	mt        sync.Mutex
}

type MsgHandleFunc func(msg *amqp.Delivery)
type RabbitMqOption func(ms *RabbitMqConn)

func NewRabbitMqConn(rabbitUri string, exchangeName string, queueType string, opts ...RabbitMqOption) (*RabbitMqConn, error) {
	var cli *RabbitMqConn
	fmt.Println("rabbitUri", rabbitUri)
	conn, err := amqp.Dial(rabbitUri)
	if err != nil {
		log.Fatalf("Failed to connect to RabbitMQ")
		return cli, err
	}

	rabbitConf := &rabbitConfig{
		uri:          rabbitUri,
		exchangeName: exchangeName,
		queueType:    queueType,
	}

	publishCh, err := conn.Channel()
	if err != nil {
		panicOnError(err, "fail to alloc channel")
	}
	cli = &RabbitMqConn{
		conn:      conn,
		conf:      rabbitConf,
		connErrCh: make(chan *amqp.Error),
		publishCh: publishCh,
		mt:        sync.Mutex{},
	}

	for _, opt := range opts {
		opt(cli)
	}

	return cli, nil
}

func WithDelayLetter(delayQueue string) RabbitMqOption {
	return func(rc *RabbitMqConn) {
		rc.conf.delayQueue = delayQueue
	}
}

func WithExchangeInternal() RabbitMqOption {
	return func(rc *RabbitMqConn) {
		rc.conf.internal = true
	}
}

func (this *RabbitMqConn) Consume(handle MsgHandleFunc, queueName string, prefech int, routeKeys ...string) {
fallback:
	for {
		if this.conn.IsClosed() {
			this.reconnect()
		}

		ch, err := this.conn.Channel()
		if err != nil {
			failOnError(err, "Failed to open a channel")
			continue
		}

		err = ch.ExchangeDeclare(
			this.conf.exchangeName, // name
			this.conf.queueType,    // type
			true,                   // durable
			false,                  // auto-deleted
			this.conf.internal,     // internal
			false,                  // no-wait
			nil,                    // arguments
		)
		if err != nil {
			failOnError(err, "Failed to declare an exchange")
			continue
		}

		queue, err := ch.QueueDeclare(
			queueName, // name
			true,      // durable
			false,     // auto-deleted
			false,     // exclusive
			false,     // no-wait
			nil,       // arguments

		)
		if err != nil {
			failOnError(err, "Failed to declare a queue")
			continue
		}

		// dead letter queue
		if this.conf.delayQueue != "" {
			_, err = ch.QueueDeclare(
				this.conf.delayQueue, // name
				true,                 // durable
				false,                // delete when unused
				false,                // exclusive
				false,                // no-wait
				amqp.Table{
					// 当消息过期时把消息发送到exchange
					"x-dead-letter-exchange": this.conf.exchangeName,
				}, // arguments
			)
			if err != nil {
				failOnError(err, "Failed to declare delay queue")
				continue
			}
		}

		if prefech == 0 {
			prefech = 1
		}
		err = ch.Qos(
			prefech, // prefetch count
			0,       // prefetch size
			false,   // global
		)
		if err != nil {
			failOnError(err, "Failed to set prefetch count")
			continue
		}

		for _, route := range routeKeys {
			err = ch.QueueBind(
				queue.Name,             // queue name
				route,                  // routing key
				this.conf.exchangeName, // exchange
				false,
				nil)
			if err != nil {
				failOnError(err, "Failed to bind a queue")
				goto fallback
			}
		}

		this.chErrCh = ch.NotifyClose(make(chan *amqp.Error))

		msgs, err := ch.Consume(
			queue.Name, // queueName
			"",         // consumerTag
			false,      // auto ack
			false,      // exclusive
			false,      // no local
			false,      // no wait
			nil,        // args
		)
		if err != nil {
			failOnError(err, "Failed to register a consumer")
			continue
		}

		for {
			select {
			case d, ok := <-msgs:
				if !ok {
					goto fallback
				}
				go handle(&d)
			case <-this.connErrCh:
				goto fallback
			case <-this.chErrCh:
				goto fallback
			}
		}
	}
}

func (this *RabbitMqConn) Publish(routeKey string, msg interface{}) error {
	msgByte, err := common.Marshal(msg)
	if err != nil {
		return err
	}

	// TODO(corvinFn): seems useless if publish fail
	if this.conn.IsClosed() {
		println("=== publish reconnect")
		this.reconnect()
	}
	// ch, err := this.conn.Channel()
	// failOnError(err, "Failed to open a channel")

	err = this.publishCh.Publish(
		this.conf.exchangeName, // exchange
		routeKey,               // routing key
		false,                  // mandatory
		false,                  // immediate
		amqp.Publishing{
			ContentType:  "text/plain",
			DeliveryMode: amqp.Persistent,
			Timestamp:    time.Now(),
			Body:         msgByte,
		})

	failOnError(err, "Failed to publish a message")
	println("send", string(msgByte))

	return err
}

func (this *RabbitMqConn) PublishDelay(msg interface{}, seconds int64) error {
	msgByte, err := common.Marshal(msg)
	if err != nil {
		return err
	}

	ch, err := this.conn.Channel()
	failOnError(err, "Failed to open a channel")

	err = ch.Publish(
		"",
		this.conf.delayQueue, // routing key
		false,                // mandatory
		false,                // immediate
		amqp.Publishing{
			ContentType:  "text/plain",
			DeliveryMode: amqp.Persistent,
			Timestamp:    time.Now(),
			Body:         msgByte,
			Expiration:   sec2MillStr(seconds),
		})

	failOnError(err, "Failed to publish a message")
	println("send", string(msgByte))

	return err
}

func sec2MillStr(seconds int64) string {
	if seconds <= 0 {
		return "0"
	}

	return strconv.FormatInt(seconds*1000, 10)
}

func (this *RabbitMqConn) reconnect() {
	this.mt.Lock()
	defer this.mt.Unlock()

	if this.conn.IsClosed() {
		this.conn = mustConnRabbit(this.conf.uri)
		this.connErrCh = this.conn.NotifyClose(make(chan *amqp.Error))
		this.publishCh, _ = this.conn.Channel()
	}
}

func mustConnRabbit(uri string) *amqp.Connection {
	for {
		conn, err := amqp.Dial(uri)
		if err == nil {
			return conn
		}

		log.Printf("trying to connect rabbitMq err:%v\n", err)
		time.Sleep(time.Second)
	}
}
