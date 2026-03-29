package factory

import (
	"errors"
	"fmt"

	m "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/middleware"
	amqp "github.com/rabbitmq/amqp091-go"
)

// -----------------------------------------------------------------------------
// HELP FUNCTIONS
// -----------------------------------------------------------------------------

func readMessages(msgs <-chan amqp.Delivery, callbackFunc func(msg m.Message, ack func(), nack func()), stopChannel <-chan bool) {
	for {
		select {
		// TODO: Ver que pasa si se cae Rabbit cuando intento leer un mensaje
		case d := <-msgs:
			callbackFunc(m.Message{Body: string(d.Body)}, func() { d.Ack(false) }, func() { d.Nack(false, false) })
		case _ = <-stopChannel:
			return
		}
	}
}

// -----------------------------------------------------------------------------
// MIDDLEWARE INTERFACE IMPLEMENTATION
// -----------------------------------------------------------------------------

type QueueMiddleware struct {
	connection  *amqp.Connection
	name        string
	channel     *amqp.Channel
	declaration amqp.Queue
	stopChannel chan bool
	consuming   bool
}

func NewQueue(name string, connectionSettings m.ConnSettings) (*QueueMiddleware, error) {
	conn, err := amqp.Dial(fmt.Sprintf("amqp://guest:guest@%s:%v/", connectionSettings.Hostname, connectionSettings.Port))
	if err != nil {
		return nil, err
	}

	ch, err := conn.Channel()
	if err != nil {
		return nil, err
	}

	q, err := ch.QueueDeclare(
		name,  // name
		true,  // durability
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		amqp.Table{
			amqp.QueueTypeArg: amqp.QueueTypeQuorum,
		},
	)
	if err != nil {
		return nil, err
	}

	stopChannel := make(chan bool)

	return &QueueMiddleware{connection: conn, name: name, channel: ch, declaration: q, stopChannel: stopChannel, consuming: false}, nil
}

func (qm *QueueMiddleware) StartConsuming(callbackFunc func(msg m.Message, ack func(), nack func())) (err error) {
	if qm.consuming {
		return nil
	}
	msgs, err := qm.channel.Consume(
		qm.name, // queue
		"",      // consumer
		false,   // auto-ack
		false,   // exclusive
		false,   // no-local
		false,   // no-wait
		nil,     // args
	)
	if err != nil {
		if errors.Is(err, amqp.ErrClosed) {
			return m.ErrMessageMiddlewareDisconnected
		}
		return m.ErrMessageMiddlewareMessage
	}

	go readMessages(msgs, callbackFunc, qm.stopChannel)

	qm.consuming = true
	return nil
}

func (qm *QueueMiddleware) StopConsuming() {
	if !qm.consuming {
		return
	}
	qm.stopChannel <- true
	qm.consuming = false
}

func (qm *QueueMiddleware) Send(msg m.Message) (err error) {
	err = qm.channel.Publish(
		"",      // exchange
		qm.name, // routing key
		false,   // mandatory
		false,   // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(msg.Body),
		})
	if err != nil {
		if errors.Is(err, amqp.ErrClosed) {
			return m.ErrMessageMiddlewareDisconnected
		}
		return m.ErrMessageMiddlewareMessage
	}
	return nil
}

func (qm *QueueMiddleware) Close() error {
	if err := qm.channel.Close(); err != nil {
		return m.ErrMessageMiddlewareClose
	}

	if err := qm.connection.Close(); err != nil {
		return m.ErrMessageMiddlewareClose
	}

	return nil
}
