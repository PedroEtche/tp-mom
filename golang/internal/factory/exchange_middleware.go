package factory

import (
	"errors"
	"fmt"

	m "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/middleware"
	amqp "github.com/rabbitmq/amqp091-go"
)

// -----------------------------------------------------------------------------
// MIDDLEWARE INTERFACE IMPLEMENTATION
// -----------------------------------------------------------------------------

type ExchangeMiddleware struct {
	connection *amqp.Connection
	name       string
	keys       []string
	channel    *amqp.Channel
}

func NewExchange(name string, keys []string, connectionSettings m.ConnSettings) (*ExchangeMiddleware, error) {
	conn, err := amqp.Dial(fmt.Sprintf("amqp://guest:guest@%s:%v/", connectionSettings.Hostname, connectionSettings.Port))
	if err != nil {
		return nil, err
	}

	ch, err := conn.Channel()
	if err != nil {
		return nil, err
	}

	err = ch.ExchangeDeclare(
		name,     // name
		"direct", // type
		false,    // durability
		false,    // auto-deleted
		false,    // internal
		false,    // no-wait
		nil,      // arguments
	)

	return &ExchangeMiddleware{connection: conn, name: name, keys: keys, channel: ch}, nil
}

func (em *ExchangeMiddleware) StartConsuming(callbackFunc func(msg m.Message, ack func(), nack func())) (err error) {
	q, err := em.channel.QueueDeclare(
		"",    // name
		false, // durability
		true,  // delete when unused
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		if errors.Is(err, amqp.ErrClosed) {
			return m.ErrMessageMiddlewareDisconnected
		}
		return m.ErrMessageMiddlewareMessage
	}

	for _, key := range em.keys {
		err = em.channel.QueueBind(
			q.Name,  // queue name
			key,     // routing key
			em.name, // exchange
			false,
			nil)
		if err != nil {
			if errors.Is(err, amqp.ErrClosed) {
				return m.ErrMessageMiddlewareDisconnected
			}
			return m.ErrMessageMiddlewareMessage
		}
	}

	msgs, err := em.channel.Consume(
		q.Name, // queue
		"",     // consumer
		false,  // auto ack
		false,  // exclusive
		false,  // no local
		false,  // no wait
		nil,    // args
	)
	if err != nil {
		if errors.Is(err, amqp.ErrClosed) {
			return m.ErrMessageMiddlewareDisconnected
		}
		return m.ErrMessageMiddlewareMessage
	}

	for d := range msgs {
		copy := d
		callbackFunc(m.Message{Body: string(copy.Body)}, func() { copy.Ack(false) }, func() { copy.Nack(false, false) })
	}

	return nil
}

func (em *ExchangeMiddleware) StopConsuming() {
	if err := em.channel.Cancel(em.name, false); err != nil {
		fmt.Printf("Error stopping consuming: %v\n", err)
	}
}

func (em *ExchangeMiddleware) Send(msg m.Message) (err error) {
	for _, key := range em.keys {
		err = em.channel.Publish(
			em.name, // exchange
			key,     // routing key
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

	}

	return nil
}

func (em *ExchangeMiddleware) Close() error {
	if err := em.channel.Close(); err != nil {
		return m.ErrMessageMiddlewareClose
	}

	if err := em.connection.Close(); err != nil {
		return m.ErrMessageMiddlewareClose
	}
	return nil
}
