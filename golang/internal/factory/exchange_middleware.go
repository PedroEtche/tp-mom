package factory

import (
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
	channel    *amqp.Channel
}

func NewExchange(name string, connectionSettings m.ConnSettings) (*ExchangeMiddleware, error) {
	conn, err := amqp.Dial(fmt.Sprintf("amqp://guest:guest@%s:%v/", connectionSettings.Hostname, connectionSettings.Port))
	if err != nil {
		return nil, err
	}

	ch, err := conn.Channel()
	if err != nil {
		return nil, err
	}

	err = ch.ExchangeDeclare(
		name,    // name
		"topic", // type
		false,   // durability
		false,   // auto-deleted
		false,   // internal
		false,   // no-wait
		nil,     // arguments
	)

	return &ExchangeMiddleware{connection: conn, name: name, channel: ch}, nil
}

func (em *ExchangeMiddleware) StartConsuming(callbackFunc func(msg m.Message, ack func(), nack func())) (err error) {
	return nil
}

func (em *ExchangeMiddleware) StopConsuming() {
}

func (em *ExchangeMiddleware) Send(msg m.Message) (err error) {
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
