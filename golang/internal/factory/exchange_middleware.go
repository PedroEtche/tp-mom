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
}

func NewExchange(name string, connectionSettings m.ConnSettings) (*ExchangeMiddleware, error) {
	conn, err := amqp.Dial(fmt.Sprintf("amqp://guest:guest@%s:%v/", connectionSettings.Hostname, connectionSettings.Port))
	if err != nil {
		return nil, err
	}

	_, err = conn.Channel()
	if err != nil {
		return nil, err
	}

	return &ExchangeMiddleware{}, nil
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
	return nil
}
