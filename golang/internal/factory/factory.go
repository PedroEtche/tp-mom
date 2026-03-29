package factory

import (
	m "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/middleware"
)

func CreateQueueMiddleware(queueName string, connectionSettings m.ConnSettings) (m.Middleware, error) {
	queue, err := NewQueue(queueName, connectionSettings)
	if err != nil {
		return nil, err
	}

	return queue, nil
}

func CreateExchangeMiddleware(exchange string, keys []string, connectionSettings m.ConnSettings) (m.Middleware, error) {
	exc, err := NewExchange(exchange, connectionSettings)
	if err != nil {
		return nil, err
	}

	return exc, nil
}
