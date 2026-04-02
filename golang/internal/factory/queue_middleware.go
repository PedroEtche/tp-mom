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

type QueueMiddleware struct {
	connection *amqp.Connection
	channel    *amqp.Channel
	queue      amqp.Queue
}

func NewQueue(name string, connectionSettings m.ConnSettings) (*QueueMiddleware, error) {
	conn, err := amqp.Dial(fmt.Sprintf("amqp://guest:guest@%s:%v/", connectionSettings.Hostname, connectionSettings.Port))
	if err != nil {
		return nil, err
	}

	// NOTE: Creo un nuevo channel para cada nuevo QueueMiddleware. Segun los docs de RabbitMQ (https://www.rabbitmq.com/docs/channels), internament van a utilizar la misma conexion TCP para evitar overhead
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

	return &QueueMiddleware{connection: conn, channel: ch, queue: q}, nil
}

func (qm *QueueMiddleware) StartConsuming(callbackFunc func(msg m.Message, ack func(), nack func())) (err error) {
	msgs, err := qm.channelConsumeSetUp()
	if err != nil {
		return err
	}

	for d := range msgs {
		copy := d
		callbackFunc(m.Message{Body: string(copy.Body)}, func() { copy.Ack(false) }, func() { copy.Nack(false, false) })
	}

	return nil
}

func (qm *QueueMiddleware) channelConsumeSetUp() (<-chan amqp.Delivery, error) {
	// NOTE: Que el tag del consumer sea el mismo que el de la queue no deberia traer problema porque cada QueueMiddleware
	// crea un nuevo channel (https://www.rabbitmq.com/docs/channels)
	msgs, err := qm.channel.Consume(
		qm.queue.Name, // queue
		qm.queue.Name, // consumer
		false,         // auto-ack
		false,         // exclusive
		false,         // no-local
		false,         // no-wait
		nil,           // args
	)
	if err != nil {
		if errors.Is(err, amqp.ErrClosed) {
			return nil, m.ErrMessageMiddlewareDisconnected
		}
		return nil, m.ErrMessageMiddlewareMessage
	}
	return msgs, nil
}

func (qm *QueueMiddleware) StopConsuming() {
	// NOTE: En este caso qm.queue.Name hace alusion al nombre del consumer
	if err := qm.channel.Cancel(qm.queue.Name, false); err != nil {
		fmt.Printf("Error stopping consuming: %v\n", err)
	}
}

func (qm *QueueMiddleware) Send(msg m.Message) (err error) {
	err = qm.channel.Publish(
		"",            // exchange
		qm.queue.Name, // routing key
		false,         // mandatory
		false,         // immediate
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
